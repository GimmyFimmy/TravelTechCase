"""
    Desc: FlightTrackerService polls flight status via an adapter, normalizes and deduplicates it
    Creator: Kirosha
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from datetime import datetime
from enum import Enum

from core.circuit_breaker import CircuitBreaker, CircuitOpenError, retry_with_backoff
from core.event_bus import EventBus, Topics
from core.models import Confidence, FlightSegment, FlightStatus, FlightStatusChanged, log

class UpdateSource(Enum):
    """API = airline API (high confidence), MAILING = fallback e-mail notice (low confidence)"""
    API = "api"
    MAILING = "mailing"

@dataclass(frozen=True)
class RawFlightUpdate:
    """What a provider returns, before normalization"""
    flight_no: str
    # provider status code: ON_TIME, DELAYED, CANCELLED (CX), DEPARTED, LANDED
    status_code: str
    delay_minutes: int
    observed_at: datetime
    source: UpdateSource

class FlightAdapter(ABC):
    """Adapter: every provider is hidden behind this interface"""

    @abstractmethod
    async def fetch(self, flight_no: str, date: datetime) -> RawFlightUpdate: ...

class MockFlightAdapter(FlightAdapter):
    """
    Scripted provider. Every flight has a queue of answers (RawFlightUpdate or an Exception),
    the head is consumed on each call, the last item is repeated forever.
    """

    def __init__(self, script: dict[str, list[RawFlightUpdate | Exception]]):
        self.__script = script

    async def fetch(self, flight_no: str, date: datetime) -> RawFlightUpdate:
        queue = self.__script.get(flight_no)
        if not queue: raise LookupError(f"нет данных по рейсу {flight_no}")

        item = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(item, Exception): raise item
        return item

@dataclass(frozen=True)
class FailedPoll:
    """A poll that failed after all retries (in prod: a DLQ topic)"""
    segment_id: str
    error: Exception

_STATUSES = {
    "DELAYED": FlightStatus.DELAYED,
    "CANCELLED": FlightStatus.CANCELLED,
    "CX": FlightStatus.CANCELLED,
    "DEPARTED": FlightStatus.DEPARTED,
    "LANDED": FlightStatus.LANDED,
}

class FlightTrackerService:
    def __init__(self, bus: EventBus, adapter: FlightAdapter, breaker: CircuitBreaker):
        self.__bus = bus
        self.__adapter = adapter
        self.__breaker = breaker

        # last known status of every tracked segment, it is also the fallback when the API is down
        self.__known: dict[str, FlightSegment] = {}

        self.dead_letters: list[FailedPoll] = []

    def track(self, segments: list[FlightSegment]) -> None:
        for segment in segments:
            self.__known[segment.id] = segment

    async def poll(self) -> None:
        for segment_id in list(self.__known):
            await self.__poll_one(segment_id)

    async def __poll_one(self, segment_id: str) -> None:
        known = self.__known.get(segment_id)
        if known is None: return

        raw = await self.__fetch(known)
        if raw is None: return

        # data arrives late and out of order: an older observation must not overwrite a newer one
        if raw.observed_at < known.observed_at:
            return log("tracker", f"{known.flight_no}: устаревшее наблюдение отброшено")

        update = self.__normalize(known, raw)

        # same news is dropped, unless it comes from a more trusted source (low → high)
        upgraded = update.confidence > known.confidence
        if update.fingerprint == known.fingerprint and not upgraded:
            if update.status != FlightStatus.SCHEDULED:
                log("tracker", f"{known.flight_no}: дубликат ({update.fingerprint}, {update.confidence.name.lower()}) отброшен")
            return

        self.__known[segment_id] = update
        log("tracker", f"{update.flight_no}: {update.status.value} +{update.delay_minutes} мин, confidence={update.confidence.name.lower()}")

        await self.__bus.publish(
            Topics.FLIGHT_STATUS_CHANGED,
            f"{update.id}:{update.fingerprint}:{update.confidence.name.lower()}",
            FlightStatusChanged(previous=known, segment=update),
        )

    async def __fetch(self, known: FlightSegment) -> RawFlightUpdate | None:
        """Retry with backoff inside a circuit breaker. If it fails, the last known status stays."""
        try:
            return await retry_with_backoff(
                lambda: self.__breaker.call(lambda: self.__adapter.fetch(known.flight_no, known.depart)),
                # an open breaker will not close within a retry, so do not hammer it
                retry_if=lambda err: not isinstance(err, CircuitOpenError),
            )
        except Exception as err:
            self.dead_letters.append(FailedPoll(known.id, err))
            log(
                "tracker",
                f"{known.flight_no}: API недоступно ({type(err).__name__}: {err}), остаёмся на последнем известном статусе "
                f"{known.status.value} +{known.delay_minutes} мин (confidence={known.confidence.name.lower()})",
            )
            return None

    def __normalize(self, known: FlightSegment, raw: RawFlightUpdate) -> FlightSegment:
        status = _STATUSES.get(raw.status_code.upper(), FlightStatus.SCHEDULED)

        return replace(
            known,
            status=status,
            delay_minutes=0 if status == FlightStatus.CANCELLED else raw.delay_minutes,
            confidence=Confidence.HIGH if raw.source == UpdateSource.API else Confidence.LOW,
            observed_at=raw.observed_at,
        )
