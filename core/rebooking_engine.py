"""
    Desc: RebookingEngine finds alternatives (hard filters + ranking) and rebooks them via a saga
    Creator: Kirosha
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Awaitable, Callable

from core.circuit_breaker import CircuitBreaker, CircuitOpenError, retry_with_backoff
from core.event_bus import Event, EventBus, Topics
from core.models import (
    BookingConfirmed, Confidence, Disruption, FlightSegment, FlightStatus, Itinerary, Severity, Trip,
    airport_of, local_time, log, utc,
)
from core.notification_service import notify_participants
from core.trip_plan_service import TripRepository
from core.vault import VaultService

# ───────────────────────────── external API(s) ─────────────────────────────

class FlightSearch(ABC):
    @abstractmethod
    async def search(self, origin: str, destination: str, not_before: datetime) -> list[Itinerary]: ...

def _leg(flight_no: str, origin: str, destination: str, depart: tuple[int, int], arrive: tuple[int, int]) -> FlightSegment:
    return FlightSegment(
        id=f"alt-{flight_no}",
        flight_no=flight_no,
        origin=origin,
        destination=destination,
        depart=utc(2026, 11, 10, *depart),
        arrive=utc(2026, 11, 10, *arrive),
        status=FlightStatus.SCHEDULED,
        delay_minutes=0,
        confidence=Confidence.HIGH,
    )

class MockFlightSearch(FlightSearch):
    """
    Static IST → BCN offers for 2026-11-10. The filters are the engine's job, not the search's,
    so the mock returns everything, including offers that must be rejected:
    TK1857 leaves too early (MCT), PC1272 is hand-luggage only, BA675+BA478 transits GB (no visa).
    """

    async def search(self, origin: str, destination: str, not_before: datetime) -> list[Itinerary]:
        return [
            Itinerary([_leg("TK1857", "IST", "BCN", (12, 20), (15, 50))], price_eur=140, baggage_kg=23),
            Itinerary([_leg("PC1272", "IST", "BCN", (14, 0), (17, 25))], price_eur=95, baggage_kg=0),
            Itinerary([_leg("TK1859", "IST", "BCN", (15, 10), (18, 40))], price_eur=150, baggage_kg=23),
            Itinerary(
                [_leg("BA675", "IST", "LHR", (13, 30), (17, 40)), _leg("BA478", "LHR", "BCN", (19, 0), (21, 15))],
                price_eur=210, baggage_kg=23,
            ),
            Itinerary(
                [_leg("IB3201", "IST", "MAD", (13, 5), (17, 15)), _leg("IB1234", "MAD", "BCN", (18, 15), (19, 25))],
                price_eur=120, baggage_kg=23,
            ),
            Itinerary([_leg("TK1861", "IST", "BCN", (17, 30), (21, 0))], price_eur=100, baggage_kg=23),
        ]

class BookingGateway(ABC):
    @abstractmethod
    async def hold(self, itinerary: Itinerary, passports: dict[str, str], idempotency_key: str) -> str: ...

    @abstractmethod
    async def release(self, hold_id: str) -> None: ...

    @abstractmethod
    async def issue(self, hold_id: str) -> str: ...

    @abstractmethod
    async def cancel(self, ticket_ref: str) -> None: ...

class PaymentGateway(ABC):
    @abstractmethod
    async def charge(self, amount_eur: int, idempotency_key: str) -> str: ...

    @abstractmethod
    async def refund(self, charge_id: str) -> None: ...

class MockBookingGateway(BookingGateway):
    def __init__(self):
        self.__hold_by_key: dict[str, str] = {}
        self.__seq = 0

    async def hold(self, itinerary: Itinerary, passports: dict[str, str], idempotency_key: str) -> str:
        # the same key returns the same hold: a retried step does not hold twice
        existing = self.__hold_by_key.get(idempotency_key)
        if existing: return existing

        # passports are used only inside this call and never logged in full
        masked = ", ".join(f"{user}:***{number[-3:]}" for user, number in passports.items())
        self.__seq += 1
        hold_id = f"HOLD-{self.__seq}"
        self.__hold_by_key[idempotency_key] = hold_id
        log("booking", f"hold {hold_id} for {itinerary.id} (passports: {masked})")
        return hold_id

    async def release(self, hold_id: str) -> None:
        log("booking", f"release {hold_id}")

    async def issue(self, hold_id: str) -> str:
        ticket = f"TKT-{hold_id.split('-')[1]}"
        log("booking", f"issued ticket {ticket}")
        return ticket

    async def cancel(self, ticket_ref: str) -> None:
        log("booking", f"cancel {ticket_ref}")

class MockPaymentGateway(PaymentGateway):
    def __init__(self, decline_suffixes: set[str] | None = None):
        # a charge whose key ends with one of these is declined (used by `--fail-payment`)
        self.decline_suffixes = decline_suffixes or set()
        self.__charge_by_key: dict[str, str] = {}
        self.__seq = 0

    async def charge(self, amount_eur: int, idempotency_key: str) -> str:
        # the same key never charges twice
        existing = self.__charge_by_key.get(idempotency_key)
        if existing: return existing

        if any(idempotency_key.endswith(suffix) for suffix in self.decline_suffixes):
            raise RuntimeError("payment declined by the bank")

        self.__seq += 1
        charge_id = f"CHG-{self.__seq}"
        self.__charge_by_key[idempotency_key] = charge_id
        log("payment", f"charge {charge_id} for {amount_eur} EUR")
        return charge_id

    async def refund(self, charge_id: str) -> None:
        log("payment", f"refund {charge_id}")

# ───────────────────────────── saga ─────────────────────────────

@dataclass(frozen=True)
class SagaStep:
    name: str
    run: Callable[[], Awaitable[None]]
    compensate: Callable[[], Awaitable[None]]

class Saga:
    """Runs the steps in order. If a step fails, the already completed ones are compensated in reverse."""

    async def execute(self, steps: list[SagaStep]) -> bool:
        done: list[SagaStep] = []

        for step in steps:
            try:
                await step.run()
                done.append(step)
                log("saga", f"step {step.name}: ok")
            except Exception as err:
                log("saga", f"step {step.name} failed: {err} — rolling back")

                for completed in reversed(done):
                    try:
                        await completed.compensate()
                        log("saga", f"compensation {completed.name}: ok")
                    except Exception as compensation_err:
                        # a failed compensation needs a human, in prod it goes to the DLQ
                        log("saga", f"compensation {completed.name} failed: {compensation_err} — manual review")
                return False

        return True

# ───────────────────────────── engine ─────────────────────────────

@dataclass(frozen=True)
class RankedOption:
    itinerary: Itinerary
    score: float

class RebookingEngine:
    def __init__(
        self,
        bus: EventBus,
        trips: TripRepository,
        search: FlightSearch,
        booking: BookingGateway,
        payment: PaymentGateway,
        vault: VaultService,
        clock: Callable[[], datetime] | None = None,
    ):
        self.__bus = bus
        self.__trips = trips
        self.__search = search
        self.__booking = booking
        self.__payment = payment
        self.__vault = vault
        self.__breaker = CircuitBreaker(name="flight-search", clock=clock)

        bus.subscribe(Topics.TRIP_DISRUPTED, self.__on_disrupted, name="rebooking")

    async def __on_disrupted(self, event: Event) -> None:
        disruption: Disruption = event.payload
        trip = self.__trips.get(disruption.trip_id)
        if trip is None: return

        broken = next((s for s in trip.segments if s.id == disruption.replace_segment_id), None)
        if broken is None: return

        candidates = await retry_with_backoff(
            lambda: self.__breaker.call(lambda: self.__search.search(broken.origin, broken.destination, disruption.not_before)),
            retry_if=lambda err: not isinstance(err, CircuitOpenError),
        )

        log(
            "rebooking",
            f"candidates: {len(candidates)}; need a departure from {broken.origin} no earlier than {local_time(disruption.not_before, broken.origin)}",
        )

        # 1. hard filters: a candidate that breaks any of them is dropped, not ranked
        ranked: list[RankedOption] = []
        for candidate in candidates:
            reason = self.__reject_reason(trip, candidate, disruption)
            if reason:
                log("rebooking", f"✗ {candidate.id}: {reason}")
                continue
            ranked.append(RankedOption(candidate, self.__score(candidate, broken)))

        # 2. ranking: the lower the score, the better
        ranked.sort(key=lambda option: option.score)

        if not ranked:
            return await notify_participants(
                self.__bus, trip, f"{disruption.id}:no-options", Severity.CRITICAL,
                "No suitable alternatives found, support help is needed.",
            )

        for i, option in enumerate(ranked, start=1):
            log("rebooking", f"✓ #{i} {option.itinerary.describe()} (score {option.score:.0f})")

        offers = "\n  ".join(f"{i}) {option.itinerary.describe()}" for i, option in enumerate(ranked[:3], start=1))
        await notify_participants(
            self.__bus, trip, f"{disruption.id}:offers", Severity.CRITICAL,
            f"Alternatives for {broken.flight_no}:\n  {offers}",
        )

        # 3. auto-rebook only within the budget, otherwise the user picks an offer himself
        if trip.auto_rebook_budget_eur <= 0: return

        for option in ranked:
            itinerary = option.itinerary
            if itinerary.price_eur > trip.auto_rebook_budget_eur:
                log("rebooking", f"skip {itinerary.id}: {itinerary.price_eur} EUR over budget {trip.auto_rebook_budget_eur} EUR")
                continue
            if await self.__rebook(trip, disruption, broken, itinerary): return

        await notify_participants(
            self.__bus, trip, f"{disruption.id}:rebook-failed", Severity.CRITICAL,
            "Auto-rebook failed, pick an option manually.",
        )

    def __reject_reason(self, trip: Trip, itinerary: Itinerary, disruption: Disruption) -> str | None:
        """Hard filters, the first failed one is the reason. None = the candidate is fine."""
        # baggage: the tariff must cover the strictest participant
        if itinerary.baggage_kg < trip.required_baggage_kg:
            return f"baggage: tariff {itinerary.baggage_kg} kg, need {trip.required_baggage_kg} kg"

        # visas: every transit country must be allowed for every participant
        for code in itinerary.via_airports:
            country = airport_of(code).country
            blocked = [p.user_id for p in trip.participants if country not in p.allowed_countries]
            if blocked: return f"visa: transit via {country} ({code}), not allowed for {', '.join(blocked)}"

        # MCT: the first leg must leave after the earliest possible departure...
        origin = itinerary.segments[0].origin
        if itinerary.depart < disruption.not_before:
            return (
                f"MCT: departure {local_time(itinerary.depart, origin)} "
                f"earlier than allowed {local_time(disruption.not_before, origin)}"
            )

        # ...and so must every connection inside the offer
        for current, following in zip(itinerary.segments, itinerary.segments[1:]):
            gap = int((following.depart - current.arrive).total_seconds() // 60)
            mct = airport_of(current.destination).mct_minutes
            if gap < mct: return f"MCT: connection in {current.destination} {gap} min, MCT {mct} min"

        return None

    def __score(self, itinerary: Itinerary, broken: FlightSegment) -> float:
        """Default weights: 1 point per minute of extra delay, 0.5 per EUR, 90 per stop"""
        delay = int((itinerary.arrive - broken.actual_arrive).total_seconds() // 60)
        return max(0, delay) + itinerary.price_eur * 0.5 + itinerary.stops * 90

    def __passports(self, trip: Trip, purpose_key: str) -> dict[str, str]:
        result = {}

        for participant in trip.participants:
            if participant.passport_ref is None: raise RuntimeError(f"{participant.user_id} has no passport_ref")
            result[participant.user_id] = self.__vault.read(
                participant.passport_ref, actor="rebooking-saga", purpose=f"rebook {purpose_key}",
            )

        return result

    async def __rebook(self, trip: Trip, disruption: Disruption, broken: FlightSegment, itinerary: Itinerary) -> bool:
        """hold → charge → issue, with compensations. The keys make every step safe to repeat."""
        key = f"{disruption.id}:{itinerary.id}"
        hold_id: str | None = None
        charge_id: str | None = None
        ticket_ref: str | None = None

        log("rebooking", f"auto-rebook {broken.flight_no} → {itinerary.id}")

        async def hold() -> None:
            nonlocal hold_id
            hold_id = await self.__booking.hold(itinerary, self.__passports(trip, key), key)

        async def release() -> None:
            if hold_id: await self.__booking.release(hold_id)

        async def charge() -> None:
            nonlocal charge_id
            charge_id = await self.__payment.charge(itinerary.price_eur, key)

        async def refund() -> None:
            if charge_id: await self.__payment.refund(charge_id)

        async def issue() -> None:
            nonlocal ticket_ref
            if hold_id is None: raise RuntimeError("no hold to issue")
            ticket_ref = await self.__booking.issue(hold_id)

        async def cancel() -> None:
            if ticket_ref: await self.__booking.cancel(ticket_ref)

        ok = await Saga().execute([
            SagaStep("hold", run=hold, compensate=release),
            SagaStep("charge", run=charge, compensate=refund),
            SagaStep("issue", run=issue, compensate=cancel),
        ])
        if not ok: return False

        ticket = ticket_ref or "n/a"
        await self.__bus.publish(
            Topics.BOOKING_CONFIRMED,
            key,
            BookingConfirmed(
                trip_id=trip.id,
                disruption_id=disruption.id,
                replaced_segment_id=broken.id,
                itinerary=itinerary,
                ticket_ref=ticket,
            ),
        )
        await notify_participants(
            self.__bus, trip, f"{disruption.id}:rebooked", Severity.INFO,
            f"Tickets rebooked: {itinerary.describe()}. Ticket {ticket}.",
        )
        return True
