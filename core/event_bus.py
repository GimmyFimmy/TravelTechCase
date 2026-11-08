"""
    Desc: In-memory event bus with topics, idempotent publish, retry and dead letter queue
    Creator: Kirosha
"""

import asyncio
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from core.models import log

class Topics:
    FLIGHT_STATUS_CHANGED = "flight.status.changed"
    TRIP_DISRUPTED = "trip.disrupted"
    TRIP_UPDATED = "trip.updated"
    NOTIF_REQUESTED = "notif.requested"
    BOOKING_CONFIRMED = "booking.confirmed"

@dataclass(frozen=True)
class Event:
    topic: str
    # idempotency key: the same topic + key is delivered only once
    key: str
    payload: Any
    at: datetime

EventHandler = Callable[[Event], Awaitable[None]]

@dataclass(frozen=True)
class DeadLetter:
    event: Event
    handler: str
    error: Exception
    attempts: int

class EventBus:
    def __init__(self, max_attempts: int = 3, retry_delay: float = 0.005, clock: Callable[[], datetime] | None = None):
        self.max_attempts = max_attempts
        self.retry_delay = retry_delay
        self.__clock = clock or (lambda: datetime.now(timezone.utc))

        self.__subs: dict[str, list[tuple[str, EventHandler]]] = {}
        self.__seen: set[str] = set()
        self.__queue: deque[Event] = deque()
        self.__draining = False

        # events whose handler failed max_attempts times
        self.dead_letters: list[DeadLetter] = []

    def subscribe(self, topic: str, handler: EventHandler, name: str = "anonymous") -> None:
        self.__subs.setdefault(topic, []).append((name, handler))

    async def publish(self, topic: str, key: str, payload: Any) -> None:
        """
        Handlers run one by one in FIFO order. A publish inside a handler only enqueues
        the event: the outer publish drains the queue, so the outermost `await publish`
        completes when the whole chain of events is processed.
        """
        if f"{topic}|{key}" in self.__seen: return log("bus", f"duplicate {topic} [{key}] dropped")
        self.__seen.add(f"{topic}|{key}")

        self.__queue.append(Event(topic=topic, key=key, payload=payload, at=self.__clock()))
        if self.__draining: return

        self.__draining = True
        try:
            while self.__queue:
                await self.__dispatch(self.__queue.popleft())
        finally:
            self.__draining = False

    async def __dispatch(self, event: Event) -> None:
        for name, handler in list(self.__subs.get(event.topic, [])):
            attempt = 0

            while True:
                attempt += 1
                try:
                    await handler(event)
                    break
                except Exception as err:
                    if attempt >= self.max_attempts:
                        self.dead_letters.append(DeadLetter(event=event, handler=name, error=err, attempts=attempt))
                        log("bus", f"handler {name} failed {attempt} times on {event.topic} → DLQ: {err}")
                        break
                    # linear backoff, the handlers are idempotent so a retry is safe
                    await asyncio.sleep(self.retry_delay * attempt)
