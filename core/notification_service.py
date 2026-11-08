"""
    Desc: NotificationService sends notifications idempotently via push → email → SMS, with an offline queue
    Creator: Kirosha
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Callable

from core.event_bus import Event, EventBus, Topics
from core.models import NotificationRequest, Severity, Trip, log

class ChannelKind(Enum):
    PUSH = "push"
    EMAIL = "email"
    SMS = "sms"

class DeliveryResult(Enum):
    DELIVERED = "delivered"
    OFFLINE = "offline"
    FAILED = "failed"

@dataclass(frozen=True)
class Notification:
    id: str
    user_id: str
    trip_id: str
    severity: Severity
    text: str
    created_at: datetime

class NotificationChannel(ABC):
    kind: ChannelKind

    @abstractmethod
    async def send(self, notification: Notification) -> DeliveryResult: ...

class MockChannel(NotificationChannel):
    """Channel stub: a user from `offline_users` is unreachable, from `failing_users` the send fails"""

    def __init__(self, kind: ChannelKind, offline_users: set[str] | None = None, failing_users: set[str] | None = None):
        self.kind = kind
        self.offline_users = offline_users or set()
        self.failing_users = failing_users or set()

    async def send(self, notification: Notification) -> DeliveryResult:
        if notification.user_id in self.offline_users:
            log(self.kind.value, f"→ {notification.user_id}: устройство оффлайн")
            return DeliveryResult.OFFLINE
        if notification.user_id in self.failing_users:
            log(self.kind.value, f"→ {notification.user_id}: ошибка отправки")
            return DeliveryResult.FAILED

        log(self.kind.value, f"→ {notification.user_id} [{notification.severity.value}] {notification.text}")
        return DeliveryResult.DELIVERED

@dataclass(frozen=True)
class QueuedNotification:
    # position in the queue, the client keeps the last seen one as its sync cursor
    seq: int
    notification: Notification

@dataclass(frozen=True)
class SyncResult:
    items: list[QueuedNotification]
    cursor: int

async def notify_participants(bus: EventBus, trip: Trip, key_base: str, severity: Severity, text: str) -> None:
    """Publishes one notif.requested per participant. The key makes it idempotent."""
    for participant in trip.participants:
        key = f"{key_base}:{participant.user_id}"
        await bus.publish(
            Topics.NOTIF_REQUESTED,
            key,
            NotificationRequest(key=key, user_id=participant.user_id, trip_id=trip.id, severity=severity, text=text),
        )

class NotificationService:
    def __init__(self, bus: EventBus, chain: list[NotificationChannel], clock: Callable[[], datetime] | None = None):
        self.__chain = chain
        self.__clock = clock or (lambda: datetime.now(timezone.utc))

        # idempotency: a key is processed only once even if the event is delivered twice
        self.__processed: set[str] = set()

        self.__offline: dict[str, list[QueuedNotification]] = {}
        self.__seq = 0

        bus.subscribe(Topics.NOTIF_REQUESTED, self.__on_requested, name="notify")

    async def __on_requested(self, event: Event) -> None:
        request: NotificationRequest = event.payload
        if request.key in self.__processed: return log("notify", f"повтор {request.key} пропущен")
        self.__processed.add(request.key)

        notification = Notification(
            id=request.key,
            user_id=request.user_id,
            trip_id=request.trip_id,
            severity=request.severity,
            text=request.text,
            created_at=self.__clock(),
        )

        queued = False

        for channel in self.__chain:
            result = DeliveryResult.FAILED
            try:
                result = await channel.send(notification)
            except Exception:
                # a broken channel is the same as a failed one, go to the next
                pass

            if result == DeliveryResult.DELIVERED: return

            if result == DeliveryResult.OFFLINE:
                # the device gets it on the next sync
                if not queued: self.__enqueue(notification)
                queued = True
                # info is push only; critical escalates to e-mail / SMS right away
                if notification.severity == Severity.INFO: return

        # nobody delivered it: keep for the next sync, so it is not lost
        if not queued: self.__enqueue(notification)

    def __enqueue(self, notification: Notification) -> None:
        self.__seq += 1
        self.__offline.setdefault(notification.user_id, []).append(QueuedNotification(self.__seq, notification))

    def sync(self, user_id: str, cursor: int) -> SyncResult:
        """Everything the user missed after `cursor`"""
        items = [q for q in self.__offline.get(user_id, []) if q.seq > cursor]
        return SyncResult(items, items[-1].seq if items else cursor)

class DeviceCache:
    """What the client app keeps locally: a cursor and a cache. Items are deduplicated by id."""

    def __init__(self, user_id: str):
        self.user_id = user_id
        self.cursor = 0
        self.cache: dict[str, Notification] = {}

    def sync_from(self, service: NotificationService) -> None:
        result = service.sync(self.user_id, self.cursor)

        for queued in result.items:
            self.cache[queued.notification.id] = queued.notification
            log(f"device:{self.user_id}", f"из очереди: {queued.notification.text}")

        self.cursor = result.cursor
        log(f"device:{self.user_id}", f"sync: получено {len(result.items)}, cursor={self.cursor}, в кэше {len(self.cache)}")
