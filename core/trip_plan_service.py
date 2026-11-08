"""
    Desc: TripPlanService keeps the shared trip plan up to date, with roles and optimistic locking
    Creator: Kirosha
"""

from dataclasses import replace
from typing import Callable

from core.event_bus import Event, EventBus, Topics
from core.models import (
    BookingConfirmed, Confidence, FlightStatusChanged, PlanItem, Role, Severity, Trip, TripUpdated, local_time, log,
)
from core.notification_service import notify_participants

class VersionConflict(Exception):
    def __init__(self, trip_id: str, expected: int, actual: int):
        super().__init__(f"поездка {trip_id} — ожидалась версия {expected}, в базе {actual}")

class PermissionDenied(Exception):
    pass

class TripRepository:
    """In-memory stand-in for a trips table with a `version` column"""

    def __init__(self):
        self.__store: dict[str, Trip] = {}

    def create(self, trip: Trip) -> None:
        self.__store[trip.id] = trip

    def get(self, trip_id: str) -> Trip | None:
        return self.__store.get(trip_id)

    def require(self, trip_id: str) -> Trip:
        trip = self.__store.get(trip_id)
        if trip is None: raise KeyError(f"поездка {trip_id} не найдена")
        return trip

    def find_by_segment(self, segment_id: str) -> list[Trip]:
        return [t for t in self.__store.values() if any(s.id == segment_id for s in t.segments)]

    def save(self, updated: Trip, expected_version: int) -> Trip:
        """UPDATE ... WHERE id = ? AND version = ?, a mismatch means someone else saved first"""
        current = self.require(updated.id)
        if current.version != expected_version: raise VersionConflict(updated.id, expected_version, current.version)

        saved = replace(updated, version=expected_version + 1)
        self.__store[saved.id] = saved
        return saved

class TripPlanService:
    def __init__(self, bus: EventBus, trips: TripRepository):
        self.__bus = bus
        self.__trips = trips

        bus.subscribe(Topics.FLIGHT_STATUS_CHANGED, self.__on_flight_changed, name="plan")
        bus.subscribe(Topics.BOOKING_CONFIRMED, self.__on_booking_confirmed, name="plan")

    def __update(self, trip_id: str, mutate: Callable[[Trip], Trip]) -> Trip | None:
        """Read-modify-write with a retry on a version conflict (3 attempts)"""
        for _ in range(3):
            current = self.__trips.get(trip_id)
            if current is None: return None

            try:
                return self.__trips.save(mutate(current), current.version)
            except VersionConflict as err:
                log("plan", f"конфликт версий, повтор: {err}")

        raise RuntimeError(f"поездка {trip_id}: слишком много конфликтов версий")

    def __with_risk(self, trip: Trip) -> Trip:
        """Items that start after departure but before we arrive are at risk"""
        first_depart = trip.segments[0].depart
        arrive = trip.final_arrive

        return replace(trip, items=[replace(i, at_risk=first_depart < i.start < arrive) for i in trip.items])

    async def __on_flight_changed(self, event: Event) -> None:
        change: FlightStatusChanged = event.payload

        # a low-confidence status is not written to the shared plan, it may be wrong
        if change.segment.confidence != Confidence.HIGH: return

        for trip in self.__trips.find_by_segment(change.segment.id):
            saved = self.__update(trip.id, lambda t: self.__with_risk(t.replace_segment(change.segment)))
            if saved: await self.__announce(trip, saved, f"рейс {change.segment.flight_no}: {change.segment.status.value}")

    async def __on_booking_confirmed(self, event: Event) -> None:
        booking: BookingConfirmed = event.payload
        before = self.__trips.get(booking.trip_id)
        if before is None: return

        def mutate(trip: Trip) -> Trip:
            segments = []
            for segment in trip.segments:
                if segment.id == booking.replaced_segment_id:
                    segments.extend(booking.itinerary.segments)
                else:
                    segments.append(segment)
            return self.__with_risk(replace(trip, segments=segments))

        saved = self.__update(booking.trip_id, mutate)
        if saved: await self.__announce(before, saved, f"переоформление: {booking.itinerary.id}, билет {booking.ticket_ref}")

    async def __announce(self, before: Trip, after: Trip, reason: str) -> None:
        """trip.updated, plus a notification if some items have just become at risk"""
        was_at_risk = {i.id for i in before.items if i.at_risk}
        at_risk = [i for i in after.items if i.at_risk]
        fresh = [i.title for i in at_risk if i.id not in was_at_risk]

        await self.__bus.publish(
            Topics.TRIP_UPDATED,
            f"{after.id}:v{after.version}",
            TripUpdated(trip_id=after.id, version=after.version, reason=reason, at_risk_titles=[i.title for i in at_risk]),
        )

        if not fresh: return

        destination = after.segments[-1].destination
        await notify_participants(
            self.__bus,
            after,
            f"{after.id}:atrisk:v{after.version}",
            Severity.CRITICAL,
            f"Под угрозой: {', '.join(fresh)} — начало раньше вашего прилёта ({local_time(after.final_arrive, destination)}). "
            "Перенесите или отмените.",
        )

    async def add_activity(self, trip_id: str, user_id: str, expected_version: int, item: PlanItem) -> Trip:
        """User edit: roles are checked and the client must pass the version it has seen"""
        trip = self.__trips.require(trip_id)
        participant = trip.participant_of(user_id)

        if participant is None: raise PermissionDenied(f"{user_id} не участник поездки {trip_id}")
        if participant.role == Role.VIEWER: raise PermissionDenied(f"{user_id} — viewer, правки запрещены")

        saved = self.__trips.save(self.__with_risk(replace(trip, items=[*trip.items, item])), expected_version)
        await self.__announce(trip, saved, f"добавлена активность {item.title}")
        return saved
