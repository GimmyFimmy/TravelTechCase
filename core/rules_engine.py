"""
    Desc: RulesEngine detects cancellations and broken connections (MCT) on flight status changes
    Creator: Kirosha
"""

from datetime import datetime, timedelta

from core.event_bus import Event, EventBus, Topics
from core.models import (
    Confidence, Disruption, DisruptionKind, FlightStatus, FlightStatusChanged, Severity, Trip, airport_of, local_time, log,
)
from core.notification_service import notify_participants
from core.trip_plan_service import TripRepository

def _disruption_id(trip_id: str, kind: DisruptionKind, segment_id: str, not_before: datetime) -> str:
    return f"{trip_id}:{kind.value}:{segment_id}:{int(not_before.timestamp() * 1000)}"

class RulesEngine:
    def __init__(self, bus: EventBus, trips: TripRepository, min_reported_delay: int = 15):
        self.__bus = bus
        self.__trips = trips

        # a delay shorter than this is not worth a notification
        self.min_reported_delay = min_reported_delay

        self.__seen: set[str] = set()

        bus.subscribe(Topics.FLIGHT_STATUS_CHANGED, self.__on_flight_changed, name="rules")

    def detect(self, trip: Trip) -> list[Disruption]:
        """Pure function: what is broken in this trip right now"""
        result = []
        segments = trip.segments

        for i, a in enumerate(segments):
            if a.status == FlightStatus.CANCELLED:
                result.append(Disruption(
                    id=_disruption_id(trip.id, DisruptionKind.CANCELLATION, a.id, a.depart),
                    trip_id=trip.id,
                    kind=DisruptionKind.CANCELLATION,
                    replace_segment_id=a.id,
                    not_before=a.depart,
                    detail=f"flight {a.flight_no} cancelled",
                ))
                continue

            if i + 1 >= len(segments): continue
            b = segments[i + 1]
            if b.status == FlightStatus.CANCELLED or a.destination != b.origin: continue

            # connection = time between the real arrival of a and the real departure of b
            mct = airport_of(a.destination).mct_minutes
            gap = int((b.actual_depart - a.actual_arrive).total_seconds() // 60)
            if gap >= mct: continue

            earliest = a.actual_arrive + timedelta(minutes=mct)
            result.append(Disruption(
                id=_disruption_id(trip.id, DisruptionKind.MISSED_CONNECTION, b.id, earliest),
                trip_id=trip.id,
                kind=DisruptionKind.MISSED_CONNECTION,
                replace_segment_id=b.id,
                not_before=earliest,
                detail=f"connection in {a.destination} between {a.flight_no} and {b.flight_no}: {gap} min, MCT {mct} min",
            ))

        return result

    async def __on_flight_changed(self, event: Event) -> None:
        change: FlightStatusChanged = event.payload
        segment = change.segment

        for trip in self.__trips.find_by_segment(segment.id):
            key_base = f"{trip.id}:{segment.id}:{segment.fingerprint}:{segment.confidence.name.lower()}"

            # Default: a low-confidence status only gives a soft heads-up. No disruption, no rebooking:
            # we do not spend money or touch the plan on a rumour.
            if segment.confidence == Confidence.LOW:
                await notify_participants(
                    self.__bus, trip, key_base, Severity.INFO,
                    f"Possible delay of {segment.flight_no} (~{segment.delay_minutes} min), unconfirmed — checking.",
                )
                continue

            # do not rely on the order of handlers: evaluate the trip with this status applied
            disruptions = self.detect(trip.replace_segment(segment))

            if not disruptions:
                if segment.status == FlightStatus.DELAYED and segment.delay_minutes >= self.min_reported_delay:
                    await notify_participants(
                        self.__bus, trip, key_base, Severity.INFO,
                        f"{segment.flight_no} is delayed by {segment.delay_minutes} min, "
                        f"arrival {local_time(segment.actual_arrive, segment.destination)}. Connections are fine.",
                    )
                continue

            for disruption in disruptions:
                if disruption.id in self.__seen: continue
                self.__seen.add(disruption.id)

                log("rules", f"DISRUPTION ({disruption.kind.value}): {disruption.detail}")
                await notify_participants(
                    self.__bus, trip, f"{disruption.id}:alert", Severity.CRITICAL,
                    f"Trip problem: {disruption.detail}. Searching alternatives.",
                )
                await self.__bus.publish(Topics.TRIP_DISRUPTED, disruption.id, disruption)
