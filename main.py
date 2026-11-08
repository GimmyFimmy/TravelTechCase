"""
    Desc: Demo of scenario A: a delay breaks a 60 minute connection, alternatives are found and rebooked
    Creator: Kirosha

    Run:  python main.py
          python main.py --fail-payment   (the best offer is declined → saga compensation)
"""

import asyncio
import sys
from datetime import datetime, timedelta

from core.circuit_breaker import CircuitBreaker
from core.event_bus import EventBus, Topics
from core.flight_tracker import FlightTrackerService, MockFlightAdapter, RawFlightUpdate, UpdateSource
from core.models import (
    Confidence, FlightSegment, FlightStatus, NotificationRequest, Participant, PlanItem, Role, Severity, Trip,
    TripUpdated, hhmm, local_time, log, utc,
)
from core.notification_service import ChannelKind, DeviceCache, MockChannel, NotificationService
from core.rebooking_engine import MockBookingGateway, MockFlightSearch, MockPaymentGateway, RebookingEngine
from core.rules_engine import RulesEngine
from core.trip_plan_service import PermissionDenied, TripPlanService, TripRepository, VersionConflict
from core.vault import VaultAccessDenied, VaultService

class Clock:
    """A fixed clock keeps the demo reproducible"""

    def __init__(self, now: datetime):
        self.now = now

    def __call__(self) -> datetime:
        return self.now

def section(title: str) -> None:
    print(f"\n=== {title} ===")

def describe_trip(trip: Trip) -> str:
    lines = [f"trip {trip.id} v{trip.version}"]

    for s in trip.segments:
        lines.append(
            f"  {s.flight_no} {s.origin}→{s.destination}  "
            f"{local_time(s.actual_depart, s.origin)} → {local_time(s.actual_arrive, s.destination)}  "
            f"[{s.status.value} +{s.delay_minutes}]"
        )
    for item in trip.items:
        lines.append(f"  • {item.title} {hhmm(item.start)}Z{'  ⚠ под угрозой' if item.at_risk else ''}")

    return "\n".join(lines)

async def main() -> None:
    fail_payment = "--fail-payment" in sys.argv[1:]
    clock = Clock(utc(2026, 11, 10, 6, 0))

    # ── infrastructure ──
    bus = EventBus(clock=clock)
    vault = VaultService(clock=clock)
    trips = TripRepository()

    # ── passports go to the Vault, the trip keeps only references ──
    trip_end = utc(2026, 11, 17, 18, 0)
    ref_u1 = vault.put("u1", "t1", "RU-5512-340981", trip_end)
    ref_u2 = vault.put("u2", "t1", "RU-5512-340982", trip_end)

    # ── trip: SVO → IST (TK412) → BCN (TK1855), the connection is 60 min ──
    tk412 = FlightSegment(
        id="TK412-20261110", flight_no="TK412", origin="SVO", destination="IST",
        depart=utc(2026, 11, 10, 8, 0), arrive=utc(2026, 11, 10, 11, 0),
        status=FlightStatus.SCHEDULED, delay_minutes=0, confidence=Confidence.HIGH,
    )
    tk1855 = FlightSegment(
        id="TK1855-20261110", flight_no="TK1855", origin="IST", destination="BCN",
        depart=utc(2026, 11, 10, 12, 0), arrive=utc(2026, 11, 10, 15, 30),
        status=FlightStatus.SCHEDULED, delay_minutes=0, confidence=Confidence.HIGH,
    )

    trips.create(Trip(
        id="t1",
        version=1,
        end=trip_end,
        participants=[
            Participant("u1", Role.OWNER, ref_u1, frozenset({"TR", "ES"}), baggage_kg=23),
            Participant("u2", Role.VIEWER, ref_u2, frozenset({"TR", "ES"}), baggage_kg=23),
        ],
        segments=[tk412, tk1855],
        items=[
            PlanItem("a1", "Sagrada Família", utc(2026, 11, 10, 17, 0), utc(2026, 11, 10, 19, 0)),
            PlanItem("a2", "Park Güell", utc(2026, 11, 11, 10, 0), utc(2026, 11, 11, 12, 0)),
        ],
        # default is 0 (offers only); here auto-rebook is on, up to 400 EUR for the party
        auto_rebook_budget_eur=400,
    ))

    # ── external API(s): the first poll times out once, then the mailing arrives before the API ──
    def update(flight_no: str, code: str, delay: int, minutes: int, source: UpdateSource) -> RawFlightUpdate:
        return RawFlightUpdate(flight_no, code, delay, utc(2026, 11, 10, 6, 0) + timedelta(minutes=minutes), source)

    adapter = MockFlightAdapter({
        "TK412": [
            TimeoutError("upstream timeout"),
            update("TK412", "DELAYED", 50, 1, UpdateSource.MAILING),
            update("TK412", "DELAYED", 50, 5, UpdateSource.API),
            update("TK412", "DELAYED", 50, 6, UpdateSource.API),
            TimeoutError("upstream timeout"),
            TimeoutError("upstream timeout"),
            TimeoutError("upstream timeout"),
        ],
        "TK1855": [update("TK1855", "ON_TIME", 0, 1, UpdateSource.API)],
    })

    # ── services; the order of subscriptions is the order of handlers ──
    api_breaker = CircuitBreaker("flight-api", failure_threshold=3, open_for=timedelta(seconds=30), clock=clock)
    tracker = FlightTrackerService(bus, adapter, api_breaker)
    plan = TripPlanService(bus, trips)
    RulesEngine(bus, trips)
    RebookingEngine(
        bus, trips,
        search=MockFlightSearch(),
        booking=MockBookingGateway(),
        payment=MockPaymentGateway(decline_suffixes={":TK1859"} if fail_payment else set()),
        vault=vault,
        clock=clock,
    )

    # u2 has no connection: push is queued, critical messages escalate to e-mail
    notifications = NotificationService(
        bus,
        chain=[
            MockChannel(ChannelKind.PUSH, offline_users={"u2"}),
            MockChannel(ChannelKind.EMAIL),
            MockChannel(ChannelKind.SMS),
        ],
        clock=clock,
    )

    async def print_update(event) -> None:
        updated: TripUpdated = event.payload
        log("plan", f"trip.updated: {updated.trip_id} v{updated.version} — {updated.reason}")

    bus.subscribe(Topics.TRIP_UPDATED, print_update, name="demo-printer")

    tracker.track(trips.require("t1").segments)

    section("исходный план")
    print(describe_trip(trips.require("t1")))

    section("poll 1: таймаут → retry → рассылка (low confidence)")
    await tracker.poll()

    section("poll 2: API подтвердило задержку (high confidence) → дистрапшн → альтернативы → переоформление")
    await tracker.poll()

    section("poll 3: тот же статус ещё раз → дедупликация")
    await tracker.poll()

    section("poll 4: API лежит → breaker открывается → fallback на последний известный статус")
    await tracker.poll()
    print(f"breaker flight-api: {api_breaker.state.value}, DLQ опросов: {len(tracker.dead_letters)}")

    section("итоговый план")
    print(describe_trip(trips.require("t1")))

    section("повторная доставка того же события (идемпотентность)")
    duplicate = NotificationRequest(key="k1", user_id="u1", trip_id="t1", severity=Severity.INFO, text="тест")
    await bus.publish(Topics.NOTIF_REQUESTED, "t1:rebooked-dup", duplicate)
    await bus.publish(Topics.NOTIF_REQUESTED, "t1:rebooked-dup", duplicate)

    section("оффлайн: u2 открывает приложение и синхронизируется")
    device = DeviceCache("u2")
    device.sync_from(notifications)
    device.sync_from(notifications)

    section("роли и оптимистичная блокировка")
    extra = PlanItem("a3", "Ужин", utc(2026, 11, 11, 18, 0), utc(2026, 11, 11, 20, 0))
    version = trips.require("t1").version

    for label, user_id, expected_version in [
        ("viewer u2", "u2", version),
        ("u1 со старой версией", "u1", version - 1),
        ("u1 с актуальной версией", "u1", version),
    ]:
        try:
            saved = await plan.add_activity("t1", user_id, expected_version, extra)
            log("demo", f"{label}: ok → v{saved.version}")
        except (PermissionDenied, VersionConflict) as err:
            log("demo", f"{label}: {type(err).__name__}: {err}")

    section("Vault: аудит и TTL")
    for record in vault.audit_log:
        log("vault", f"{hhmm(record.at)}Z {record.ref} {record.actor} «{record.purpose}» {'выдано' if record.granted else 'отказано'}")

    clock.now = trip_end + timedelta(days=8)
    try:
        vault.read(ref_u1, actor="demo", purpose="после конца поездки")
    except VaultAccessDenied as err:
        log("vault", f"VaultAccessDenied: {err}")

    section("DLQ шины")
    print(f"необработанных событий: {len(bus.dead_letters)}")

if __name__ == "__main__":
    asyncio.run(main())
