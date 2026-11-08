"""
    Desc: Models of the trip, flights, participants, itineraries and event payloads
    Creator: Kirosha
"""

from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum, IntEnum

def log(who: str, msg: str) -> None:
    print(f"[{who}] {msg}")

class Confidence(IntEnum):
    """LOW = unconfirmed source (mailing), HIGH = airline API"""
    LOW = 0
    HIGH = 1

class FlightStatus(Enum):
    SCHEDULED = "scheduled"
    DELAYED = "delayed"
    CANCELLED = "cancelled"
    DEPARTED = "departed"
    LANDED = "landed"

class Role(Enum):
    OWNER = "owner"
    EDITOR = "editor"
    VIEWER = "viewer"

class Severity(Enum):
    INFO = "info"
    CRITICAL = "critical"

class DisruptionKind(Enum):
    CANCELLATION = "cancellation"
    MISSED_CONNECTION = "missed_connection"

# ───────────────────────────── time helpers ─────────────────────────────
# Everything is stored and compared in UTC, local time is used for messages only.

def utc(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)

EPOCH = utc(1970, 1, 1)

def hhmm(time: datetime) -> str:
    return f"{time.hour:02d}:{time.minute:02d}"

def local_time(moment: datetime, airport_code: str) -> str:
    airport = airport_of(airport_code)
    return f"{hhmm(moment + timedelta(minutes=airport.utc_offset_minutes))} {airport_code}"

# ───────────────────────────── airports ─────────────────────────────

@dataclass(frozen=True)
class Airport:
    code: str
    country: str
    # minimum connection time, demo values
    mct_minutes: int
    utc_offset_minutes: int

AIRPORTS = {
    "SVO": Airport("SVO", "RU", 60, 180),
    "IST": Airport("IST", "TR", 45, 180),
    "BCN": Airport("BCN", "ES", 45, 60),
    "MAD": Airport("MAD", "ES", 40, 60),
    "LHR": Airport("LHR", "GB", 60, 0),
}

def airport_of(code: str) -> Airport:
    """Default for an unknown airport: MCT 90 min, UTC+0"""
    return AIRPORTS.get(code) or Airport(code, "??", 90, 0)

# ───────────────────────────── flights ─────────────────────────────

@dataclass(frozen=True)
class FlightSegment:
    id: str
    flight_no: str
    origin: str
    destination: str
    depart: datetime
    arrive: datetime
    status: FlightStatus
    delay_minutes: int
    confidence: Confidence
    # when the provider observed this status (not when we received it)
    observed_at: datetime = EPOCH

    @property
    def actual_depart(self) -> datetime:
        return self.depart + timedelta(minutes=self.delay_minutes)

    @property
    def actual_arrive(self) -> datetime:
        return self.arrive + timedelta(minutes=self.delay_minutes)

    @property
    def fingerprint(self) -> str:
        """Same fingerprint = same news, used for dedup"""
        return f"{self.status.value}|{self.delay_minutes}"

@dataclass(frozen=True)
class Itinerary:
    """A replacement offer: one or more segments with one tariff"""
    segments: list[FlightSegment]
    # price for the whole party, EUR
    price_eur: int
    # checked baggage allowed by the tariff, per passenger
    baggage_kg: int

    @property
    def id(self) -> str:
        return "+".join(s.flight_no for s in self.segments)

    @property
    def depart(self) -> datetime:
        return self.segments[0].depart

    @property
    def arrive(self) -> datetime:
        return self.segments[-1].arrive

    @property
    def stops(self) -> int:
        return len(self.segments) - 1

    @property
    def via_airports(self) -> list[str]:
        """Intermediate airports, transit countries are taken from them"""
        return [s.destination for s in self.segments[:-1]]

    def describe(self) -> str:
        start = local_time(self.depart, self.segments[0].origin)
        end = local_time(self.arrive, self.segments[-1].destination)
        via = f", пересадка: {', '.join(self.via_airports)}" if self.stops > 0 else ""
        return f"{self.id}: {start} → {end}, багаж {self.baggage_kg} кг, {self.price_eur} EUR{via}"

# ───────────────────────────── trip ─────────────────────────────

@dataclass(frozen=True)
class Participant:
    user_id: str
    role: Role
    # only a reference, the passport itself lives in the Vault
    passport_ref: str | None
    # countries where the participant may enter or transit (visas)
    allowed_countries: frozenset[str]
    baggage_kg: int

@dataclass(frozen=True)
class PlanItem:
    id: str
    title: str
    start: datetime
    end: datetime
    at_risk: bool = False

@dataclass(frozen=True)
class Trip:
    id: str
    # optimistic lock, bumped by the repository on every save
    version: int
    end: datetime
    participants: list[Participant]
    segments: list[FlightSegment]
    items: list[PlanItem] = field(default_factory=list)
    # auto-rebook is allowed up to this price (0 = off, only offers are sent)
    auto_rebook_budget_eur: int = 0

    def replace_segment(self, segment: FlightSegment) -> "Trip":
        return replace(self, segments=[segment if s.id == segment.id else s for s in self.segments])

    def participant_of(self, user_id: str) -> Participant | None:
        return next((p for p in self.participants if p.user_id == user_id), None)

    @property
    def required_baggage_kg(self) -> int:
        """The strictest baggage requirement among participants"""
        return max((p.baggage_kg for p in self.participants), default=0)

    @property
    def final_arrive(self) -> datetime:
        return self.segments[-1].actual_arrive

# ───────────────────────────── event payloads ─────────────────────────────

@dataclass(frozen=True)
class FlightStatusChanged:
    previous: FlightSegment
    segment: FlightSegment

@dataclass(frozen=True)
class Disruption:
    id: str
    trip_id: str
    kind: DisruptionKind
    # the segment that has to be replaced
    replace_segment_id: str
    # earliest departure of a replacement from the origin of that segment
    not_before: datetime
    detail: str

@dataclass(frozen=True)
class NotificationRequest:
    # idempotency key
    key: str
    user_id: str
    trip_id: str
    severity: Severity
    text: str

@dataclass(frozen=True)
class BookingConfirmed:
    trip_id: str
    disruption_id: str
    replaced_segment_id: str
    itinerary: Itinerary
    ticket_ref: str

@dataclass(frozen=True)
class TripUpdated:
    trip_id: str
    version: int
    reason: str
    at_risk_titles: list[str]
