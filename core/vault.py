"""
    Desc: Vault for passport data, TTL = end of the trip + grace, only passport_ref goes outside
    Creator: Kirosha
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

from core.models import log

class VaultAccessDenied(Exception):
    def __init__(self, ref: str):
        super().__init__(f"нет данных по {ref} (не найдено или срок хранения истёк)")

@dataclass(frozen=True)
class _VaultEntry:
    user_id: str
    trip_id: str
    passport_number: str
    expires_at: datetime

@dataclass(frozen=True)
class AuditRecord:
    """Audit never contains the passport data, only who, why and the result"""
    at: datetime
    ref: str
    actor: str
    purpose: str
    granted: bool

class VaultService:
    def __init__(self, grace: timedelta = timedelta(days=7), clock: Callable[[], datetime] | None = None):
        # default: passport lives 7 days after the end of the trip
        self.grace = grace
        self.__clock = clock or (lambda: datetime.now(timezone.utc))

        self.__entries: dict[str, _VaultEntry] = {}
        self.__audit: list[AuditRecord] = []
        self.__seq = 0

    @property
    def audit_log(self) -> list[AuditRecord]:
        return list(self.__audit)

    def put(self, user_id: str, trip_id: str, passport_number: str, trip_end: datetime) -> str:
        """Returns passport_ref, an opaque reference. In prod it is a random id, here a counter."""
        self.__seq += 1
        ref = f"pp_{self.__seq:06d}"
        self.__entries[ref] = _VaultEntry(user_id, trip_id, passport_number, trip_end + self.grace)
        return ref

    def read(self, ref: str, actor: str, purpose: str) -> str:
        """Every read is audited, including the denied ones"""
        self.purge_expired()

        entry = self.__entries.get(ref)
        self.__audit.append(AuditRecord(at=self.__clock(), ref=ref, actor=actor, purpose=purpose, granted=entry is not None))

        if entry is None: raise VaultAccessDenied(ref)
        return entry.passport_number

    def purge_expired(self) -> int:
        """Removes everything past its TTL, in prod it runs by a schedule as well"""
        now = self.__clock()
        expired = [ref for ref, entry in self.__entries.items() if now >= entry.expires_at]

        for ref in expired:
            del self.__entries[ref]

        if expired: log("vault", f"удалено по TTL: {len(expired)}")
        return len(expired)
