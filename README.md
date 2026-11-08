<h1 align="center">travel_tech_case</h1>

<p align="center"><em>Flight tracking and auto-rebooking for a travel planning service</em></p>

<div align="center">

[![Documentation](https://img.shields.io/badge/docs-online-4ade80?style=flat-square)](https://example.com/travel_tech_case/docs/)
[![Python](https://img.shields.io/badge/language-Python-60a5fa?style=flat-square)]()
[![asyncio](https://img.shields.io/badge/runtime-asyncio-f87171?style=flat-square)]()
[![stdlib](https://img.shields.io/badge/deps-stdlib%20only-a87ef5?style=flat-square)]()

</div>

---

A demo of the full flow: track flights, detect disruptions, find alternatives, rebook via a saga, keep the shared trip plan up to date, notify users, and protect passport data in a vault. Everything is in memory, no external services.

```bash
python main.py                  # scenario A: a delay breaks a 60 min connection
python main.py --fail-payment   # same, but the best offer is declined by the bank → saga compensation
```

No installation needed. `pyproject.toml` exists for `pip install -e .` (`import core`).

---

## Architecture

Services do not know about each other; they talk only through the Event Bus (topics `flight.status.changed`, `trip.disrupted`, `trip.updated`, `notif.requested`, `booking.confirmed`).

- **Tracker** turns provider responses into normalized statuses with `confidence` and drops duplicates.
- **RulesEngine** checks connections against MCT on a confirmed status and publishes `trip.disrupted`.
- **RebookingEngine** applies hard filters (baggage, visas, MCT), ranks the rest, and rebooks the best option via the saga `hold → charge → issue` with compensations.
- **TripPlanService** keeps the shared plan up to date with optimistic locking and roles.
- **NotificationService** goes idempotently through push → email → SMS and accumulates an offline queue with a sync cursor.
- **Vault** stores passports only. Only `passport_ref` goes outside; every read is audited; TTL = end of trip + 7 days.

---

## Structure

```
travel_tech_case/
├── pyproject.toml
├── README.md
├── main.py                          demo of scenario A (+ offline, API failure, roles, Vault)
└── core/
    ├── __init__.py
    ├── models.py                    models, airports (MCT, TZ), event payloads
    ├── event_bus.py                 bus: topics, idempotent publish, retry, DLQ
    ├── circuit_breaker.py           CircuitBreaker + retry_with_backoff
    ├── flight_tracker.py            FlightAdapter, MockFlightAdapter, FlightTrackerService
    ├── rules_engine.py              cancellation and broken-connection detection
    ├── notification_service.py      channel chain, offline queue, sync cursor, DeviceCache
    ├── rebooking_engine.py          search, filters, ranking, saga, mocks
    ├── trip_plan_service.py         TripRepository (version), TripPlanService (roles)
    └── vault.py                     passport store with TTL and audit
```

---

## What the demo shows

1. Plan: SVO → IST (TK412) → BCN (TK1855), 60 min connection, MCT in IST is 45 min.
2. `poll 1`: the first request times out, the retry gets a mailing "50 min delay" (`confidence=low`). This is a soft info notification; the plan and the money are not touched.
3. `poll 2`: the API confirms the delay (`confidence=high`). The 10 min connection is below the 45 min MCT, so it is a disruption. Of 6 candidates, TK1857 (departs before the MCT), PC1272 (no baggage tariff) and BA675+BA478 (GB transit without a visa) are dropped. TK1859, TK1861 and IB3201+IB1234 remain. The best (TK1859) is auto-rebooked, the plan is updated, "Sagrada Família" is marked "at risk".
4. `poll 3`: the same status from the API is dropped as a duplicate.
5. `poll 4`: the API is down; after 3 errors the breaker opens, the tracker stays on the last known status, polls go to the DLQ.
6. u2 (viewer) goes offline: push is queued, critical messages are also sent to email; on `sync` the client receives everything by cursor. A viewer cannot edit the plan; a stale version is rejected. Vault read audit is visible, reads are denied after TTL.

With `--fail-payment`, the payment for TK1859 is declined: the saga releases the hold (compensation) and moves to the next option, TK1861.

Time is stored and compared in UTC (`datetime` with `tzinfo=utc`); local airport time is used only in messages.

---

## Constraint → Solution

| Constraint | How the code handles it |
|---|---|
| Data arrives late; confidence and dedup needed | `flight_tracker.py`: source `MAILING` → `LOW`, `API` → `HIGH`. A duplicate by `fingerprint` (status + delay) is dropped, except a confidence upgrade low → high. An observation older than the accepted one (`observed_at`) is ignored. `event_bus.py`: an event key is never delivered twice. A low status is not written to the plan and does not start rebooking. |
| The user may be offline | `notification_service.py`: push → `OFFLINE` puts the message in a queue, `INFO` stops there, `CRITICAL` continues to email and SMS. `sync(user_id, cursor)` returns what was missed; `DeviceCache` keeps the cursor and local cache and dedups by id. |
| Baggage, visas, time zones | `rebooking_engine.py` `__reject_reason`: tariff ≥ the strictest participant's baggage, transit countries allowed for every participant, departure not before "arrival + MCT" and MCT inside the offer. All time in UTC, local only for texts (`local_time`). |
| Do not keep passports longer than needed | `vault.py`: TTL = end of trip + 7 days, `purge_expired()`, only `passport_ref` goes outside, every read (including a denial) is audited, audit contains no passport data. The saga reads passports only on the `hold` step; the gateway masks the number in logs. |
| External APIs are unreliable | `circuit_breaker.py`: closed → open → half-open, `retry_with_backoff` with exponential pause. Tracker and search go through them. Fallback: last known status. DLQ: polls (`FailedPoll`) and events (`bus.dead_letters`). The saga compensates steps in reverse. |
| Multi-user mode | `models.py`: roles `OWNER/EDITOR/VIEWER` in `participants`. `trip_plan_service.py`: `save(trip, expected_version)` raises `VersionConflict`, background updates do read-modify-write with 3 retries, user edits check role and version. |

Patterns: adapter (`FlightAdapter`, gateways), saga (`Saga`, `SagaStep`), outbox/idempotency (event, notification, hold, and charge keys), circuit breaker, optimistic locking.

---

## What to replace in prod

| Now (in-memory) | In prod | Why |
|---|---|---|
| `EventBus` | Kafka (same topics, partition key = `trip_id`) | persistence, per-trip order, replay |
| Idempotency and outbox in `set` | transactional outbox in PostgreSQL + `processed_events` table | event and data change in one transaction |
| `TripRepository` | PostgreSQL: `trips`, `trip_participants`, `version` column | `UPDATE ... WHERE version = ?` |
| Offline notification queue and cursor | PostgreSQL or Redis Streams with TTL | survives restart, cursor = position in the stream |
| `__known` in tracker and breaker state | Redis | shared state between instances |
| `VaultService` | KMS/HSM + encrypted store, TTL job, audit in append-only log | encryption, key rotation, immutable audit |
| `DeadLetter`, `FailedPoll` | Kafka DLQ topic + alerts | manual triage and replay |
| Mock adapters, search, gateways | real airline APIs, GDS, payment provider | replaced behind `ABC` interfaces without touching the core |
| `poll()` | scheduled workers + provider webhooks | do not waste quota, reduce latency |

---

## Defaults

- Low confidence (`LOW`) produces an info notification only. Disruption, plan update and rebooking require `HIGH`.
- Auto-rebook works only when `auto_rebook_budget_eur > 0` and the option's price is within budget (400 EUR in the demo). Default is 0: offers only.
- Price (`price_eur`) is for the whole party; baggage is per passenger. Transit is checked for every participant.
- Ranking: score = minutes of extra delay + 0.5 × EUR + 90 × stops, lower is better.
- MCT and airport time zones are demo values in `models.py`; an unknown airport gets MCT 90 min, UTC+0.
- Visas and countries are fictional for the demo and are not legal advice.
- A delay shorter than 15 minutes does not trigger a notification.
- If an activity starts after departure but before arrival, it is marked "at risk" and participants are notified.
- Private methods and attributes use the `__` prefix, same as in snitch.

---

## Three theses

1. **Do not trust the first rumour, do not spend money on it.** Every status carries `confidence` and observation time, duplicates and stale data are dropped, and rebooking and payments start only from confirmed data.
2. **Every side effect is idempotent and reversible.** Event, notification, hold and charge keys prevent double processing, and the saga rolls back what was already done in reverse: the user is not charged twice and is not left with a dangling booking.
3. **Minimum data and minimum trust in the outside world.** Passports live in the Vault only until the end of the trip + grace and are handed out by reference with an audit, and API failures are localized by a breaker, retry, fallback to the last known status, and a DLQ.

---

## Contributing

Contributions are welcome — open an issue or PR.

- **Keep it small.** This is a demo, not a platform. New behavior should be reachable from `main.py` and fit in one file under `core/`.
- **Match the style.** `__` for private members, one class per responsibility, no imports outside stdlib.
- **One thing per PR.** Focused PRs merge faster.

If you're unsure whether an idea fits, open an issue first.

---

<p align="center"><sub>
<a href="https://example.com/travel_tech_case/docs/">Docs</a> ·
<a href="https://github.com/example/travel_tech_case/issues">Issues</a> ·
<a href="https://github.com/example/travel_tech_case/releases">Releases</a>
</sub></p>