# Data Contracts — Bronze & Silver

What each layer promises its consumers. A change that breaks a guarantee here is a breaking change.

## Layer overview

| Layer | Path | Grain | Engine |
|---|---|---|---|
| Bronze | `data/bronze/traffy/dt=<YYYY-MM-DD>/` | one row per ticket *per daily snapshot* | pandas (ingest DAG) |
| Silver — tickets | `data/silver/traffy_tickets/` | **one row per ticket** | Spark |
| Silver — categories | `data/silver/traffy_ticket_category/` | **one row per (ticket, category)** | Spark |

Format is Parquet throughout. A folder = a table; `part-*.parquet` files hold the data; an empty `_SUCCESS` marker means the write completed.

---

## Bronze — `traffy`

**Source:** Traffy Fondue live GeoJSON feed (`/teamchadchart-stat-api/geojson/v1`), newest-N snapshot. Paginated to cover the current Bangkok day; partition `dt` = the run's Bangkok date.

**Shape:** all source `properties` lifted to top-level columns, plus:
- `lon`, `lat` — split from `geometry.coordinates`
- `_ingested_at`, `_source` (`"traffy"`), `_run_id` — load metadata
- `dt` — partition column (Bangkok date)

**Guarantees**
- Data is **raw / reshape-only** — no cleaning, dedup, type-casting, or category normalization.
- Re-running a date **overwrites that `dt` partition only** → idempotent per partition.

**Explicitly NOT guaranteed (silver's job to fix)**
- Timestamps (`last_activity`, `timestamp`, `timestamp_finished`, …) are **strings**, not timestamp types.
- A ticket may appear in **many** partitions (it was active over multiple days) → duplicates across `dt`.
- `problem_type_fondue` is an **array** (a ticket can have several categories).
- Rows may have null/out-of-Bangkok coordinates.

---

## Silver — `traffy_tickets`

The trusted, current view. Everything downstream reads this, not bronze.

**Grain:** one row per `ticket_id` (the latest version, by `last_activity`).

**Guarantees**
- **Deduplicated** across all bronze partitions — `ticket_id` is unique.
- `ticket_id` is **non-null**.
- Coordinates (`lon`/`lat`) are **non-null** and **inside the Bangkok bounding box**.
- Timestamp columns are real **timestamp** types.
- A canonical **`status`** column is derived from the raw `state_type_latest` code (`start`→`reported`, `inprogress`→`in_progress`, `finish`→`resolved`, `forward`→`forwarded`, `irrelevant`→`rejected`). An unrecognised or null code yields a **null** `status` — drift surfaces rather than being silently miscategorised.
- Retains all bronze columns (incl. the raw `state_type_latest`) plus the parsed types and `status`.

## Silver — `traffy_ticket_category`

**Grain:** one row per `(ticket_id, category)` — `problem_type_fondue` exploded. Use this for "complaints by category"; never join it where you'd double-count tickets.

**Guarantees**
- A ticket with N categories yields N rows; a ticket with an empty/null array yields **0** rows.

---

## Bronze → Silver promotion (quality gate)

Before silver is written, the candidate is validated (`spark/quality/silver_checks.py`). All checks must pass:

| Check | Rule |
|---|---|
| `non_empty` | row count > 0 |
| `not_null` | `ticket_id`, `lon`, `lat` all non-null |
| `unique_ticket_id` | no duplicate `ticket_id` |
| `geo_bounds` | every row inside the Bangkok bbox |
| `rowcount_delta` | silver row count is `≤` the distinct bronze ticket count and `≥ 50%` of it — catches a dedup bug (too many) or a mass drop from an over-aggressive filter (too few). Skipped if no bronze baseline is passed. |

**On failure:** the gate raises, the job exits non-zero, and **nothing is written** — the existing silver is left untouched (failure blocks promotion; bronze remains the source of truth). Spark's schema-on-read is a second guard: a partition whose column types drift is rejected at read time.

---

## Freshness & rebuild semantics

- The live feed is a snapshot, so bronze's current-day partition is overwritten on each run (eventually-consistent for "today"; closed days are stable).
- Silver is **rebuilt from all bronze partitions** every run (`overwrite`), so it always reflects the full deduped history on disk.
- Authoritative completeness/backfill comes from the monthly archive source (separate), reconciled by the same `ticket_id` dedup.

## Consumers

- **Gold `fact_ticket_lifecycle`** ← `traffy_tickets` (per-ticket milestones, resolution time).
- **Gold `fact_district_daily`** ← `traffy_tickets` (+ `traffy_ticket_category` for per-category backlog).
