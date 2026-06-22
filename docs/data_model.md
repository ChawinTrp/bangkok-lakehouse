# Data Model — Gold (star schema)

The gold layer is a Kimball star: two fact tables surrounded by three conformed
dimensions. Built from silver by `spark/transforms/gold_traffy.py`, written to
`data/gold/` as Parquet.

```
            dim_date
               │
dim_district ──┼── fact_ticket_lifecycle      (grain: one ticket)
               │
dim_district ──┼── fact_district_daily ── dim_category   (grain: district × category × day)
               │
            dim_date
```

`dim_date` and `dim_district` are **conformed** — shared by both facts, so the two
marts can be filtered and compared on the same keys.

## Dimensions (natural keys)

| Dim | Grain | PK | Notes |
|---|---|---|---|
| `dim_district` | one district | `district` | Derived from silver (distinct, non-null). |
| `dim_category` | one problem category | `category` | Raw Thai value; English normalization deferred. |
| `dim_date` | one calendar day | `date` | **Generated**, not sourced. Spans first reported day → latest reported-or-finished day. Attributes: year, month, day, day_of_week, week_of_year, is_weekend, month_name, quarter. |

**Key decision — natural keys, not surrogate keys.** At ~50 districts / a few dozen
categories, surrogate-key machinery buys nothing and adds join indirection. Trade-off
accepted: a renamed district would break FKs, and there's no clean SCD Type 2 history.

**Key decision — `dim_district` derived from silver (for now).** Honest limitation:
because the dimension comes from the same data as the facts, a "district ∈ dim_district"
referential check can't truly fail, and a source typo would create a bogus district row.
Upgrade path: a curated 50-khet reference list makes that check meaningful.

## Facts

### `fact_ticket_lifecycle` — accumulating snapshot
- **Grain:** one row per ticket.
- `ticket_id` is a **degenerate dimension** (lives in the fact, no dim table).
- **FKs:** `district` → dim_district, `reported_date` → dim_date.
- Category is deliberately **absent** — a ticket has many categories, so it would
  break the one-ticket grain (category analysis lives in the other fact).
- **Milestones:** `reported_at`, `in_progress_at`, `finished_at` fill in over the
  ticket's life. **Measures:** `days_to_resolve`, `is_resolved`, `is_reopened`.
- **Build:** a projection of silver (already one current row per ticket). The snapshot
  **self-heals** on each run — silver always holds the latest state, so a full recompute
  is idempotent. At scale this would be a Delta `MERGE` keyed on `ticket_id`.

### `fact_district_daily` — periodic snapshot (dense)
- **Grain:** one row per (district × category × day).
- **Dense:** a row exists for every grid cell, even on quiet days — so `backlog`
  carries across days where nothing happened, and trends need no gap-filling.
- **FKs:** `district`, `category`, `date` (all conformed).
- **Measures and additivity:**
  - `opened`, `closed` — **additive** (sum across any dimension).
  - `backlog` (cumulative opened − closed) — **semi-additive**: sum across
    district/category ✓, across days ✗ (double-counts standing tickets).
  - `median_resolution_time` (median `days_to_resolve` of tickets finished that day)
    — **non-additive**; null where nothing closed.
- **Build:** per-day `opened`/`closed` counts joined onto the dense
  (district × category × date) grid; `backlog` is a window cumulative sum over date
  per (district, category).

## Proof (`spark/proof_queries.py`)
1. **Top districts by unresolved flooding backlog** (latest day) — from `fact_district_daily`.
2. **Median resolution time by district** — from `fact_ticket_lifecycle`.

## Current data limitation (honest)
The historical bulk seed isn't loaded yet, so `dim_date` currently spans only the few
days of live polling done so far. The model is complete; the date range widens once the
monthly-archive seed lands.
