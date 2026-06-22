# Bangkok Civic-Data Lakehouse — Engineering Notes for Learning

A companion to the code: why we chose what we chose, what we considered and rejected, and the bugs we hit and fixed. Written so that future-me — or a system-design interviewer — can read it and follow the reasoning, not just the file list.

The project is an incremental medallion lakehouse over [Traffy Fondue](https://www.traffy.in.th/), Bangkok's live citizen-complaint platform (~1.3M geolocated tickets with a real status lifecycle). The thesis question: *which districts are underserved by city services?* The engineering point: exercise **incremental processing** — watermark-style ingestion, snapshot facts, quality gates — the things a static full-refresh pipeline never touches.

## Table of contents
1. Stack choices
2. Data modeling decisions
3. Architectural patterns
4. Integration semantics — the Traffy API
5. The ticket state machine
6. Configuration management
7. Bugs we hit and their lessons
8. What we deliberately did NOT build
9. System design interview cheatsheet

---

## 1. Stack choices

**Orchestration — Airflow on docker-compose, not Cloud Composer.** Airflow is the standard the job market asks for, and the skill we wanted to practice is *DAG design* — dependencies, retries, idempotency, backfills — not paying for managed infrastructure. Composer runs ~$300+/month; docker-compose (Postgres + LocalExecutor) is free and exercises the identical DAG-authoring surface. Cost accepted: we manage our own Airflow upgrades and there's no managed HA. Lesson: match the tool to the *skill being practiced*, not to the most production-grade option.

**Transform engine — PySpark.** The dataset is ~1.3M rows and the resume needs the word "Spark" to be honest. Pandas would handle this volume fine today, so the genuine justification is forward-looking: the transforms (dedup over a window, explode, cumulative backlog) are exactly the operations that stop fitting in memory as history accumulates, and writing them in Spark now means they don't get rewritten later. Cost accepted: Spark's startup and shuffle overhead is real pain at this tiny scale (see the bugs section).

**Storage — local Parquet, behind a seam.** Phases 1–3 write Parquet to local disk; GCS/BigQuery is Phase 4. All storage goes through `include/storage.py` so flipping the backend touches one file. This is deliberate: a free weekend is never blocked on GCP billing/IAM setup, and the medallion logic is provably storage-agnostic. Lesson: put the cloud dependency behind an interface and you can build and test the whole pipeline offline.

**Spark runtime — Docker, not local.** Local PySpark on Windows is blocked by the winutils/NativeIO Hadoop issue. Rather than fight it, Spark runs in a `bangkok-spark` container. Spark tests are marked `@pytest.mark.spark` and skipped locally (`addopts = -m 'not spark'`), run in-container in CI/dev. Cost accepted: a two-tier test story (fast non-Spark locally, Spark in Docker). Lesson: when a platform fights you, containerize the painful dependency instead of bending your OS to it.

## 2. Data modeling decisions

**The medallion split is a contract boundary, not just folders.** Bronze is reshape-only (raw, duplicated, string timestamps); silver is the trusted, deduped, typed view; gold is the star schema for analytics. The non-obvious part is that bronze *deliberately guarantees almost nothing* — it lists its non-guarantees (duplicates across partitions, string timestamps, possible null geo) as a mirror image of what silver promises to fix. Writing the bronze→silver promises down as `docs/contracts.md` turns "silver is clean" from a vibe into a checkable claim. See `docs/contracts.md`.

**Gold is a Kimball star with natural keys.** Three conformed dimensions (`dim_district`, `dim_category`, `dim_date`) and two facts. We chose **natural keys** (district name, category, the date itself) over surrogate integer keys. At ~50 districts and a few dozen categories, surrogate-key machinery buys nothing and adds join indirection; building it would have been cargo-culting the textbook. Cost accepted honestly: a renamed district breaks foreign keys, and there's no clean SCD Type 2 history path. The road not taken (surrogate keys) is the right call at 100× this dimension size — the lesson is that dimensional modeling rules scale *with* the data, and a 50-row dimension is not where you spend complexity budget.

**Two fact tables, two snapshot types — this is the heart of the model.**

- `fact_ticket_lifecycle` is an **accumulating snapshot**: grain is one row per ticket, with milestone timestamps (`reported_at`, `in_progress_at`, `finished_at`) that fill in over the ticket's life. `ticket_id` is a *degenerate dimension* — it lives in the fact with no dimension table because it has no attributes worth one. Category is deliberately **absent** here: a ticket has many categories, so putting it at ticket grain would shatter the one-ticket grain.
- `fact_district_daily` is a **dense periodic snapshot**: grain is district × category × day, with a row for *every* grid cell even on quiet days.

**Measure additivity drove the dense decision.** `backlog` (open tickets at end of day) is **semi-additive**: you can sum it across districts on a given day, but summing it across days double-counts standing tickets. For that running total to exist on days when nothing happened, every day needs a row — which is exactly why the snapshot is dense, not sparse. `median_resolution_time` is **non-additive** (you cannot sum or average medians), so it's stored per-cell and never pre-aggregated. Getting additivity right at modeling time is what stops a dashboard from quietly lying.

## 3. Architectural patterns

**Thin DAGs, fat modules.** The Airflow DAG (`dags/traffy_ingest.py`) contains orchestration only — task wiring, schedule, retries. Every piece of real logic lives in `include/traffy.py` or `spark/`. This keeps the logic unit-testable without an Airflow runtime and means a DAG-integrity test can import the DAG fast. Lesson: orchestrators should orchestrate, not compute.

**The storage seam (shell pattern).** `include/storage.py` is the backend switch — local Parquet now, GCS/BigQuery in Phase 4. We built the seam before we needed it so the cloud migration is a one-file change rather than a refactor threaded through every transform.

**Spark-out-of-Airflow via DockerOperator (DooD).** The silver job doesn't run in the Airflow worker — Airflow *launches* the `bangkok-spark` container on the host daemon (Docker-out-of-Docker: the host `/var/run/docker.sock` is mounted into the Airflow containers, and `HOST_PROJECT_PATH` is passed through because bind mounts resolve on the *host* daemon, not inside Airflow). This keeps Airflow's image lean and isolates the heavy Spark dependency. The non-obvious gotcha: `mount_tmp_dir=False` is required under DooD or the operator tries to bind a worker-local tmp dir that doesn't exist on the host.

**Recompute over merge — for now.** The accumulating snapshot *wants* an upsert (update a ticket's row as it progresses). Plain Parquet can't update in place. Rather than reach for Delta Lake immediately, we recompute the whole fact from silver each run — silver already holds the latest state per ticket, so a full rebuild is idempotent and trivially correct at this scale. The honest senior answer: "at volume I'd use a Delta `MERGE` keyed on `ticket_id`; at this size a recompute is cheaper and simpler." The seam to swap it in is clean.

## 4. Integration semantics — the Traffy API

The single most useful thing we learned: **the live endpoint guarantees less than it looks like.** It's a GeoJSON `FeatureCollection` at `/teamchadchart-stat-api/geojson/v1`, and we verified its real behavior rather than trusting the docs:

- `limit` and `offset` both work (300 is a *default* page size, not a cap).
- It's ordered by `last_activity` **descending** — a reopened old ticket jumps back to the front.
- There is **no server-side date filter** — `start_date`/`end_date` are silently ignored; newest is always "now."
- Deep offsets (~1.3M) return HTTP 502, and the ordering shifts as tickets update mid-page.

**Consequence for design.** Because there's no date filter and the ordering is unstable at depth, you cannot replay full history through the live feed. So the architecture is *bulk seed (date-addressable monthly archive) + daily incremental from the live feed* — the production "initial load + CDC" shape. The reason is "no date filter + unstable ordering + deep-offset timeouts," **not** the "300-record cap" we originally assumed. Lesson: probe an external API's *actual* guarantees before you design around its documentation.

## 5. The ticket state machine

Traffy tickets move through a lifecycle, and silver normalizes the raw codes into a canonical vocabulary (`normalize_state`):

| Raw `state_type_latest` | Canonical `status` |
|---|---|
| `start` | `reported` |
| `inprogress` | `in_progress` |
| `finish` | `resolved` |
| `forward` | `forwarded` |
| `irrelevant` | `rejected` |

The deliberate decision: **an unrecognized or null code maps to a null `status`, never to a guessed bucket.**

```python
mapping = F.create_map([F.lit(x) for kv in STATE_MAP.items() for x in kv])
df.withColumn("status", mapping[F.col("state_type_latest")])
```

A new state Traffy introduces tomorrow surfaces as a null we can see and investigate, rather than being silently miscategorised as "resolved." In the real data this caught 3 tickets with states outside our map. Lesson: when normalizing an external enum, fail *visible*, not silent.

## 6. Configuration management

| Tier | Example | Lives in | When to change |
|------|---------|----------|----------------|
| Hardcoded | `BKK_BBOX`, `STATE_MAP` | Python constants | Code review + commit |
| Env var | `TRAFFY_API_URL`, `LAKEHOUSE_ROOT`, `STORAGE_BACKEND`, `HOST_PROJECT_PATH` | `.env` | Restart, no code change |
| (future) DB/Airflow Variable | freshness-SLA hour (Phase 4) | Airflow Variables | Live, via UI |

Precedence is the usual: a live setting beats an env var beats a hardcoded default. The bounding box and state map are hardcoded because changing them *is* a modeling decision that belongs in review; the API URL and storage root are env vars because they differ per environment without changing behavior.

## 7. Bugs we hit and their lessons

This is the most valuable section. None of these are sanitized — the wrong first guess is part of the story.

**Bug 1 — the non-idempotent global watermark.** *Symptom:* re-running an ingest for a past day would have produced different output each time. *Diagnosis:* the first design stored the watermark as mutable global state ("last cutoff seen"). *Root cause:* a re-run read a *moved* cutoff, so the same date didn't reproduce. *Fix:* make the window a pure argument fed from Airflow's `data_interval_start/end` — same date in, same rows out. *Lesson:* idempotency means the inputs fully determine the output; any hidden mutable state breaks replay. (This one was caught by reasoning about re-runs *before* it shipped — the best kind of catch.)

**Bug 2 — the 7-hour timezone skew.** *Symptom:* partitions landed under the wrong date. *Root cause:* Airflow's `data_interval_*` are UTC-tagged; formatting them directly produced a 7-hour skew against Bangkok time. *Fix:* `.in_timezone("Asia/Bangkok")` before formatting the partition date. *Lesson:* orchestrator timestamps are UTC by contract — convert at the boundary, every time.

**Bug 3 — the live feed isn't date-addressable.** *Symptom:* a scheduled run for a completed past interval fetched *current* data and windowed it to empty. *Diagnosis path:* assumed the live feed behaved like a historical source; it doesn't — it only holds "now." *Root cause:* the `logical_date`-window idempotency pattern requires a date-addressable source, which the newest-N feed is not. *Fix:* the live DAG became a "today-so-far" snapshot (window `[today 00:00, now]`, overwrite `dt=today`); the `logical_date`/backfill treatment moved to the monthly-archive source. *Lesson:* idempotent backfill is a property of the *source*, not just your code — you can't replay history from a feed that only knows the present.

**Bug 4 — the empty partition that crashed Spark.** *Symptom:* the silver build died with `java.lang.IndexOutOfBoundsException: Index 0 out of bounds for length 0` deep in Parquet read. *Diagnosis path:* the first instinct was that the just-added `normalize_state` broke the shuffle — wrong. Isolating the pipeline step by step showed the failure was in *reading bronze*, before any transform. Listing the partitions showed `dt=2026-06-22/part-000.parquet` was 600 bytes vs 600KB for real days. *Root cause:* an empty-window run called `flatten_traffy([])`, which returns a *column-less* pandas DataFrame; written to Parquet it has no schema, and Spark's partitioned reader can't open it. *Fix (immediate):* drop the bad partition. *Fix (proper, tracked):* guard the writer to skip empty windows. *Lesson:* an empty result and a malformed result are different failures — handle "zero rows" explicitly at write time, and when isolating a bug, verify *which stage* fails before blaming the most recent change.

**Bug 5 — the date dimension that dropped closure days.** *Symptom:* a unit test for `fact_district_daily` failed — a day with a closure was missing from the output. *Root cause:* `dim_date` spanned only `min..max(reported)`, but tickets get *finished* after the last reported date, so closure days fell outside the calendar and the periodic snapshot silently dropped them. *Fix:* span the calendar to `max(reported, finished)`. *Lesson:* a generated dimension must cover the full range of *every* event that references it, not just the primary one — and a test on a tiny dataset caught what eyeballing 4,752 rows never would.

## 8. What we deliberately did NOT build

The discipline of saying no is half of system design. Each of these is a sentence we can defend in an interview, not a gap.

| Not built | Why not (yet) |
|-----------|---------------|
| Streaming (Kafka → Structured Streaming) | Batch D-1 is a *choice* — no consumer needs sub-day freshness. Scoped as a labelled v2 stretch. |
| Surrogate keys / SCD Type 2 | Over-engineering for ~50 stable districts; natural keys are defensible at this scale. |
| dbt for the gold layer | The transforms are few and pure PySpark; dbt's value (lineage, tests, docs) doesn't yet outweigh the added moving part. |
| Data lineage (OpenLineage/Marquez) | Worth it once there are many DAGs; premature for three transforms. |
| TOPSIS service-stress ranking | Deferred — prove the core star first, add the differentiator second. |
| A curated 50-khet `dim_district` | Currently derived from silver; the honest cost is that the referential check can't truly fail yet. Upgrade path is clear. |
| PM2.5 second source | One narrative (civic complaints) beats two half-built ones. |

## 9. System design interview cheatsheet

Using *this project* as the worked example.

**Clarify scope.** Functional: ingest a live civic-complaint feed daily, clean and conform it, expose marts answering "where is the backlog worst" and "how long do complaints take." Non-functional: daily freshness (not real-time), idempotent + replayable, cheap (free tier), correctness over latency.

**Data model.** Medallion bronze/silver/gold; gold is a star with conformed `dim_date`/`dim_district`, an accumulating-snapshot lifecycle fact, and a dense periodic-snapshot district mart. Lead with *grain* for each table — it's the answer to half of all follow-ups.

**Ingestion.** Bulk seed from a date-addressable archive + daily incremental from a newest-N feed (the "initial load + CDC" pattern), forced by the live API having no date filter. Watermark/window is client-side and idempotent.

**Quality.** A fail-closed gate between bronze and silver: `non_empty`, `not_null`, `unique`, `geo_bounds`, `rowcount_delta`. On failure nothing is written and the last good silver stays — a bad batch can't poison the trusted layer.

**Scaling discussion.**

| Scale | What breaks | Fix |
|-------|-------------|-----|
| ~1.3M tickets (today) | Nothing | Local Parquet + single-node Spark in Docker |
| ~10M | Local disk + full-table silver recompute slows | Move to GCS/BigQuery (the storage seam already exists); partition silver by date |
| ~100M | Full daily recompute of gold is wasteful | Incremental gold: Delta `MERGE` on the lifecycle fact keyed by `ticket_id`; only recompute affected snapshot days |
| ~1B / multi-city | Single Airflow + single Spark node | Partition by city/region; Spark on a managed cluster (Dataproc/Databricks jobs); consider streaming if a consumer finally needs sub-day freshness |

**Failure modes & mitigations.** Empty upstream window → skip the partition (don't write a malformed file). Late/reopened tickets → the accumulating snapshot self-heals because silver always holds the latest state. Upstream API 502 at depth → never paginate that deep; the monthly archive is the completeness backstop. Timezone skew → convert orchestrator UTC at the boundary.

**Observability tiers.** Today: quality-gate reports printed per run, Airflow task logs. Phase 4: a freshness SLA (yesterday's data landed + transformed by 06:00) with an alert on breach; on-failure callbacks. That's the honest "what I'd add next" answer.

---

**Closing thought.** The through-line of this codebase is *build for today, leave seams for tomorrow*: local storage behind an interface, recompute where merge isn't yet worth it, batch where streaming isn't needed — each shortcut is a labelled decision with a clear upgrade path, not an accident. The most-quoted line from the bugs above is the real lesson: an empty result and a malformed result are different failures, and idempotency is a property you design for, not one you hope for.
