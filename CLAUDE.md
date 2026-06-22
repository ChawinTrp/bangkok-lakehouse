# CLAUDE.md — Bangkok Location-Intelligence Lakehouse

> Entry point for this repo. The phase-by-phase plan is [`docs/EXECUTION.md`](docs/EXECUTION.md); the bronze→silver promises are [`docs/contracts.md`](docs/contracts.md). This file is the quick orientation + the hard-won facts that aren't obvious from the code.
>
> **This is the live build repo.** The older `C:\Projects\bangkok-location-lakehouse\` folder is the abandoned pre-pivot POI version — ignore it.

## What this is

An **incremental medallion lakehouse** over **Traffy Fondue** — Bangkok's live citizen-complaint platform (~1.33M geolocated tickets, thousands/day, with a real status lifecycle). It is CT's **flagship portfolio project for entry-level Data Engineer interviews** in Bangkok (see memory `de-job-hunt-active`).

Thesis question: *"Which districts are underserved by city services, and how does that overlay site quality?"*

The point of the project is to genuinely exercise **incremental processing** (watermarks, bulk-seed + CDC, accumulating snapshots, freshness SLA) — the things a static full-refresh pipeline never touches.

## Working style here — LEARN, don't just build

CT is doing this to **learn DE for interviews**, not to ship fast (memory `ct-wants-to-understand`). For anything on this project:
- Teach the *why* in plain English; use analogies (the kitchen analogy for ELT, box-and-arrow for DAGs landed well).
- Walk through code line by line rather than dumping finished files.
- Have CT **hand-build at least one piece** himself.
- Same loop as before: declare each table's grain, write the test first (TDD), then the transform.
- The study material lives in the vault: `Obsidian/Second Brain/02 - Areas/Learning/DE Learning Module/` (8 modules). Module 01 = Lakehouse & Medallion Foundations; modelling (Phase 3) ≈ Module 04.

## Current state (2026-06-22)

- **Phases 0–2 DONE.** Bronze ingestion runs in Airflow; silver (PySpark) is built, quality-gated, contracted, and wired into the DAG.
- **Phase 1 (bronze):** live Traffy ingest as a "today-so-far" snapshot — `fetch_traffy_until(boundary)` paginates the newest-first feed back to today 00:00 (Asia/Bangkok), overwrites `dt=today`. The `logical_date`-interval idempotency + backfill belong to the (still-pending) monthly-archive source, not the live feed.
- **Phase 2 (silver):** `dedup_latest` (latest `last_activity` per `ticket_id`) → `parse_timestamps` → `filter_bangkok_bbox` → `normalize_state` (raw `state_type_latest` → canonical `status` via `STATE_MAP`; unknown/null → null) + a separate exploded `traffy_ticket_category` table. Quality gate (`non_empty`, `not_null`, `unique_ticket_id`, `geo_bounds`, `rowcount_delta`) runs **before** the write and blocks promotion on failure. 15 spark tests pass in the `bangkok-spark` container.
- **Next = Phase 3 (gold):** `dim_district` / `dim_category` / `dim_date` + `fact_ticket_lifecycle` (accumulating snapshot, grain = one ticket) + `fact_district_daily` (periodic snapshot, grain = district × category × day). Then `docs/data_model.md`.
- **Pending:** monthly-archive seed/backfill source (needs CT's real name/email to register — ask first). Deferred (noted in EXECUTION.md): Thai problem-category normalization map, referential `district ∈ dim_district` check (→ Phase 3), poison-test demo GIF.

## Structure

| Path | Role |
|---|---|
| `dags/traffy_ingest.py` | Airflow DAG: `load_bronze >> silver_transform`. Silver runs as a **separate** `bangkok-spark` container via `DockerOperator` (Docker-out-of-Docker) — Airflow triggers Spark, doesn't run it in-process. |
| `include/traffy.py` | Live-feed fetch + flatten + day-window + bronze write. All real ingest logic (DAGs stay thin). |
| `include/storage.py` | Storage backend switch (local Parquet now → GCS/BQ in Phase 4). Keep — this abstraction is the seam that makes Phase 4 a one-file change. |
| `spark/transforms/silver_traffy.py` | Silver transforms (pure, unit-testable) + `STATE_MAP` + `main()`. |
| `spark/quality/silver_checks.py` | Quality gate (`run_quality_gate` / `assert_quality`). |
| `Dockerfile.spark` | The `bangkok-spark` image (python3.11 + JRE + pyspark 3.5 + pandas/pyarrow/pytest/requests). |
| `docker-compose.yaml` | Local Airflow (Postgres + LocalExecutor; init behind an `["init"]` profile). Mounts the Docker socket for DooD. |
| `tests/` | `test_traffy.py` (non-spark), `test_silver.py` + `test_quality.py` (spark, in-container), `conftest.py` (shared spark fixture). |
| `docs/` | `EXECUTION.md` (plan), `contracts.md` (bronze→silver contract). |

**Design rule (keep it):** DAGs stay thin — all real logic lives in `include/` and `spark/`. Storage goes through `include/storage.py` so flipping to GCS/BigQuery in Phase 4 touches one file.

## Spark on Windows — runs in Docker, not locally

Local PySpark on Windows is blocked by the winutils/NativeIO issue, so **Spark runs in the `bangkok-spark` container**. Spark tests are marked `@pytest.mark.spark` and **skipped locally** (`pyproject` `addopts = -m 'not spark'`).

```bash
# spark tests (in container)
docker run --rm -v "${PWD}:/app" -w /app bangkok-spark pytest -m spark
# silver end-to-end (writes data/silver/…)
docker run --rm -v "${PWD}:/app" -w /app bangkok-spark python -m spark.transforms.silver_traffy
```

## Run locally

```powershell
python -m venv .venv ; .venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
pytest          # non-spark suite (spark tests deselected)
ruff check .    # lint

# Airflow
docker compose up airflow-init   # one-time, runs the init profile
docker compose up -d             # → http://localhost:8080 (airflow/airflow)
```

## Traffy Fondue API — VERIFIED LIVE 2026-06-16

Endpoint: `https://publicapi.traffy.in.th/teamchadchart-stat-api/geojson/v1` — GeoJSON `FeatureCollection`.

- **`limit` works** — `300` is just the *default* page size, NOT a hard cap. `limit=1000` OK (~5 MB).
- **`offset` works** — pages into history; `offset=500000` reached Feb 2025 tickets.
- **Ordered by `last_activity` DESC** (most-recently-touched first). A reopened old ticket jumps back to the front — exactly what a `last_activity` watermark should re-ingest.
- **No server-side date filter** — `start_date`/`end_date`/`start`/`end` all ignored (newest always = now). The watermark must be **client-side**.
- **Deep offsets time out** — `offset≈1,335,000` returns HTTP 502; ordering also shifts as tickets update → **unreliable for full-history replay**.
- Top-level metadata: `total` / `count_total` ≈ full DB size (~1,335,964), `count` = features returned this call.

**Consequence:** bulk-seed-from-archive + daily-incremental-watermark is the right shape — but the *reason* is "no date filter + unstable `last_activity` ordering + deep-offset 502s," NOT a "300-record cap."

**Second endpoint (for the seed/backfill):** `.../download/bangkok_monthly` — **date-addressable history**, `file_name=bangkok_YYYY-MM` returns a whole month (csv/json via `output_type`). REQUIRES registration params `name`/`org`/`email`/`purpose` + source attribution. Better than the Kaggle archive (official, addressable by month). Not yet hit — needs CT's real name/email, **ask first**.

**Key fields:** `ticket_id`, `last_activity`, `timestamp`, `state_type_latest` (`start`/`inprogress`/`finish`/`forward`/`irrelevant`), `timestamp_inprogress`, `timestamp_finished`, `problem_type_fondue` (array), `district`/`subdistrict`/`province`, `coordinates [lon,lat]`, `description` (Thai), `org`, `photo_url`, `count_reopen`, `view_count`. Timestamps are `'YYYY-MM-DD HH:MM:SS'` strings.

## Guardrails

- **Cost:** stay in GCP free tier — BigQuery sandbox, minimal GCS. No Cloud Composer, no persistent Dataproc. Flag any step that would bill before running it.
- **Scope:** no streaming in v1 — it's a *deliberately deferred* labelled v2 stretch.
- **API politeness:** respect the ~30-min GeoJSON cache; poll on schedule, don't hammer; attribute Traffy Fondue as the source.
- **One narrative:** civic location-intelligence. Resist bolting on unrelated datasets.
