# Execution Plan — Bangkok Location-Intelligence Lakehouse

> **Pivot (2026-06-16):** primary source is now **Traffy Fondue** — Bangkok's live citizen-complaint platform (~1.33M tickets, thousands/day, geolocated, with a real status lifecycle). This makes daily freshness *genuine* and exercises the skill site-selection couldn't: **incremental processing** (watermarks, bulk-seed + CDC, accumulating snapshots). Same location-intelligence thesis, sharper question — "which districts are underserved by city services, and how does that overlay site quality?"
>
> Working plan. Check items off as you go. Each phase ends with a **proof** — the thing you can show an interviewer.
> Design principle: **local-first** — Phases 1–2 need zero GCP setup (bronze/silver = local Parquet). GCP wiring is its own phase, so a free weekend is never blocked by billing/IAM yak-shaving.

## Data sources

| Source | Role | Notes |
|---|---|---|
| **Traffy Fondue live API** | daily incremental | `https://publicapi.traffy.in.th/teamchadchart-stat-api/geojson/v1` — GeoJSON, returns ~300 most-recent tickets per call, ~2h cache. Cannot re-pull full history → poll + accumulate. |
| **Traffy Fondue 2022–2025 (Kaggle)** | one-time historical seed | Full archive for the bulk bronze load + backfill replay. |
| **Bangkok district reference** | `dim_district` | 50 khet / khwaeng. `district`/`subdistrict` already labelled on each ticket — join is trivial. |
| **PM2.5 air quality (Air4Thai/WAQI)** | optional 2nd source | Only if multi-source ingestion is wanted. Park to v2 if time-tight. |

**Key fields** (verified live 2026-06-16): `ticket_id`, `message_id`, `timestamp`, `last_activity`, `state_type_latest` (start/inprogress/finish/forward/irrelevant), `timestamp_inprogress`, `timestamp_finished`, `problem_type_fondue` (array), `district`/`subdistrict`/`province`, `coordinates [lon,lat]`, `description` (Thai), `org`, `photo_url`, `view_count`.

## Phase 0 — Today (~1 hr)

- [ ] `git init` + first commit; create **public** GitHub repo, push — *local git done (7 commits); **not pushed yet — no GitHub remote configured***
- [x] `cp .env.example .env`
- [x] `docker compose up airflow-init && docker compose up -d` — Airflow at http://localhost:8080 (`airflow`/`airflow`)
- [x] Confirm the `traffy_ingest` DAG appears with no import errors
- [ ] `make test` passes locally — *no Makefile/CI yet; tests run via `pytest` (non-spark) + `pytest -m spark` in the `bangkok-spark` container + `ruff`. Makefile + CI workflow deferred to Phase 5.*

**Proof:** Airflow UI screenshot with the DAG loaded, green CI badge on the repo. *(DAG runs locally; GitHub repo + CI badge still pending.)*

## Phase 1 — Bronze: bulk seed + daily incremental (weekend 1)

- [ ] **Seed task (once):** load the historical archive to `data/bronze/traffy/dt=<report_date>/` Parquet + load metadata — *pending: switched target from Kaggle to the official **monthly-archive** endpoint (`download/bangkok_monthly`, date-addressable); needs CT's real name/email to register. This source also owns backfill.*
- [x] **Daily incremental DAG `traffy_ingest`:** poll the live GeoJSON → bronze. *(Implemented as a "today-so-far" **window snapshot** — `fetch_traffy_until` paginates the newest-first feed back to today 00:00 (Asia/Bangkok), overwrites `dt=today`. The live newest-N feed isn't date-addressable, so the `last_activity`/`logical_date` watermark lives on the monthly-archive source, not here — see CLAUDE.md.)*
- [x] Idempotency: re-running a date overwrites that partition only — proved (bronze overwrites `dt`; silver dedups across partitions: dt=06-20 + dt=06-21 → 2215 deduped rows)
- [x] Schedule `@daily`, `catchup=False`. *(Full medallion architecture diagram in the README is a Phase 4 item.)*

**Proof:** two runs → identical partition; silver dedups across partitions. *(Side-by-side seed tree pending the seed task.)*

## Phase 2 — Silver with PySpark (weekend 2)

- [x] `spark/transforms/silver_traffy.py`: dedup on `ticket_id` (latest `last_activity` wins); **explode** `problem_type_fondue` array → one row per ticket-category; normalize `state_type_latest` → canonical `status` via `STATE_MAP`; parse timestamps; Bangkok bbox filter. *(Schema-on-read, not an explicit StructType. Thai problem-category normalization deferred — the live category set is large/open; status was the high-value piece feeding the Phase 3 lifecycle fact.)*
- [x] Quality gate `spark/quality/silver_checks.py`: `non_empty`, `not_null` (geo + `ticket_id`), `geo_bounds`, **`rowcount_delta`** (silver vs distinct bronze tickets) — **failure blocks promotion**. *(Write atomicity comes from Spark's `overwrite` mode, not a temp-dir+rename. **Referential** `district` ∈ `dim_district` deferred to Phase 3 — `dim_district` doesn't exist until then.)*
- [x] Wire into DAG: `load_bronze >> silver_transform` (Spark via `DockerOperator`, Docker-out-of-Docker)
- [x] Document the bronze→silver contract in `docs/contracts.md`

**Proof:** a deliberately-poisoned bronze partition (null `ticket_id`) failed the gate → exit 1, nothing written, silver stayed at the clean row count. *(Verified live; 30-sec demo GIF for the README still TODO.)*

## Phase 3 — Gold + data model (weekend 3)

- [x] Dimensions: `dim_district`, `dim_category`, `dim_date` (natural keys; dims derived from silver, `dim_date` generated). *(`build_dim_*` in `spark/transforms/gold_traffy.py`.)*
- [x] **`fact_ticket_lifecycle`** — *accumulating snapshot*, grain: **one ticket**; milestones (reported/in-progress/finished); measures `days_to_resolve`, `is_resolved`, `is_reopened`. `ticket_id` degenerate dim; rebuilt from silver each run (self-healing).
- [x] **`fact_district_daily`** — *periodic snapshot, **dense***, grain: **district × category × day**; measures opened, closed, **backlog** (semi-additive, cumulative over the dense grid), **median_resolution_time** (non-additive).
- [ ] Optional differentiator: TOPSIS "service-stress" ranking — *deferred (labelled follow-up); core star comes first.*
- [x] `docs/data_model.md` — star diagram + grain/key/additivity/SCD decisions + honest limitations.
- [x] **Gold wired into the DAG** (2026-06-23): `load_bronze >> silver_transform >> gold_transform` (gold via the same DockerOperator/DooD pattern as silver) — the pipeline is now end-to-end bronze→silver→gold.

**Proof:** `spark/proof_queries.py` answers both — top districts by flooding backlog (from `fact_district_daily`) and median resolution time by district (from `fact_ticket_lifecycle`). 21 spark tests pass in the `bangkok-spark` container. **Full DAG run verified end-to-end in Airflow (2026-06-23):** all three tasks green on live data → gold rebuilt (`dim_date` 5d, `fact_ticket_lifecycle` 3,618 rows, `fact_district_daily` 9,600 rows). *(Data window still short — historical seed not yet loaded.)*

> **Accelerated track (revised 2026-06-22).** Phases 0–3 done. CT has prior **GCP and Looker Studio** experience (Champ data track), so the IAM/billing variance and the BI-tool learning tax are largely gone — **Databricks/Delta is the only genuinely new piece left.** Remaining work re-sequenced to front-load what makes the project *applyable this week*, then layer the rest while interviewing. Revised estimate: **~18–28 focused hours** (was ~38–59 before accounting for prior experience).

## Milestone 0 — Make it applyable (do first, ~1–2h)

- [x] `git remote add` + push the **public** GitHub repo — live at [github.com/ChawinTrp/bangkok-lakehouse](https://github.com/ChawinTrp/bangkok-lakehouse)
- [ ] Minimal CI (GitHub Actions): `ruff` + non-spark `pytest` + DAG-integrity on push → green badge. (Spark suite stays Docker-only; note that in the README.) ← **next increment**
- [ ] Resume bullets live (Phases 0–3 only — bullets drafted in LEARNING.md/summary); start applications

**Proof:** public repo link + green CI badge. *This is the gate to start applying — everything below can land afterwards.*

## Phase 4 — GCP wiring + orchestration hardening (FAST — prior GCP experience, ~6–9h)

- [ ] Prereq: **monthly-archive seed** (register name/email; load history) — also unblocks backfill. ~3–5h (counted separately below).
- [ ] Flip `STORAGE_BACKEND=gcs` via `include/storage.py`: bronze/silver → GCS, gold → BigQuery (`bronze_raw`/`silver`/`gold`). Free tier; no Composer/Dataproc. *(Fast: GCS/BQ setup is familiar — the new work is just the storage-backend impl + the gold→BQ load.)*
- [ ] Orchestration hardening: retries + exponential backoff, task SLAs, on-failure callback
- [ ] **Freshness SLA:** yesterday's data lands + transforms by 06:00, alert on breach
- [ ] Backfill: replay history by `logical_date` off the seed (`airflow dags backfill`)
- [ ] README: final medallion architecture diagram

**Proof:** BigQuery gold queryable in console; a backfill repopulating N days; a freshness-SLA alert firing on a late batch.

## Phase 5 — Databricks + dashboard + case study (~6–9h)

- [ ] **Databricks Free Edition: port `silver_traffy.py` to a Delta notebook** — *the one new-learning chunk; keep it TDD-light.* Note API/Delta differences in `docs/databricks_notes.md` → "Databricks" becomes honest.
- [ ] Looker Studio dashboard on gold (**FAST — prior Looker experience**): district backlog map, resolution-time trend, backlog by category
- [ ] Rewrite README as a mini case study (problem → design decisions → result), link the dashboard + `LEARNING.md`

**Proof:** public repo + live dashboard link + the Delta notebook.

## Phase 6 — Showcase integration (~2–3h)

- [ ] Case-study webpage page (4-block formula; headline = the design insight)
- [ ] Master CV: add project + the now-true bullets
- [ ] Fit Analysis: mark DE tool gaps closed (incremental/CDC, freshness SLA, BigQuery, Databricks, Looker)

## Cross-cutting follow-ups
- [x] **Bronze empty-partition guard** (2026-06-23, commit `c0475dd`) — `write_bronze_parquet` skips empty windows (returns `None`); unit + spark-regression tests. See `LEARNING.md` §7 bug 4.
- [ ] Monthly-archive seed (register + load) ~3–5h — prerequisite for Phase 4 backfill.

## Parked (v2 — deliberately deferred, say so in interviews)

- Streaming layer (Kafka → Spark Structured Streaming) on the live-endpoint poll — "batch D-1 by choice; streaming scoped, no consumer needs sub-day freshness"
- dbt for the gold layer
- Data lineage with OpenLineage/Marquez
- PM2.5 air-quality as a second source

## Decisions log

| Decision | Choice | Why |
|---|---|---|
| **Primary dataset** | **Traffy Fondue (real, live)** over synthetic delivery data | Real, daily-fresh, geolocated, with a genuine status lifecycle; authentic mess for the "fix the pipeline" practice; civic location-intelligence narrative |
| **Ingestion shape** | **Bulk seed (Kaggle) + incremental live API (watermark)** | The live endpoint caps ~300 recent tickets, so full re-pull is impossible — forces the production "initial load + CDC" pattern |
| **Backfill** | Replay history by `logical_date` off the seed | Daily run and backfill share one code path |
| Orchestrator hosting | docker-compose locally, not Cloud Composer | Composer ≈ $300+/mo; the skill is DAG design, not paying for managed infra |
| Local-first storage | Parquet on disk → GCS/BQ in Phase 4 | Weekend 1 never blocked by GCP setup |
| Spark runtime | Local Docker + Databricks Free for one notebook | Free, and both keywords become honest |

## Guardrails

- **Costs:** stay in GCP free tier — BigQuery sandbox limits, GCS minimal; no Cloud Composer, no Dataproc cluster left running. Flag any step that would bill before running it.
- **Scope:** no streaming in v1 — park it as a labeled v2 stretch so interviews hear "deliberately deferred," not "didn't know."
- **API politeness:** respect the live endpoint's cache window; poll on schedule, don't hammer; attribute Traffy Fondue as the source; keep within their open-data terms.
- **One narrative:** civic location-intelligence. Resist adding unrelated datasets.

## Earned resume bullets (write only when true)

- "Built an incremental medallion lakehouse over live Bangkok civic-complaint data (Traffy Fondue) — daily watermark-based ingestion with a historical bulk seed, mirroring a production initial-load + CDC pattern"
- "Modeled a complaint-lifecycle accumulating-snapshot fact (reported → in-progress → resolved) with resolution-time SLAs, plus a daily district-backlog periodic snapshot mart"
- "Orchestrated batch ELT with Airflow — dependency-aware DAGs, retries, a 6am freshness SLA, and backfills via logical-date replay — with CI validation of DAGs and transforms"
- "Published a star-schema gold layer powering a Looker Studio dashboard of district complaint backlog and resolution times"
