# Bangkok Location-Intelligence Lakehouse

An **incremental medallion lakehouse** over [Traffy Fondue](https://www.traffy.in.th/) — Bangkok's live citizen-complaint platform (~1.3M geolocated tickets, thousands/day, with a real status lifecycle).

**Question it answers:** *which districts are underserved by city services, and how does that overlay site quality?*

The point: exercise genuine **incremental processing** — bulk historical seed + daily watermark-based ingestion, accumulating-snapshot facts, a freshness SLA — the things a static full-refresh pipeline never touches.

## Architecture (medallion / ELT)

```
Traffy API ─┐
            ├─→ bronze (raw, as-is)  ─→ silver (clean, typed, deduped)  ─→ gold (star schema)  ─→ dashboard
Kaggle seed ┘        local Parquet            PySpark + quality gate          BigQuery (Phase 4)
```

See [`docs/EXECUTION.md`](docs/EXECUTION.md) for the phase-by-phase plan.

## Local dev

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
pytest        # run the unit tests
ruff check .  # lint
```

Status: **Phases 0–2 done** (built from scratch as a learning project).

- **Phase 0** — scaffolding, CI, Dockerised Airflow ✅
- **Phase 1** — bronze: live Traffy ingestion (today-so-far snapshot, paginated) running in Airflow ✅
- **Phase 2** — silver: PySpark transform (dedup-latest, timestamp parse, Bangkok bbox, category explode, `state_type_latest` → canonical `status`), a fail-closed quality gate, and the bronze→silver [data contract](docs/contracts.md); wired into the DAG via `DockerOperator` ✅
- **Phase 3** — gold: star schema (dimensions + accumulating-snapshot & periodic-snapshot facts) ⏭ next
