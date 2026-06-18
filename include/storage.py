"""Storage layer — local Parquet now, GCS/BigQuery in Phase 4.

Every read/write goes through this module so that flipping STORAGE_BACKEND=gcs
later touches ONE file, not every DAG. Sole job: land a bronze partition
idempotently. (Correctness comes from the [start, end) window in traffy.py, not
from any stored watermark.)
"""

from __future__ import annotations

import os
from pathlib import Path

BACKEND = os.environ.get("STORAGE_BACKEND", "local")
LAKEHOUSE_ROOT = Path(os.environ.get("LAKEHOUSE_ROOT", "./data"))


# --- bronze partitions -------------------------------------------------------

def bronze_path(source: str, run_date: str) -> Path:
    """Partition directory, e.g. data/bronze/traffy/dt=2026-06-16/."""
    return LAKEHOUSE_ROOT / "bronze" / source / f"dt={run_date}"


def write_bronze_parquet(df, source: str, run_date: str, filename: str = "part-000.parquet") -> str:
    """Write the DataFrame to its dated partition, overwriting that partition only.

    Idempotency: the path is deterministic (source + run_date + filename), so a
    re-run of the same date overwrites the same file instead of appending — run
    twice, get one identical partition, no duplicates.
    """
    if BACKEND == "local":
        target = bronze_path(source, run_date)
        target.mkdir(parents=True, exist_ok=True)
        out = target / filename
        df.to_parquet(out, index=False)
        return str(out)
    if BACKEND == "gcs":
        raise NotImplementedError("GCS backend lands in Phase 4 — see docs/EXECUTION.md")
    raise ValueError(f"Unknown STORAGE_BACKEND: {BACKEND}")
