"""Storage layer — local Parquet now, GCS/BigQuery in Phase 4.

Every read/write goes through this module so that flipping STORAGE_BACKEND=gcs
later touches ONE file, not every DAG. Sole job: land a bronze partition
idempotently. (Correctness comes from the [start, end) window in traffy.py, not
from any stored watermark.)
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

BACKEND = os.environ.get("STORAGE_BACKEND", "local")
LAKEHOUSE_ROOT = Path(os.environ.get("LAKEHOUSE_ROOT", "./data"))


# --- bronze partitions -------------------------------------------------------

def bronze_path(source: str, run_date: str) -> Path:
    """Partition directory, e.g. data/bronze/traffy/dt=2026-06-16/."""
    return LAKEHOUSE_ROOT / "bronze" / source / f"dt={run_date}"


def write_bronze_parquet(
    df, source: str, run_date: str, filename: str = "part-000.parquet"
) -> str | None:
    """Write the DataFrame to its dated partition, overwriting that partition only.

    Idempotency: the path is deterministic (source + run_date + filename), so a
    re-run of the same date overwrites the same file instead of appending — run
    twice, get one identical partition, no duplicates.

    Empty window = no partition. flatten_traffy([]) yields a zero-row, zero-COLUMN
    DataFrame; writing it lands a schema-less parquet that later crashes Spark's
    partitioned reader (IndexOutOfBoundsException in buildReaderWithPartitionValues).
    So when there are no rows, skip the write and return None — the silver reader
    then simply sees one fewer partition.
    """
    if len(df) == 0:
        logger.info("no rows for %s dt=%s — skipping empty partition write", source, run_date)
        return None
    if BACKEND == "local":
        target = bronze_path(source, run_date)
        target.mkdir(parents=True, exist_ok=True)
        out = target / filename
        df.to_parquet(out, index=False)
        return str(out)
    if BACKEND == "gcs":
        raise NotImplementedError("GCS backend lands in Phase 4 — see docs/EXECUTION.md")
    raise ValueError(f"Unknown STORAGE_BACKEND: {BACKEND}")
