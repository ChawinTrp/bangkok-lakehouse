"""Tests for include/storage.py — the bronze write boundary.

The key behavior under test: an empty window must NOT land a partition. An empty
DataFrame from flatten_traffy([]) has zero rows *and zero columns*; writing it
produces a schema-less ~600-byte parquet that crashes Spark's partitioned reader
later. "No data = no partition" — skip the write instead.
"""

import pandas as pd

import include.storage as storage
from include.traffy import flatten_traffy


def test_write_skips_empty_partition(tmp_path, monkeypatch):
    # LAKEHOUSE_ROOT is read into a module global at import time -> patch the global.
    monkeypatch.setattr(storage, "LAKEHOUSE_ROOT", tmp_path)

    empty = flatten_traffy([], run_id="2026-06-22")  # zero features -> column-less df

    out = storage.write_bronze_parquet(empty, source="traffy", run_date="2026-06-22")

    assert out is None  # nothing written
    assert not storage.bronze_path("traffy", "2026-06-22").exists()  # no partition dir


def test_write_lands_nonempty_partition(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "LAKEHOUSE_ROOT", tmp_path)

    df = pd.DataFrame([{"ticket_id": "A", "lon": 100.5, "lat": 13.8}])

    out = storage.write_bronze_parquet(df, source="traffy", run_date="2026-06-22")

    assert out is not None
    assert storage.bronze_path("traffy", "2026-06-22").joinpath("part-000.parquet").exists()
    assert len(pd.read_parquet(out)) == 1  # round-trips
