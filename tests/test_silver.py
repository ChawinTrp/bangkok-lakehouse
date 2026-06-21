"""Spark tests for the silver transforms. Run in Docker: pytest -m spark
(skipped by default locally — see pyproject addopts)."""

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from spark.transforms.silver_traffy import dedup_latest  # noqa: E402

pytestmark = pytest.mark.spark


@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def test_dedup_keeps_latest_row_per_ticket(spark):
    rows = [
        ("A", "2026-06-18 09:00:00", "stale"),
        ("A", "2026-06-20 12:00:00", "current"),  # latest for A -> this one wins
        ("B", "2026-06-19 08:00:00", "only-B"),
    ]
    df = spark.createDataFrame(rows, ["ticket_id", "last_activity", "note"])
    out = {r["ticket_id"]: r["note"] for r in dedup_latest(df).collect()}
    assert out == {"A": "current", "B": "only-B"}


def test_dedup_is_idempotent(spark):
    rows = [
        ("A", "2026-06-20 12:00:00", "x"),
        ("B", "2026-06-19 08:00:00", "y"),
    ]
    df = spark.createDataFrame(rows, ["ticket_id", "last_activity", "note"])
    once = dedup_latest(df)
    twice = dedup_latest(once)
    assert once.count() == twice.count() == 2
    assert sorted(r["ticket_id"] for r in twice.collect()) == ["A", "B"]
