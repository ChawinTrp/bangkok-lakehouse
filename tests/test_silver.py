"""Spark tests for the silver transforms. Run in Docker: pytest -m spark
(skipped by default locally — see pyproject addopts)."""

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql.types import ArrayType, StringType, StructField, StructType  # noqa: E402

from spark.transforms.silver_traffy import (  # noqa: E402
    dedup_latest,
    explode_categories,
    filter_bangkok_bbox,
    normalize_state,
    parse_timestamps,
)

pytestmark = pytest.mark.spark


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


def test_parse_timestamps_converts_strings_and_keeps_nulls(spark):
    from datetime import datetime

    rows = [("A", "2026-06-20 12:00:00"), ("B", None)]
    df = spark.createDataFrame(rows, ["ticket_id", "last_activity"])
    parsed = parse_timestamps(df, cols=["last_activity"]).collect()
    vals = {r["ticket_id"]: r["last_activity"] for r in parsed}
    assert vals["A"] == datetime(2026, 6, 20, 12, 0, 0)  # now a real datetime, not a string
    assert vals["B"] is None


def test_filter_bangkok_bbox_keeps_only_inside(spark):
    rows = [
        ("IN", 100.5, 13.8),     # inside Bangkok
        ("WEST", 99.0, 13.8),    # lon too far west
        ("NORTH", 100.5, 15.0),  # lat too far north
        ("NULL", None, None),    # missing coords
    ]
    df = spark.createDataFrame(rows, ["ticket_id", "lon", "lat"])
    kept = [r["ticket_id"] for r in filter_bangkok_bbox(df).collect()]
    assert kept == ["IN"]


def test_normalize_state_maps_known_and_nulls_unknown(spark):
    rows = [
        ("A", "finish"),       # -> resolved
        ("B", "inprogress"),   # -> in_progress
        ("C", "weird_state"),  # unknown -> null (surfaces drift, never silently bucketed)
        ("D", None),           # null -> null
    ]
    df = spark.createDataFrame(rows, ["ticket_id", "state_type_latest"])
    out = {r["ticket_id"]: r["status"] for r in normalize_state(df).collect()}
    assert out == {"A": "resolved", "B": "in_progress", "C": None, "D": None}


def test_normalize_state_preserves_original_column(spark):
    df = spark.createDataFrame([("A", "start")], ["ticket_id", "state_type_latest"])
    row = normalize_state(df).collect()[0]
    assert row["state_type_latest"] == "start"  # raw value retained per the contract
    assert row["status"] == "reported"


def test_explode_categories_one_row_per_pair(spark):
    schema = StructType([
        StructField("ticket_id", StringType()),
        StructField("problem_type_fondue", ArrayType(StringType())),
    ])
    rows = [("A", ["tree", "road"]), ("B", ["flood"]), ("C", [])]
    df = spark.createDataFrame(rows, schema)
    out = sorted((r["ticket_id"], r["category"]) for r in explode_categories(df).collect())
    assert out == [("A", "road"), ("A", "tree"), ("B", "flood")]  # C's empty array -> no rows
