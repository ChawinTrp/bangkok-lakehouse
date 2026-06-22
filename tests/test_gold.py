"""Gold (star schema) tests. Run in Docker: pytest -m spark."""

import datetime as dt

import pytest

pytest.importorskip("pyspark")

from pyspark.sql.types import (  # noqa: E402
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark.transforms.gold_traffy import (  # noqa: E402
    build_dim_category,
    build_dim_date,
    build_dim_district,
    build_fact_district_daily,
    build_fact_ticket_lifecycle,
)

pytestmark = pytest.mark.spark


def ts(s):
    return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S") if s else None


# silver_tickets-shaped rows: ticket_id, district, timestamp, timestamp_inprogress,
# timestamp_finished, status, count_reopen
TICKET_SCHEMA = StructType([
    StructField("ticket_id", StringType()),
    StructField("district", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("timestamp_inprogress", TimestampType()),
    StructField("timestamp_finished", TimestampType()),
    StructField("status", StringType()),
    StructField("count_reopen", LongType()),
])


def tickets_df(spark, rows):
    return spark.createDataFrame(rows, TICKET_SCHEMA)


def test_dim_district_is_distinct_nonnull(spark):
    df = tickets_df(spark, [
        ("A", "Bang Rak", ts("2026-06-01 09:00:00"), None, None, "reported", 0),
        ("B", "Bang Rak", ts("2026-06-02 09:00:00"), None, None, "reported", 0),
        ("C", None, ts("2026-06-02 09:00:00"), None, None, "reported", 0),
    ])
    out = sorted(r["district"] for r in build_dim_district(df).collect())
    assert out == ["Bang Rak"]  # deduped, null dropped


def test_dim_category_distinct(spark):
    cats = spark.createDataFrame(
        [("A", "flood"), ("B", "flood"), ("C", "road")], ["ticket_id", "category"]
    )
    out = sorted(r["category"] for r in build_dim_category(cats).collect())
    assert out == ["flood", "road"]


def test_dim_date_is_contiguous_with_attributes(spark):
    df = tickets_df(spark, [
        ("A", "Bang Rak", ts("2026-06-01 09:00:00"), None, None, "reported", 0),
        ("B", "Bang Rak", ts("2026-06-03 09:00:00"), None, None, "reported", 0),
    ])
    rows = {r["date"]: r for r in build_dim_date(df).collect()}
    assert sorted(str(d) for d in rows) == ["2026-06-01", "2026-06-02", "2026-06-03"]
    # 2026-06-06 is a Saturday
    sat = build_dim_date(tickets_df(spark, [
        ("A", "X", ts("2026-06-06 09:00:00"), None, None, "reported", 0)
    ])).collect()[0]
    assert sat["is_weekend"] is True


def test_lifecycle_measures(spark):
    df = tickets_df(spark, [
        # resolved in 4 days, reopened
        ("A", "Bang Rak", ts("2026-06-01 09:00:00"), ts("2026-06-02 09:00:00"),
         ts("2026-06-05 09:00:00"), "resolved", 2),
        # still open
        ("B", "Din Daeng", ts("2026-06-01 09:00:00"), None, None, "in_progress", 0),
    ])
    out = {r["ticket_id"]: r for r in build_fact_ticket_lifecycle(df).collect()}
    assert out["A"]["is_resolved"] is True
    assert out["A"]["is_reopened"] is True
    assert out["A"]["days_to_resolve"] == 4
    assert out["B"]["is_resolved"] is False
    assert out["B"]["days_to_resolve"] is None
    assert out["B"]["reported_date"] == dt.date(2026, 6, 1)


def test_district_daily_backlog_carries_across_quiet_day(spark):
    # one district/category: open 2 on day1, nothing on day2, close 1 on day3.
    # backlog must read 2, 2, 1 — proving the dense grid + cumulative sum.
    tk = tickets_df(spark, [
        ("A", "Bang Rak", ts("2026-06-01 09:00:00"), None, None, "reported", 0),
        ("B", "Bang Rak", ts("2026-06-01 10:00:00"), None, ts("2026-06-03 10:00:00"),
         "resolved", 0),
    ])
    cats = spark.createDataFrame(
        [("A", "flood"), ("B", "flood")], ["ticket_id", "category"]
    )
    dd = build_dim_district(tk)
    dc = build_dim_category(cats)
    ddate = build_dim_date(tk)
    out = {
        str(r["date"]): r
        for r in build_fact_district_daily(tk, cats, dd, dc, ddate).collect()
        if r["category"] == "flood"
    }
    assert (out["2026-06-01"]["opened"], out["2026-06-01"]["closed"]) == (2, 0)
    assert out["2026-06-01"]["backlog"] == 2
    assert out["2026-06-02"]["opened"] == 0  # dense: a row exists on the quiet day
    assert out["2026-06-02"]["backlog"] == 2  # backlog carried across the quiet day
    assert out["2026-06-03"]["closed"] == 1
    assert out["2026-06-03"]["backlog"] == 1
    assert out["2026-06-03"]["median_resolution_time"] == 2  # B took 2 days
