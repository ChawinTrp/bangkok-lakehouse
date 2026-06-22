"""Gold transforms for Traffy: silver -> star schema (dimensions + facts).

Reads the two silver tables and builds a small star:
  dims  : dim_district, dim_category, dim_date (natural keys)
  facts : fact_ticket_lifecycle (accumulating snapshot, grain = one ticket)
          fact_district_daily   (periodic snapshot, dense, grain = district x category x day)

Each builder is a pure function on Spark DataFrames so it is unit-testable on its
own; main() reads silver, builds everything, and writes data/gold/.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

LAKEHOUSE_ROOT = os.environ.get("LAKEHOUSE_ROOT", "data")


# --- dimensions (natural keys; derived from silver, see data_model.md) ---------

def build_dim_district(tickets: DataFrame) -> DataFrame:
    """One row per district. PK = district (natural key)."""
    return (
        tickets.select("district")
        .where(F.col("district").isNotNull() & (F.trim(F.col("district")) != ""))
        .distinct()
    )


def build_dim_category(categories: DataFrame) -> DataFrame:
    """One row per problem category (raw Thai value). PK = category."""
    return (
        categories.select("category")
        .where(F.col("category").isNotNull() & (F.trim(F.col("category")) != ""))
        .distinct()
    )


def build_dim_date(tickets: DataFrame, end_date=None) -> DataFrame:
    """One row per calendar day from the first reported ticket to `end_date`.

    Generated (not sourced): a date dimension is a calendar, so we synthesise the
    range and precompute the attributes dashboards group by. `end_date` defaults to
    the latest reported date in the data.
    """
    # span the full activity range: a ticket can be FINISHED after the last day any
    # ticket was reported, so the calendar must reach the latest finished date too,
    # else the periodic snapshot would silently drop those closure days.
    bounds = tickets.agg(
        F.min(F.to_date("timestamp")).alias("start"),
        F.max(F.to_date("timestamp")).alias("max_reported"),
        F.max(F.to_date("timestamp_finished")).alias("max_finished"),
    ).first()
    start = bounds["start"]
    latest = max(d for d in (bounds["max_reported"], bounds["max_finished"]) if d is not None)
    end = end_date or latest

    spark = tickets.sparkSession
    days = spark.range(1).select(
        F.explode(F.sequence(F.lit(start), F.lit(end), F.expr("interval 1 day"))).alias("date")
    )
    return days.select(
        "date",
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.dayofmonth("date").alias("day"),
        F.dayofweek("date").alias("day_of_week"),  # 1=Sun .. 7=Sat
        F.weekofyear("date").alias("week_of_year"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.quarter("date").alias("quarter"),
    )


# --- facts ---------------------------------------------------------------------

def build_fact_ticket_lifecycle(tickets: DataFrame) -> DataFrame:
    """Accumulating snapshot, grain = one ticket.

    A projection of silver (already one current row per ticket). Milestone
    timestamps fill in over the ticket's life; the snapshot self-heals on each run
    because silver always holds the latest state. ticket_id is a degenerate
    dimension; district + reported_date are FKs into the dims. Category is NOT here
    (a ticket has many categories -> it would break the one-ticket grain).
    """
    return tickets.select(
        "ticket_id",  # degenerate dimension
        "district",  # FK -> dim_district
        F.to_date("timestamp").alias("reported_date"),  # FK -> dim_date
        F.col("timestamp").alias("reported_at"),
        F.col("timestamp_inprogress").alias("in_progress_at"),
        F.col("timestamp_finished").alias("finished_at"),
        "status",
        (F.coalesce(F.col("count_reopen"), F.lit(0)) > 0).alias("is_reopened"),
        F.col("timestamp_finished").isNotNull().alias("is_resolved"),
        F.datediff(F.col("timestamp_finished"), F.col("timestamp")).alias("days_to_resolve"),
    )


def build_fact_district_daily(
    tickets: DataFrame, categories: DataFrame, dim_district: DataFrame,
    dim_category: DataFrame, dim_date: DataFrame,
) -> DataFrame:
    """Dense periodic snapshot, grain = district x category x day.

    opened/closed are per-day counts; backlog is the running open count
    (cumulative opened - closed) carried across every day via the dense grid;
    median_resolution_time is the median days_to_resolve of tickets finished that
    day (null where nothing closed). backlog is semi-additive (sum across
    district/category, never across days); the median is non-additive.
    """
    # ticket x category base, carrying the district + the two milestone dates.
    base = categories.join(
        tickets.select(
            "ticket_id", "district",
            F.to_date("timestamp").alias("reported_date"),
            F.to_date("timestamp_finished").alias("finished_date"),
            F.datediff(F.col("timestamp_finished"), F.col("timestamp")).alias("days_to_resolve"),
        ),
        "ticket_id",
    )

    opened = (
        base.groupBy("district", "category", F.col("reported_date").alias("date"))
        .agg(F.count("*").alias("opened"))
    )
    closed = (
        base.where(F.col("finished_date").isNotNull())
        .groupBy("district", "category", F.col("finished_date").alias("date"))
        .agg(
            F.count("*").alias("closed"),
            F.percentile_approx("days_to_resolve", 0.5).alias("median_resolution_time"),
        )
    )

    # dense grid: every (district, category, day) gets a row.
    grid = (
        dim_district.crossJoin(dim_category)
        .crossJoin(dim_date.select("date"))
    )

    joined = (
        grid.join(opened, ["district", "category", "date"], "left")
        .join(closed, ["district", "category", "date"], "left")
        .fillna({"opened": 0, "closed": 0})
    )

    running = Window.partitionBy("district", "category").orderBy("date").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    return (
        joined.withColumn("backlog", F.sum(F.col("opened") - F.col("closed")).over(running))
        .select(
            "district", "category", "date",
            "opened", "closed", "backlog", "median_resolution_time",
        )
    )


def main() -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("gold_traffy").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    tickets = spark.read.parquet(f"{LAKEHOUSE_ROOT}/silver/traffy_tickets")
    categories = spark.read.parquet(f"{LAKEHOUSE_ROOT}/silver/traffy_ticket_category")

    dim_district = build_dim_district(tickets)
    dim_category = build_dim_category(categories)
    dim_date = build_dim_date(tickets)
    fact_lifecycle = build_fact_ticket_lifecycle(tickets)
    fact_daily = build_fact_district_daily(
        tickets, categories, dim_district, dim_category, dim_date
    )

    out = f"{LAKEHOUSE_ROOT}/gold"
    for name, df in [
        ("dim_district", dim_district), ("dim_category", dim_category),
        ("dim_date", dim_date), ("fact_ticket_lifecycle", fact_lifecycle),
        ("fact_district_daily", fact_daily),
    ]:
        df.coalesce(1).write.mode("overwrite").parquet(f"{out}/{name}")
        print(f"{name:24s}: {df.count()} rows")

    spark.stop()


if __name__ == "__main__":
    main()
