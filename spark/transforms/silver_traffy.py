"""Silver transform for Traffy: bronze (raw, duplicated) -> clean, one row per ticket.

Reads all bronze partitions, keeps the latest row per ticket_id, parses timestamps,
and writes the conformed silver_tickets table. Transforms are small pure functions on
Spark DataFrames so each is unit-testable on its own.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

LAKEHOUSE_ROOT = os.environ.get("LAKEHOUSE_ROOT", "data")

# Bangkok bounding box: (south, west, north, east) = (lat_min, lon_min, lat_max, lon_max)
BKK_BBOX = (13.49, 100.32, 14.00, 100.94)

# String columns that hold 'YYYY-MM-DD HH:MM:SS' timestamps in bronze.
DEFAULT_TS_COLS = ("timestamp", "last_activity", "timestamp_inprogress", "timestamp_finished")


def dedup_latest(
    df: DataFrame, key: str = "ticket_id", order_col: str = "last_activity"
) -> DataFrame:
    """Keep one row per `key` — the one with the greatest `order_col`.

    This is the reconciler: live-feed page overlaps, reopened tickets, and (later)
    live-vs-monthly overlap all collapse here to a single current row per ticket.
    It is idempotent — running it on already-deduped data returns the same rows —
    because ranking by a key and keeping rank 1 is a fixed point.
    """
    ranked = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(ranked))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def parse_timestamps(
    df: DataFrame, cols=DEFAULT_TS_COLS, fmt: str = "yyyy-MM-dd HH:mm:ss"
) -> DataFrame:
    """Convert string timestamp columns to real timestamp type (nulls stay null).

    Only columns present in df are touched, so it's safe on partial schemas.
    """
    for col_name in cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.to_timestamp(F.col(col_name), fmt))
    return df


def filter_bangkok_bbox(df: DataFrame, lon_col: str = "lon", lat_col: str = "lat") -> DataFrame:
    """Keep only rows whose coordinates fall inside Bangkok's bounding box.

    `between` is null-safe here: rows with null lon/lat evaluate to null (not True)
    and are therefore dropped — exactly what we want for missing geo.
    """
    south, west, north, east = BKK_BBOX
    return df.filter(
        F.col(lat_col).between(south, north) & F.col(lon_col).between(west, east)
    )


def explode_categories(
    df: DataFrame, col: str = "problem_type_fondue", key: str = "ticket_id"
) -> DataFrame:
    """One row per (ticket, category). Grain change -> this is a SEPARATE table.

    F.explode drops empty/null arrays, so a ticket with no category contributes
    no rows (correct for category counts).
    """
    return df.select(key, F.explode(F.col(col)).alias("category"))


def build_silver_tickets(df: DataFrame) -> DataFrame:
    """bronze -> one clean, typed, in-Bangkok row per ticket."""
    df = dedup_latest(df)
    df = parse_timestamps(df)
    df = filter_bangkok_bbox(df)
    return df


def main() -> None:
    """Read all bronze partitions, build the two silver tables, write them out."""
    spark = SparkSession.builder.appName("silver_traffy").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.read.parquet(f"{LAKEHOUSE_ROOT}/bronze/traffy")

    tickets = build_silver_tickets(bronze)

    # quality gate: validate BEFORE writing. If it raises, nothing is published and
    # the existing silver is left untouched (failure blocks promotion).
    from spark.quality.silver_checks import assert_quality

    assert_quality(tickets)

    categories = explode_categories(tickets)

    # coalesce(1): the dedup shuffle leaves ~200 partitions; collapse to one file so
    # we don't litter each table with hundreds of tiny parquet parts.
    tickets.coalesce(1).write.mode("overwrite").parquet(
        f"{LAKEHOUSE_ROOT}/silver/traffy_tickets"
    )
    categories.coalesce(1).write.mode("overwrite").parquet(
        f"{LAKEHOUSE_ROOT}/silver/traffy_ticket_category"
    )

    print(f"silver_tickets rows         : {tickets.count()}")
    print(f"silver_ticket_category rows : {categories.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
