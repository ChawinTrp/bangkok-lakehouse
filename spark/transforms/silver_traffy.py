"""Silver transform for Traffy: bronze (raw, duplicated) -> clean, one row per ticket.

Reads all bronze partitions, keeps the latest row per ticket_id, parses timestamps,
and writes the conformed silver_tickets table. Transforms are small pure functions on
Spark DataFrames so each is unit-testable on its own.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


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
