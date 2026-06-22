"""Proof queries against the gold star schema (Phase 3).

Answers the two questions from the execution plan, using the star directly:
  1. Top districts by unresolved flooding complaints (backlog), latest day.
  2. Median resolution time by district.

Run in Docker:
  docker run --rm -v "${PWD}:/app" -w /app bangkok-spark python -m spark.proof_queries
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LAKEHOUSE_ROOT = os.environ.get("LAKEHOUSE_ROOT", "data")
FLOOD = "น้ำท่วม"  # Traffy's flooding category


def main() -> None:
    spark = SparkSession.builder.appName("proof_queries").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    g = f"{LAKEHOUSE_ROOT}/gold"

    daily = spark.read.parquet(f"{g}/fact_district_daily")
    lifecycle = spark.read.parquet(f"{g}/fact_ticket_lifecycle")

    # Q1 — top 5 districts by unresolved flooding complaints (backlog) on the latest day.
    latest = daily.agg(F.max("date").alias("d")).first()["d"]
    print(f"\nQ1 — top districts by flooding backlog on {latest}:")
    (
        daily.where((F.col("category") == FLOOD) & (F.col("date") == latest))
        .select("district", "backlog")
        .orderBy(F.desc("backlog"))
        .limit(5)
        .show(truncate=False)
    )

    # Q2 — median resolution time (days) by district, resolved tickets only.
    print("Q2 — median resolution time by district (top 5 slowest):")
    (
        lifecycle.where(F.col("is_resolved"))
        .groupBy("district")
        .agg(
            F.percentile_approx("days_to_resolve", 0.5).alias("median_days"),
            F.count("*").alias("resolved_n"),
        )
        .orderBy(F.desc("median_days"))
        .limit(5)
        .show(truncate=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()
