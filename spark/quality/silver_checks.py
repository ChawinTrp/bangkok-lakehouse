"""Quality gate for the silver layer.

Run on the silver *candidate* (post-transform, pre-publish). If any check fails we
raise — so main() never writes a bad table. Bronze stays the source of truth and the
existing silver is left untouched (failure blocks promotion).
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.transforms.silver_traffy import BKK_BBOX


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


def check_non_empty(df: DataFrame) -> CheckResult:
    n = df.count()
    return CheckResult("non_empty", n > 0, f"{n} rows")


def check_not_null(df: DataFrame, cols) -> CheckResult:
    cond = None
    for c in cols:
        is_null = F.col(c).isNull()
        cond = is_null if cond is None else (cond | is_null)
    bad = df.filter(cond).count() if cond is not None else 0
    return CheckResult("not_null", bad == 0, f"{bad} rows null in {list(cols)}")


def check_unique(df: DataFrame, key: str = "ticket_id") -> CheckResult:
    total = df.count()
    distinct = df.select(key).distinct().count()
    return CheckResult(f"unique_{key}", total == distinct, f"{total - distinct} duplicate {key}")


def check_geo_bounds(df: DataFrame, lon_col: str = "lon", lat_col: str = "lat") -> CheckResult:
    south, west, north, east = BKK_BBOX
    inside = F.col(lat_col).between(south, north) & F.col(lon_col).between(west, east)
    outside = df.filter(~inside).count()
    return CheckResult("geo_bounds", outside == 0, f"{outside} rows outside Bangkok bbox")


def run_quality_gate(df: DataFrame) -> list[CheckResult]:
    """Run every check and return the results (does not raise)."""
    return [
        check_non_empty(df),
        check_not_null(df, ["ticket_id", "lon", "lat"]),
        check_unique(df),
        check_geo_bounds(df),
    ]


def assert_quality(df: DataFrame) -> list[CheckResult]:
    """Run the gate, print a report, and raise if any check fails (blocks promotion)."""
    results = run_quality_gate(df)
    for r in results:
        print(f"  [{'PASS' if r.passed else 'FAIL'}] {r.name}: {r.detail}")
    failed = [r.name for r in results if not r.passed]
    if failed:
        raise ValueError(f"quality gate FAILED: {failed}")
    return results
