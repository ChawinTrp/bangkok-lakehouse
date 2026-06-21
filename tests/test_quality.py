"""Quality-gate tests. Run in Docker: pytest -m spark."""

import pytest

pytest.importorskip("pyspark")

from spark.quality.silver_checks import assert_quality, run_quality_gate  # noqa: E402

pytestmark = pytest.mark.spark


def df_of(spark, rows):
    return spark.createDataFrame(rows, ["ticket_id", "lon", "lat"])


def results_by_name(df):
    return {c.name: c.passed for c in run_quality_gate(df)}


def results_with_bronze(df, bronze_count):
    return {c.name: c.passed for c in run_quality_gate(df, bronze_count=bronze_count)}


def test_gate_passes_clean_data(spark):
    df = df_of(spark, [("A", 100.5, 13.8), ("B", 100.6, 13.7)])
    results = assert_quality(df)  # must not raise
    assert all(r.passed for r in results)


def test_gate_fails_on_null_ticket_id(spark):
    df = df_of(spark, [("A", 100.5, 13.8), (None, 100.6, 13.7)])
    assert results_by_name(df)["not_null"] is False
    with pytest.raises(ValueError):
        assert_quality(df)


def test_gate_fails_on_duplicate_ticket_id(spark):
    df = df_of(spark, [("A", 100.5, 13.8), ("A", 100.6, 13.7)])
    assert results_by_name(df)["unique_ticket_id"] is False


def test_gate_fails_on_out_of_bounds_coords(spark):
    df = df_of(spark, [("A", 99.0, 13.8)])  # lon too far west
    assert results_by_name(df)["geo_bounds"] is False


def test_rowcount_delta_skipped_when_no_bronze_count(spark):
    # backward compatible: without a bronze baseline the delta check isn't added
    df = df_of(spark, [("A", 100.5, 13.8)])
    assert "rowcount_delta" not in results_by_name(df)


def test_rowcount_delta_passes_within_range(spark):
    # silver kept 2 of 3 distinct bronze tickets (1 dropped by bbox) -> ratio .67, ok
    df = df_of(spark, [("A", 100.5, 13.8), ("B", 100.6, 13.7)])
    assert results_with_bronze(df, 3)["rowcount_delta"] is True


def test_rowcount_delta_fails_when_silver_exceeds_bronze(spark):
    # 2 silver rows vs 1 distinct bronze ticket is impossible without a dedup bug
    df = df_of(spark, [("A", 100.5, 13.8), ("B", 100.6, 13.7)])
    assert results_with_bronze(df, 1)["rowcount_delta"] is False


def test_rowcount_delta_fails_on_mass_loss(spark):
    # 1 of 100 distinct bronze tickets survived -> below the floor ratio, likely a filter bug
    df = df_of(spark, [("A", 100.5, 13.8)])
    assert results_with_bronze(df, 100)["rowcount_delta"] is False
