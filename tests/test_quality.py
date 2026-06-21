"""Quality-gate tests. Run in Docker: pytest -m spark."""

import pytest

pytest.importorskip("pyspark")

from spark.quality.silver_checks import assert_quality, run_quality_gate  # noqa: E402

pytestmark = pytest.mark.spark


def df_of(spark, rows):
    return spark.createDataFrame(rows, ["ticket_id", "lon", "lat"])


def results_by_name(df):
    return {c.name: c.passed for c in run_quality_gate(df)}


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
