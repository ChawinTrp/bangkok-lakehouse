"""Shared pytest fixtures. The Spark session is session-scoped (expensive to start,
so create one and reuse it) and only spins up when a test actually requests it."""

import pytest


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    s = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
