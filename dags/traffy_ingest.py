"""Daily bronze ingestion DAG for Traffy Fondue.

Thin by design — all real logic lives in include/. Each run processes ONE day's
window (its data_interval), so re-runs and backfills are idempotent: same date in,
same partition out.
"""

from __future__ import annotations

import os

import pendulum
from airflow.decorators import dag, task

# dags/ and include/ share the project root, which is on the path inside the Airflow image.
from include.storage import write_bronze_parquet
from include.traffy import fetch_traffy_data, flatten_traffy, select_in_window

TRAFFY_URL = os.environ.get(
    "TRAFFY_API_URL",
    "https://publicapi.traffy.in.th/teamchadchart-stat-api/geojson/v1",
)
FETCH_LIMIT = 1000


@dag(
    dag_id="traffy_ingest",
    schedule="@daily",
    # tz fix (1/2): run in Bangkok local time so data_interval matches last_activity's zone
    start_date=pendulum.datetime(2026, 6, 1, tz="Asia/Bangkok"),
    catchup=False,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["bronze", "traffy"],
)
def traffy_ingest():
    @task
    def load_bronze(data_interval_start=None, data_interval_end=None):
        # tz fix (2/2): interval is Bangkok-local (DAG tz) -> format to last_activity's shape
        start = data_interval_start.strftime("%Y-%m-%d %H:%M:%S")
        end = data_interval_end.strftime("%Y-%m-%d %H:%M:%S")
        run_date = data_interval_start.strftime("%Y-%m-%d")

        features = fetch_traffy_data(TRAFFY_URL, FETCH_LIMIT)
        windowed = select_in_window(features, start, end)
        df = flatten_traffy(windowed, run_id=run_date)
        out = write_bronze_parquet(df, source="traffy", run_date=run_date)

        print(f"landed {len(df)} rows -> {out}") 
        return out

    load_bronze()


traffy_ingest()
