"""Daily bronze ingestion DAG for Traffy Fondue.

Thin by design — all real logic lives in include/. Each run processes ONE day's
window (its data_interval), so re-runs and backfills are idempotent: same date in,
same partition out.
"""

from __future__ import annotations

import os

import pendulum
from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# dags/ and include/ share the project root, which is on the path inside the Airflow image.
from include.storage import write_bronze_parquet
from include.traffy import fetch_traffy_until, flatten_traffy, select_in_window

# Host path of the repo — DockerOperator binds it on the HOST daemon (DooD), not in-container.
HOST_PROJECT_PATH = os.environ["HOST_PROJECT_PATH"]

TRAFFY_URL = os.environ.get(
    "TRAFFY_API_URL",
    "https://publicapi.traffy.in.th/teamchadchart-stat-api/geojson/v1",
)
PAGE_SIZE = 500  # small pages: fast requests + cover the whole day by paginating


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
    def load_bronze():
        # The live endpoint is a newest-N SNAPSHOT, not date-addressable, so this job
        # captures "today so far" in Bangkok time and overwrites today's partition.
        # (Historical/backfill by logical_date is the monthly-archive DAG's job.)
        now_bkk = pendulum.now("Asia/Bangkok")
        run_date = now_bkk.strftime("%Y-%m-%d")
        start = now_bkk.start_of("day").strftime("%Y-%m-%d %H:%M:%S")  # today 00:00 Bangkok
        end = now_bkk.strftime("%Y-%m-%d %H:%M:%S")  # now

        # paginate the newest-first feed until it dips below today's start, then window it
        features = fetch_traffy_until(TRAFFY_URL, boundary=start, page_size=PAGE_SIZE)
        windowed = select_in_window(features, start, end)
        df = flatten_traffy(windowed, run_id=run_date)
        out = write_bronze_parquet(df, source="traffy", run_date=run_date)

        print(f"landed {len(df)} rows -> {out}")
        return out

    # Silver runs as a SEPARATE container (the bangkok-spark image): Airflow triggers
    # Spark, it doesn't run Spark in-process. DooD = the host daemon launches it.
    silver = DockerOperator(
        task_id="silver_transform",
        image="bangkok-spark",
        command="python -m spark.transforms.silver_traffy",
        working_dir="/app",
        mounts=[Mount(source=HOST_PROJECT_PATH, target="/app", type="bind")],
        environment={"LAKEHOUSE_ROOT": "data"},
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        auto_remove="success",
        mount_tmp_dir=False,  # required under DooD: don't try to bind a worker tmp dir
    )

    # Gold runs the same way silver does — a separate bangkok-spark container that
    # reads silver and builds the star schema (dims + the two snapshot facts).
    gold = DockerOperator(
        task_id="gold_transform",
        image="bangkok-spark",
        command="python -m spark.transforms.gold_traffy",
        working_dir="/app",
        mounts=[Mount(source=HOST_PROJECT_PATH, target="/app", type="bind")],
        environment={"LAKEHOUSE_ROOT": "data"},
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        auto_remove="success",
        mount_tmp_dir=False,  # required under DooD: don't try to bind a worker tmp dir
    )

    load_bronze() >> silver >> gold


traffy_ingest()
