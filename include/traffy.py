"""Bronze ingestion logic for Traffy Fondue.

Bronze rule: reshape only. No cleaning, dedup, or category normalization here —
that is silver's job.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd
import requests


def select_in_window(features, start, end):
    """Keep features whose last_activity falls in the half-open window [start, end).

    This is the idempotency fix: the cutoff is a pure ARGUMENT (fed from Airflow's
    data_interval_start/end), not mutable global state. Re-running the same date
    passes the same window -> identical output. The interval is half-open so two
    adjacent days never both claim a ticket on the midnight boundary.

    Args:
        features: list of Traffy GeoJSON features. Each has
            feature["properties"]["last_activity"], a 'YYYY-MM-DD HH:MM:SS' string.
        start: window start, inclusive ('YYYY-MM-DD HH:MM:SS' string).
        end: window end, exclusive ('YYYY-MM-DD HH:MM:SS' string).

    Returns:
        list of features with start <= last_activity < end.
    """
    result = []

    for feature in features:
        last_activity = feature["properties"]["last_activity"]
        if start <= last_activity < end:
            result.append(feature)

    return result


def flatten_traffy(features, run_id):
    """Reshape Traffy GeoJSON features into a flat pandas DataFrame (one row each).

    Bronze rule: reshape only. Keep every source field as-is — do NOT clean,
    dedup, translate, or explode the problem_type_fondue array.

    For each feature, build one row that:
      - lifts everything in feature["properties"] up to top-level columns,
      - pulls feature["geometry"]["coordinates"] (a [lon, lat] pair) into
        separate "lon" and "lat" columns,
      - adds load metadata: "_ingested_at" (UTC now, ISO string),
        "_source" = "traffy", "_run_id" = run_id.

    Args:
        features: list of Traffy GeoJSON feature dicts.
        run_id: identifier for this ingestion run (string).

    Returns:
        pandas.DataFrame, one row per feature.
    """
    if not run_id:
        raise ValueError("run_id is required")

    ingested_at = dt.datetime.now(dt.UTC).isoformat()

    rows = []
    for feature in features:
        props = feature["properties"]
        lon, lat = feature["geometry"]["coordinates"]
        rows.append(
            {
                **props,  # lift every source property up to a top-level column
                "lon": lon,
                "lat": lat,
                "_ingested_at": ingested_at,
                "_source": "traffy",
                "_run_id": run_id,
            }
        )

    return pd.DataFrame(rows)

def fetch_traffy_data(api_url, limit):
    """Fetch Traffy Fondue data from the API.

    Args:
        api_url: the URL of the Traffy Fondue API endpoint.
        limit: the maximum number of features to fetch.

    Returns:
        A list of Traffy GeoJSON features.
    """
    params = {"limit": limit}
    response = requests.get(api_url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()["features"]