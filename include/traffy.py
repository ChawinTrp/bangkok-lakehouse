"""Bronze ingestion logic for Traffy Fondue.

Bronze rule: reshape only. No cleaning, dedup, or category normalization here —
that is silver's job.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd


def filter_new_tickets(features, watermark):
    """Keep only tickets new since the watermark; return the advanced watermark.

    Args:
        features: list of Traffy GeoJSON features. Each one has
            feature["properties"]["last_activity"], a 'YYYY-MM-DD HH:MM:SS' string.
        watermark: the highest last_activity we have already processed
            (a 'YYYY-MM-DD HH:MM:SS' string), or None on the first ever run.

    Returns:
        (new_features, new_watermark)
        - new_features: features with last_activity >= watermark
          (all of them if watermark is None).
        - new_watermark: the new high-water mark. It must NEVER move backwards.
    """
    if not features:
        return [], watermark

    if watermark is None:
        return features, max(feature["properties"]["last_activity"] for feature in features)

    new_features = []

    for feature in features:
        if feature["properties"]["last_activity"] >= watermark:
            new_features.append(feature)

    if not new_features:
        return [], watermark
    new_watermark = max(feature["properties"]["last_activity"] for feature in new_features)

    return new_features, new_watermark


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