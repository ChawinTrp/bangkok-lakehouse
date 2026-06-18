"""Tests for include/traffy.py — the bronze ingestion logic.

We test filter_new_tickets first (the watermark). It is a pure function:
in = features + a watermark, out = the new features + the advanced watermark.
No network, no files — so it runs in milliseconds and is easy to reason about.
"""

import pandas as pd

from include.traffy import filter_new_tickets, flatten_traffy


def feat(ticket_id, last_activity):
    """Build a minimal Traffy GeoJSON feature (only the fields these tests need)."""
    return {"properties": {"ticket_id": ticket_id, "last_activity": last_activity}}


def test_first_run_no_watermark_keeps_everything():
    # On the very first run there is no watermark yet -> take all features.
    features = [feat("A", "2026-06-15 09:00:00"), feat("B", "2026-06-16 10:00:00")]
    new, watermark = filter_new_tickets(features, None)
    assert {f["properties"]["ticket_id"] for f in new} == {"A", "B"}
    assert watermark == "2026-06-16 10:00:00"  # advanced to the newest seen


def test_keeps_only_tickets_at_or_after_watermark():
    features = [
        feat("OLD", "2026-06-15 08:00:00"),
        feat("NEW", "2026-06-16 12:00:00"),
    ]
    new, watermark = filter_new_tickets(features, "2026-06-16 00:00:00")
    assert [f["properties"]["ticket_id"] for f in new] == ["NEW"]
    assert watermark == "2026-06-16 12:00:00"


def test_boundary_ticket_is_kept():
    # A ticket whose last_activity == watermark is KEPT (>=): reprocess, don't drop.
    features = [feat("EDGE", "2026-06-16 00:00:00")]
    new, _ = filter_new_tickets(features, "2026-06-16 00:00:00")
    assert [f["properties"]["ticket_id"] for f in new] == ["EDGE"]


def test_all_old_returns_none_and_watermark_does_not_go_backward():
    features = [feat("OLD", "2026-06-10 00:00:00")]
    new, watermark = filter_new_tickets(features, "2026-06-16 00:00:00")
    assert new == []
    assert watermark == "2026-06-16 00:00:00"  # must NOT regress to the old ticket's date


def test_empty_features_keeps_watermark():
    new, watermark = filter_new_tickets([], "2026-06-16 00:00:00")
    assert new == []
    assert watermark == "2026-06-16 00:00:00"

def test_empty_features_and_no_watermark_returns_none():
    new, watermark = filter_new_tickets([], None)
    assert new == []
    assert watermark is None


# --- flatten_traffy: nested GeoJSON feature -> flat DataFrame row ---

def gj(ticket_id, lon, lat, district, problem_types, last_activity="2026-06-16 10:00:00"):
    """Build a minimal Traffy GeoJSON feature with geometry + properties."""
    return {
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "properties": {
            "ticket_id": ticket_id,
            "last_activity": last_activity,
            "district": district,
            "problem_type_fondue": problem_types,
        },
    }


def test_flatten_one_row_per_feature():
    feats = [
        gj("A", 100.5, 13.8, "Chatuchak", ["tree"]),
        gj("B", 100.6, 13.7, "Bang Rak", ["road", "light"]),
    ]
    df = flatten_traffy(feats, run_id="run-1")
    assert len(df) == 2


def test_flatten_splits_coordinates_into_lon_lat():
    df = flatten_traffy([gj("A", 100.5562, 13.80667, "Chatuchak", ["tree"])], run_id="r")
    assert df.loc[0, "lon"] == 100.5562
    assert df.loc[0, "lat"] == 13.80667


def test_flatten_lifts_properties_to_columns():
    df = flatten_traffy([gj("A", 100.5, 13.8, "Chatuchak", ["tree"])], run_id="r")
    assert df.loc[0, "ticket_id"] == "A"
    assert df.loc[0, "district"] == "Chatuchak"


def test_flatten_keeps_problem_type_array_unchanged():
    # bronze rule: do NOT explode or clean the array — keep it exactly as the source gave it
    df = flatten_traffy([gj("A", 100.5, 13.8, "Chatuchak", ["tree", "flood"])], run_id="r")
    assert df.loc[0, "problem_type_fondue"] == ["tree", "flood"]


def test_flatten_adds_load_metadata():
    df = flatten_traffy([gj("A", 100.5, 13.8, "Chatuchak", ["tree"])], run_id="run-42")
    assert df.loc[0, "_source"] == "traffy"
    assert df.loc[0, "_run_id"] == "run-42"
    assert pd.notna(df.loc[0, "_ingested_at"])
