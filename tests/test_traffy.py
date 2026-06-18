"""Tests for include/traffy.py — the bronze ingestion logic.

select_in_window is a pure function: in = features + a [start, end) window,
out = the features inside that window. No network, no files, no hidden state —
so re-running with the same inputs always gives the same output (idempotent).
"""

import pandas as pd

from include.traffy import flatten_traffy, select_in_window


def feat(ticket_id, last_activity):
    """Build a minimal Traffy GeoJSON feature (only the fields these tests need)."""
    return {"properties": {"ticket_id": ticket_id, "last_activity": last_activity}}


def ids(features):
    return [f["properties"]["ticket_id"] for f in features]


def test_window_keeps_only_tickets_inside():
    features = [
        feat("BEFORE", "2026-06-16 23:59:59"),
        feat("IN1", "2026-06-17 00:00:00"),
        feat("IN2", "2026-06-17 12:30:00"),
        feat("AFTER", "2026-06-18 00:00:00"),
    ]
    out = select_in_window(features, "2026-06-17 00:00:00", "2026-06-18 00:00:00")
    assert ids(out) == ["IN1", "IN2"]


def test_window_start_inclusive_end_exclusive():
    # half-open [start, end): the start instant is in, the end instant is out
    features = [feat("START", "2026-06-17 00:00:00"), feat("END", "2026-06-18 00:00:00")]
    out = select_in_window(features, "2026-06-17 00:00:00", "2026-06-18 00:00:00")
    assert ids(out) == ["START"]


def test_window_no_matches():
    features = [feat("OLD", "2026-06-01 00:00:00")]
    out = select_in_window(features, "2026-06-17 00:00:00", "2026-06-18 00:00:00")
    assert out == []


def test_window_empty_features():
    assert select_in_window([], "2026-06-17 00:00:00", "2026-06-18 00:00:00") == []


def test_window_is_replayable():
    # same inputs -> same output, every time (no global watermark state)
    features = [feat("A", "2026-06-17 09:00:00"), feat("B", "2026-06-17 10:00:00")]
    first = select_in_window(features, "2026-06-17 00:00:00", "2026-06-18 00:00:00")
    second = select_in_window(features, "2026-06-17 00:00:00", "2026-06-18 00:00:00")
    assert ids(first) == ids(second) == ["A", "B"]


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
