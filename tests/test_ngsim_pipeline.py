"""Unit tests for the parsing and cleaning functions (no network access).

Run from the project folder:  python -m pytest -q
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import ngsim_pipeline as pipeline  # noqa: E402


def make_numeric_frames(rows: list[dict]) -> pd.DataFrame:
    frames = pd.DataFrame(rows)
    return pipeline.parse_timestamps(frames, "America/Los_Angeles", pipeline.CleaningLog())


def test_build_where_clause_uses_config_values():
    config = {"location": "us-101", "max_vehicle_id": 150, "frame_step": 10}
    assert pipeline.build_where_clause(config) == (
        "location='us-101' AND vehicle_id <= 150 AND frame_id % 10 = 0")


def test_convert_numeric_counts_unparseable_values():
    raw = pd.DataFrame({"v_vel": ["10.5", "abc", None]})
    log = pipeline.CleaningLog()
    converted = pipeline.convert_numeric_columns(raw, ["v_vel"], log)
    assert converted["v_vel"].iloc[0] == pytest.approx(10.5)
    assert np.isnan(converted["v_vel"].iloc[1])
    assert log.entries[0]["values_affected"] == 1


def test_convert_units_feet_to_meters():
    frames = pd.DataFrame({"v_length": [10.0], "v_vel": [10.0], "v_acc": [-10.0],
                           "space_headway": [100.0], "time_headway": [2.0]})
    converted = pipeline.convert_units(frames, pipeline.CleaningLog())
    assert converted["vehicle_length_m"].iloc[0] == pytest.approx(3.048)
    assert converted["speed_mps"].iloc[0] == pytest.approx(3.048)
    assert converted["accel_mps2"].iloc[0] == pytest.approx(-3.048)
    assert converted["space_headway_m"].iloc[0] == pytest.approx(30.48)
    assert converted["time_headway_s"].iloc[0] == pytest.approx(2.0)


def test_mask_headway_sentinels():
    frames = pd.DataFrame({
        "preceding": [0, 12, 12, 12],
        "space_headway_m": [0.0, 15.0, 4.0, 20.0],
        "time_headway_s": [0.0, 9999.99, 0.0, 1.8],
    })
    masked = pipeline.mask_headway_sentinels(frames, pipeline.CleaningLog())
    assert masked["space_headway_m"].isna().tolist() == [True, False, False, False]
    assert masked["time_headway_s"].isna().tolist() == [True, True, True, False]
    assert masked["time_headway_s"].iloc[3] == pytest.approx(1.8)


def test_assign_trajectory_ids_splits_reused_vehicle_ids():
    base_ms = 1118846979700
    frames = make_numeric_frames([
        {"vehicle_id": 7, "global_time": base_ms},
        {"vehicle_id": 7, "global_time": base_ms + 1000},
        {"vehicle_id": 7, "global_time": base_ms + 900_000},
        {"vehicle_id": 8, "global_time": base_ms},
    ])
    labelled = pipeline.assign_trajectory_ids(frames, max_gap_s=5, log=pipeline.CleaningLog())
    trajectories_of_7 = labelled.loc[labelled["vehicle_id"] == 7, "trajectory_id"]
    assert trajectories_of_7.nunique() == 2
    assert labelled["trajectory_id"].nunique() == 3
    assert trajectories_of_7.iloc[0] == "V0007_074939"


def test_label_categories_maps_documented_codes():
    frames = pd.DataFrame({"v_class": [1, 2, 3], "lane_id": [1, 7, 8]})
    labelled = pipeline.label_categories(frames, pipeline.CleaningLog())
    assert labelled["vehicle_class"].tolist() == ["motorcycle", "auto", "truck"]
    assert labelled["lane_type"].tolist() == ["mainline", "on_ramp", "off_ramp"]
