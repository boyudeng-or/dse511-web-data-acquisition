"""Acquisition, cleaning, validation, and analysis for a small NGSIM US-101 sample.

The notebook ``acquisition.ipynb`` runs the stages in order:
acquire -> clean -> validate -> analyse. Each stage reads only from the
previous stage's output folder, so raw data are never overwritten.
"""

from __future__ import annotations

import json
import logging
import math
import os
import platform
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import requests

PROJECT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = PROJECT_DIR / "config.json"
RAW_DIR = PROJECT_DIR / "data" / "raw"
CLEAN_DIR = PROJECT_DIR / "data" / "clean"
FIGURE_DIR = PROJECT_DIR / "figures"
LOG_DIR = PROJECT_DIR / "logs"

TOKEN_ENV_VAR = "SOCRATA_APP_TOKEN"
FEET_TO_METERS = 0.3048
TIME_HEADWAY_SENTINEL_S = 9999.99
VEHICLE_CLASS_LABELS = {1: "motorcycle", 2: "auto", 3: "truck"}
LANE_TYPE_LABELS = {
    1: "mainline", 2: "mainline", 3: "mainline", 4: "mainline", 5: "mainline",
    6: "auxiliary", 7: "on_ramp", 8: "off_ramp",
}
MANIFEST_NAME = "manifest.json"


# ---------------------------------------------------------------------------
# Configuration and logging
# ---------------------------------------------------------------------------

def load_config(config_path: Path = CONFIG_PATH) -> dict:
    """Read the JSON configuration that holds every query and cleaning parameter."""
    with open(config_path, encoding="utf-8") as config_file:
        return json.load(config_file)


def project_relative(path: Path) -> Path:
    """Shorten paths in log messages so outputs do not reveal the local folder structure."""
    try:
        return path.resolve().relative_to(PROJECT_DIR)
    except ValueError:
        return path


def get_logger(name: str = "ngsim_pipeline") -> logging.Logger:
    """Log to the console and to logs/pipeline.log (appended across runs)."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler = logging.FileHandler(LOG_DIR / "pipeline.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.setLevel(logging.INFO)
    return logger


# ---------------------------------------------------------------------------
# Stage 1: acquisition
# ---------------------------------------------------------------------------

class AcquisitionStopped(RuntimeError):
    """The source refused, throttled, or returned unusable data; we stop instead of retrying."""


def build_where_clause(config: dict) -> str:
    """SoQL filter: one freeway, the first N vehicle IDs, and one frame per second."""
    return (
        f"location='{config['location']}' "
        f"AND vehicle_id <= {int(config['max_vehicle_id'])} "
        f"AND frame_id % {int(config['frame_step'])} = 0"
    )


def resource_url(config: dict) -> str:
    return f"{config['portal_base_url']}/resource/{config['dataset_id']}.json"


def metadata_url(config: dict) -> str:
    return f"{config['portal_base_url']}/api/views/{config['dataset_id']}.json"


class SourceClient:
    """Thin wrapper around requests that enforces a delay, never retries, and logs every request."""

    def __init__(self, config: dict, request_log_path: Path, logger: logging.Logger):
        self.config = config
        self.request_log_path = request_log_path
        self.logger = logger
        self.session = requests.Session()
        self.session.headers["User-Agent"] = config["user_agent"]
        app_token = os.environ.get(TOKEN_ENV_VAR)
        self.token_used = bool(app_token)
        if app_token:
            self.session.headers["X-App-Token"] = app_token
        self._last_request_time: float | None = None

    def get(self, url: str, params: dict | None = None) -> requests.Response:
        if self._last_request_time is not None:
            elapsed = time.monotonic() - self._last_request_time
            remaining_delay = self.config["request_delay_s"] - elapsed
            if remaining_delay > 0:
                time.sleep(remaining_delay)
        requested_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self._last_request_time = time.monotonic()
        try:
            response = self.session.get(url, params=params, timeout=self.config["timeout_s"])
        except requests.RequestException as error:
            self._record(requested_at, url, params, status=None, note=str(error))
            raise AcquisitionStopped(f"Network error for {url}: {error}") from error

        self._record(requested_at, url, params, status=response.status_code, note="")
        if response.status_code in (401, 403, 429):
            raise AcquisitionStopped(
                f"Source refused or throttled the request (HTTP {response.status_code}); "
                "stopping without retrying."
            )
        if not response.ok:
            raise AcquisitionStopped(f"HTTP {response.status_code} for {response.url}")
        return response

    def _record(self, requested_at: str, url: str, params: dict | None, status: int | None, note: str) -> None:
        entry = {"requested_at_utc": requested_at, "url": url, "params": params or {},
                 "http_status": status, "note": note}
        with open(self.request_log_path, "a", encoding="utf-8") as log_file:
            log_file.write(json.dumps(entry) + "\n")
        level = logging.INFO if status == 200 else logging.WARNING
        self.logger.log(level, "GET %s params=%s -> %s %s", url, params or {}, status, note)


def find_run_dirs(raw_dir: Path = RAW_DIR) -> list[Path]:
    return sorted(path for path in raw_dir.glob("run_*") if path.is_dir())


def find_latest_complete_run(raw_dir: Path = RAW_DIR) -> Path | None:
    complete_runs = [path for path in find_run_dirs(raw_dir) if (path / MANIFEST_NAME).exists()]
    return complete_runs[-1] if complete_runs else None


def find_incomplete_run(raw_dir: Path = RAW_DIR) -> Path | None:
    incomplete_runs = [path for path in find_run_dirs(raw_dir) if not (path / MANIFEST_NAME).exists()]
    return incomplete_runs[-1] if incomplete_runs else None


def acquire(config: dict, force_refresh: bool = False, raw_dir: Path = RAW_DIR,
            logger: logging.Logger | None = None) -> Path:
    """Download the sample into a timestamped raw folder and return that folder.

    - Reuses the latest complete run unless ``force_refresh`` is True (no repeated downloads).
    - A refresh always writes a *new* run folder, so earlier raw data are never overwritten.
    - An interrupted run is resumed: pages already on disk are not requested again.
    """
    logger = logger or get_logger()
    latest_run = find_latest_complete_run(raw_dir)
    if latest_run is not None and not force_refresh:
        logger.info("Reusing cached raw data in %s (no requests sent).", project_relative(latest_run))
        return latest_run

    run_dir = find_incomplete_run(raw_dir)
    if run_dir is None:
        run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        run_dir = raw_dir / f"run_{run_stamp}"
        run_dir.mkdir(parents=True)
        logger.info("Starting new raw run folder %s", project_relative(run_dir))
    else:
        logger.info("Resuming incomplete raw run folder %s", project_relative(run_dir))

    client = SourceClient(config, run_dir / "requests_log.jsonl", logger)
    started_at = datetime.now(timezone.utc)

    metadata_path = run_dir / "dataset_metadata.json"
    if not metadata_path.exists():
        response = client.get(metadata_url(config))
        metadata_path.write_text(response.text, encoding="utf-8")

    where_clause = build_where_clause(config)
    count_path = run_dir / "row_count.json"
    if not count_path.exists():
        response = client.get(resource_url(config), {"$select": "count(*) AS n_rows", "$where": where_clause})
        count_path.write_text(response.text, encoding="utf-8")
    expected_rows = int(json.loads(count_path.read_text(encoding="utf-8"))[0]["n_rows"])
    if expected_rows > config["max_rows"]:
        raise AcquisitionStopped(
            f"Query matches {expected_rows} rows, above the max_rows limit of {config['max_rows']}; "
            "narrow the query in config.json instead of downloading more."
        )

    page_size = int(config["page_size"])
    n_pages = math.ceil(expected_rows / page_size)
    page_params_template = {
        "$select": ",".join(config["columns"]),
        "$where": where_clause,
        "$order": ":id",
        "$limit": page_size,
    }
    for page_index in range(n_pages):
        page_path = run_dir / f"page_{page_index:03d}.json"
        if page_path.exists():
            continue
        params = {**page_params_template, "$offset": page_index * page_size}
        response = client.get(resource_url(config), params)
        try:
            records = response.json()
        except ValueError as error:
            raise AcquisitionStopped(f"Page {page_index} was not valid JSON: {error}") from error
        if not isinstance(records, list) or not records:
            raise AcquisitionStopped(f"Page {page_index} returned no records; stopping.")
        page_path.write_text(response.text, encoding="utf-8")

    rows_per_page = {
        path.name: len(json.loads(path.read_text(encoding="utf-8")))
        for path in sorted(run_dir.glob("page_*.json"))
    }
    total_rows = sum(rows_per_page.values())
    if total_rows != expected_rows:
        logger.warning("Downloaded %d rows but the count query reported %d.", total_rows, expected_rows)

    source_metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    request_entries = [json.loads(line) for line in
                       (run_dir / "requests_log.jsonl").read_text(encoding="utf-8").splitlines() if line]
    finished_at = datetime.now(timezone.utc)
    manifest = {
        "source_name": config["source_name"],
        "dataset_page": f"{config['portal_base_url']}/d/{config['dataset_id']}",
        "resource_endpoint": resource_url(config),
        "metadata_endpoint": metadata_url(config),
        "query_template": page_params_template,
        "expected_rows": expected_rows,
        "downloaded_rows": total_rows,
        "rows_per_page": rows_per_page,
        "n_requests": len(request_entries),
        "app_token_used": client.token_used,
        "collection_started_utc": started_at.isoformat(timespec="seconds"),
        "collection_finished_utc": finished_at.isoformat(timespec="seconds"),
        "source_license": (source_metadata.get("license") or {}).get("name"),
        "source_license_terms": (source_metadata.get("license") or {}).get("termsLink"),
        "source_rows_updated_utc": datetime.fromtimestamp(
            source_metadata["rowsUpdatedAt"], tz=timezone.utc).isoformat(timespec="seconds")
        if source_metadata.get("rowsUpdatedAt") else None,
        "source_attribution": source_metadata.get("attribution"),
        "python_version": platform.python_version(),
        "requests_version": requests.__version__,
    }
    (run_dir / MANIFEST_NAME).write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    logger.info("Acquisition complete: %d rows in %d pages, %d requests.",
                total_rows, len(rows_per_page), len(request_entries))
    return run_dir


def load_raw_frames(run_dir: Path) -> pd.DataFrame:
    """Concatenate the raw JSON pages exactly as received (all values are strings)."""
    records: list[dict] = []
    for page_path in sorted(run_dir.glob("page_*.json")):
        records.extend(json.loads(page_path.read_text(encoding="utf-8")))
    return pd.DataFrame.from_records(records)


# ---------------------------------------------------------------------------
# Stage 2: cleaning (frame level)
# ---------------------------------------------------------------------------

class CleaningLog:
    """Collects one row per cleaning step so the notebook can show what changed."""

    def __init__(self) -> None:
        self.entries: list[dict] = []

    def add(self, step: str, description: str, rows_before: int, rows_after: int, values_affected: int) -> None:
        self.entries.append({"step": step, "description": description, "rows_before": rows_before,
                             "rows_after": rows_after, "values_affected": values_affected})

    def to_frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.entries)


def check_expected_columns(raw_frames: pd.DataFrame, expected_columns: list[str]) -> None:
    missing = sorted(set(expected_columns) - set(raw_frames.columns))
    if missing:
        raise ValueError(f"Raw data are missing expected columns: {missing}")


def convert_numeric_columns(frames: pd.DataFrame, columns: list[str], log: CleaningLog) -> pd.DataFrame:
    """The API returns every value as a string; convert and count values that fail to parse."""
    converted = frames.copy()
    n_failed = 0
    for column in columns:
        parsed = pd.to_numeric(converted[column], errors="coerce")
        n_failed += int((parsed.isna() & converted[column].notna()).sum())
        converted[column] = parsed
    log.add("convert_numeric", "Strings -> numbers; count of values that could not be parsed",
            len(frames), len(converted), n_failed)
    return converted


def remove_duplicate_frames(frames: pd.DataFrame, log: CleaningLog) -> pd.DataFrame:
    """Drop exact duplicates, then duplicates of the true key (vehicle_id, global_time).

    (vehicle_id, frame_id) is NOT a key: frame numbers restart in each collection period and
    vehicle IDs are reused across periods.
    """
    deduplicated = frames.drop_duplicates()
    deduplicated = deduplicated.drop_duplicates(subset=["vehicle_id", "global_time"])
    log.add("remove_duplicates", "Exact duplicates and duplicate (vehicle_id, global_time) rows",
            len(frames), len(deduplicated), len(frames) - len(deduplicated))
    return deduplicated.reset_index(drop=True)


def parse_timestamps(frames: pd.DataFrame, local_timezone: str, log: CleaningLog) -> pd.DataFrame:
    """global_time is milliseconds since 1970-01-01 UTC."""
    parsed = frames.copy()
    parsed["timestamp_utc"] = pd.to_datetime(parsed["global_time"], unit="ms", utc=True)
    parsed["timestamp_local"] = parsed["timestamp_utc"].dt.tz_convert(local_timezone)
    log.add("parse_timestamps", f"global_time (ms since epoch) -> UTC and {local_timezone} datetimes",
            len(frames), len(parsed), int(parsed["timestamp_utc"].notna().sum()))
    return parsed


def assign_trajectory_ids(frames: pd.DataFrame, max_gap_s: float, log: CleaningLog) -> pd.DataFrame:
    """Split each vehicle_id into separate trajectories wherever the time gap exceeds max_gap_s.

    The source states that repeated vehicle IDs are not associated, i.e. the same ID can belong
    to different vehicles in different collection periods.
    """
    ordered = frames.sort_values(["vehicle_id", "global_time"]).reset_index(drop=True)
    gap_s = ordered.groupby("vehicle_id")["global_time"].diff() / 1000
    starts_new_trajectory = gap_s.isna() | (gap_s > max_gap_s)
    segment_number = starts_new_trajectory.astype(int).groupby(ordered["vehicle_id"]).cumsum()
    segment_start = ordered.groupby([ordered["vehicle_id"], segment_number])["timestamp_local"].transform("min")
    ordered["trajectory_id"] = (
        "V" + ordered["vehicle_id"].astype(int).astype(str).str.zfill(4)
        + "_" + segment_start.dt.strftime("%H%M%S")
    )
    n_trajectories = ordered["trajectory_id"].nunique()
    log.add("assign_trajectory_ids",
            f"{ordered['vehicle_id'].nunique()} vehicle IDs split into {n_trajectories} trajectories "
            f"(gap > {max_gap_s} s starts a new vehicle)",
            len(frames), len(ordered), n_trajectories - ordered["vehicle_id"].nunique())
    return ordered


def convert_units(frames: pd.DataFrame, log: CleaningLog) -> pd.DataFrame:
    """Feet-based source units -> SI units."""
    converted = frames.copy()
    converted["vehicle_length_m"] = converted["v_length"] * FEET_TO_METERS
    converted["speed_mps"] = converted["v_vel"] * FEET_TO_METERS
    converted["accel_mps2"] = converted["v_acc"] * FEET_TO_METERS
    converted["space_headway_m"] = converted["space_headway"] * FEET_TO_METERS
    converted["time_headway_s"] = converted["time_headway"]
    log.add("convert_units", "ft, ft/s, ft/s^2 -> m, m/s, m/s^2 (x 0.3048)",
            len(frames), len(converted), 4 * len(converted))
    return converted


def label_categories(frames: pd.DataFrame, log: CleaningLog) -> pd.DataFrame:
    """Replace numeric codes with documented labels; unknown codes become missing and are counted."""
    labelled = frames.copy()
    labelled["vehicle_class"] = pd.Categorical(
        labelled["v_class"].map(VEHICLE_CLASS_LABELS), categories=["auto", "truck", "motorcycle"])
    labelled["lane_type"] = pd.Categorical(
        labelled["lane_id"].map(LANE_TYPE_LABELS), categories=["mainline", "auxiliary", "on_ramp", "off_ramp"])
    n_unknown = int(labelled["vehicle_class"].isna().sum() + labelled["lane_type"].isna().sum())
    log.add("label_categories", "v_class 1/2/3 -> motorcycle/auto/truck; lane_id -> lane_type; "
            "unknown codes counted", len(frames), len(labelled), n_unknown)
    return labelled


def mask_headway_sentinels(frames: pd.DataFrame, log: CleaningLog) -> pd.DataFrame:
    """Headway values that are codes rather than measurements become missing.

    - preceding == 0: no leader in the section, so space and time headway are recorded as 0.
    - time_headway == 9999.99: recorded for stopped vehicles (speed 0), i.e. infinite headway.
    - time_headway == 0 with a leader present cannot be a real headway.
    """
    masked = frames.copy()
    no_leader = masked["preceding"] == 0
    time_sentinel = np.isclose(masked["time_headway_s"], TIME_HEADWAY_SENTINEL_S)
    zero_time_with_leader = (masked["time_headway_s"] == 0) & ~no_leader
    masked.loc[no_leader, ["space_headway_m", "time_headway_s"]] = np.nan
    masked.loc[time_sentinel | zero_time_with_leader, "time_headway_s"] = np.nan
    n_affected = int(no_leader.sum() + time_sentinel.sum() + zero_time_with_leader.sum())
    log.add("mask_headway_sentinels",
            f"No leader: {int(no_leader.sum())}; 9999.99 (stopped): {int(time_sentinel.sum())}; "
            f"zero with leader: {int(zero_time_with_leader.sum())} -> NaN",
            len(frames), len(masked), n_affected)
    return masked


def flag_implausible_values(frames: pd.DataFrame, limits: dict, log: CleaningLog) -> pd.DataFrame:
    """Flag (not drop) values outside plausible ranges so the analyst can decide later."""
    flagged = frames.copy()
    speed_low, speed_high = limits["speed_mps"]
    flagged["flag_speed"] = ~flagged["speed_mps"].between(speed_low, speed_high)
    flagged["flag_accel"] = flagged["accel_mps2"].abs() > limits["abs_accel_mps2"]
    flagged["flag_time_headway"] = flagged["time_headway_s"] > limits["time_headway_s"]
    flagged["any_flag"] = flagged[["flag_speed", "flag_accel", "flag_time_headway"]].any(axis=1)
    log.add("flag_implausible_values",
            f"speed outside {limits['speed_mps']} m/s: {int(flagged['flag_speed'].sum())}; "
            f"|accel| > {limits['abs_accel_mps2']} m/s^2: {int(flagged['flag_accel'].sum())}; "
            f"time headway > {limits['time_headway_s']} s: {int(flagged['flag_time_headway'].sum())}",
            len(frames), len(flagged), int(flagged["any_flag"].sum()))
    return flagged


FRAME_COLUMNS = [
    "trajectory_id", "vehicle_id", "frame_id", "total_frames", "timestamp_local", "timestamp_utc",
    "vehicle_class", "vehicle_length_m", "lane_id", "lane_type", "speed_mps", "accel_mps2",
    "preceding", "space_headway_m", "time_headway_s",
    "flag_speed", "flag_accel", "flag_time_headway", "any_flag",
]


def clean_frames(raw_frames: pd.DataFrame, config: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run every frame-level cleaning step in order; return the clean frames and the step log."""
    log = CleaningLog()
    check_expected_columns(raw_frames, config["columns"])
    frames = convert_numeric_columns(raw_frames, config["columns"], log)
    frames = remove_duplicate_frames(frames, log)
    frames = parse_timestamps(frames, config["local_timezone"], log)
    frames = assign_trajectory_ids(frames, config["trajectory_gap_s"], log)
    frames = convert_units(frames, log)
    frames = label_categories(frames, log)
    frames = mask_headway_sentinels(frames, log)
    frames = flag_implausible_values(frames, config["plausible_limits"], log)
    frames = frames[FRAME_COLUMNS].astype({"vehicle_id": "int64", "frame_id": "int64",
                                           "total_frames": "int64", "lane_id": "int64",
                                           "preceding": "int64"})
    return frames.sort_values(["trajectory_id", "timestamp_utc"]).reset_index(drop=True), log.to_frame()


# ---------------------------------------------------------------------------
# Stage 2b: one row per vehicle trajectory (the unit of observation)
# ---------------------------------------------------------------------------

def assign_collection_period(start_times_local: pd.Series, end_times_local: pd.Series,
                             min_gap_minutes: float = 5) -> pd.Series:
    """Group trajectories into collection periods separated by gaps of at least min_gap_minutes.

    Vehicle IDs restart in every NGSIM collection period, so a filter on low vehicle IDs returns
    a short burst of the earliest vehicles from each period rather than continuous coverage.
    Labels show the actual local time span covered by the sample, e.g. "P1 07:49-07:51".
    """
    order = start_times_local.sort_values()
    new_period = order.diff() > pd.Timedelta(minutes=min_gap_minutes)
    period_number = new_period.cumsum().add(1).reindex(start_times_local.index)
    labels = {}
    for number in sorted(period_number.unique()):
        in_period = period_number == number
        labels[number] = (f"P{number} {start_times_local[in_period].min():%H:%M}"
                          f"-{end_times_local[in_period].max():%H:%M}")
    return pd.Categorical(period_number.map(labels), categories=[labels[n] for n in sorted(labels)], ordered=True)


def count_lane_changes(lane_ids: pd.Series) -> int:
    return int((lane_ids.diff().fillna(0) != 0).sum())


def build_vehicle_table(frames: pd.DataFrame, frame_step: int) -> pd.DataFrame:
    """Aggregate the 1-second frames to one tidy row per vehicle trajectory.

    Headway summaries use mainline lanes only, because ramp and auxiliary-lane following is not
    comparable with mainline car following.
    """
    on_mainline = frames["lane_type"] == "mainline"
    frames = frames.assign(
        mainline_time_headway_s=frames["time_headway_s"].where(on_mainline),
        mainline_space_headway_m=frames["space_headway_m"].where(on_mainline),
    )
    grouped = frames.groupby("trajectory_id", sort=True, observed=True)
    vehicles = grouped.agg(
        vehicle_id=("vehicle_id", "first"),
        vehicle_class=("vehicle_class", "first"),
        vehicle_length_m=("vehicle_length_m", "median"),
        start_time_local=("timestamp_local", "min"),
        end_time_local=("timestamp_local", "max"),
        total_frames_source=("total_frames", "first"),
        n_sampled_frames=("frame_id", "size"),
        mean_speed_mps=("speed_mps", "mean"),
        sd_accel_mps2=("accel_mps2", "std"),
        median_time_headway_s=("mainline_time_headway_s", "median"),
        median_space_headway_m=("mainline_space_headway_m", "median"),
        n_valid_time_headway=("mainline_time_headway_s", "count"),
        most_common_lane=("lane_id", lambda lanes: int(lanes.mode().iloc[0])),
        n_flagged_frames=("any_flag", "sum"),
    ).reset_index()
    vehicles["n_lane_changes"] = grouped["lane_id"].apply(count_lane_changes).to_numpy()
    vehicles["duration_s"] = (vehicles["end_time_local"] - vehicles["start_time_local"]).dt.total_seconds()
    vehicles["completeness_ratio"] = vehicles["n_sampled_frames"] / (vehicles["total_frames_source"] / frame_step)
    vehicles["collection_period"] = assign_collection_period(vehicles["start_time_local"],
                                                             vehicles["end_time_local"])
    vehicles["vehicle_class"] = pd.Categorical(vehicles["vehicle_class"], categories=["auto", "truck", "motorcycle"])
    ordered_columns = [
        "trajectory_id", "vehicle_id", "collection_period", "vehicle_class", "vehicle_length_m",
        "start_time_local", "end_time_local", "duration_s", "n_sampled_frames", "total_frames_source",
        "completeness_ratio", "mean_speed_mps", "sd_accel_mps2", "median_time_headway_s",
        "median_space_headway_m", "n_valid_time_headway", "most_common_lane", "n_lane_changes",
        "n_flagged_frames",
    ]
    return vehicles[ordered_columns]


# ---------------------------------------------------------------------------
# Stage 3: validation
# ---------------------------------------------------------------------------

VEHICLE_SCHEMA = {
    "trajectory_id": "str",
    "vehicle_id": "int64",
    "vehicle_class": "category",
    "vehicle_length_m": "float64",
    "duration_s": "float64",
    "n_sampled_frames": "int64",
    "mean_speed_mps": "float64",
    "median_time_headway_s": "float64",
    "most_common_lane": "int64",
    "n_lane_changes": "int64",
}


def dtype_matches(series: pd.Series, expected: str) -> bool:
    if expected == "str":
        return pd.api.types.is_string_dtype(series)
    if expected == "category":
        return isinstance(series.dtype, pd.CategoricalDtype)
    return str(series.dtype) == expected


def validate(frames: pd.DataFrame, vehicles: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Return a table of named checks; the notebook asserts that every check passed."""
    per_trajectory = frames.groupby("trajectory_id", observed=True)
    checks = [
        ("at_least_min_vehicles", len(vehicles) >= config["min_vehicles"],
         f"{len(vehicles)} trajectories (minimum {config['min_vehicles']})"),
        ("trajectory_id_unique", vehicles["trajectory_id"].is_unique, "one row per trajectory"),
        ("frame_key_unique", not frames.duplicated(["trajectory_id", "timestamp_utc"]).any(),
         "(trajectory_id, timestamp) is unique"),
        ("one_class_per_trajectory", bool((per_trajectory["vehicle_class"].nunique() == 1).all()),
         "vehicle class constant within each trajectory"),
        ("one_length_per_trajectory", bool((per_trajectory["vehicle_length_m"].nunique() == 1).all()),
         "vehicle length constant within each trajectory"),
        ("one_source_trajectory_per_id", bool((per_trajectory["total_frames"].nunique() == 1).all()),
         "total_frames constant within each trajectory (split matched the source)"),
        ("frames_increase_within_trajectory",
         bool(per_trajectory["frame_id"].apply(lambda ids: ids.is_monotonic_increasing).all()),
         "frame_id increases within each trajectory"),
        ("no_missing_keys", not vehicles[["trajectory_id", "vehicle_class", "mean_speed_mps"]].isna().any().any(),
         "key columns have no missing values"),
        ("speeds_plausible", bool(vehicles["mean_speed_mps"].between(*config["plausible_limits"]["speed_mps"]).all()),
         f"mean speed within {config['plausible_limits']['speed_mps']} m/s"),
        ("collection_date", bool((frames["timestamp_local"].dt.date.astype(str) == "2005-06-15").all()),
         "all frames on 2005-06-15 (local)"),
        ("trajectories_complete", bool(vehicles["completeness_ratio"].between(0.9, 1.1).all()),
         "sampled frames match source total_frames / frame_step within 10% "
         f"(range {vehicles['completeness_ratio'].min():.3f}-{vehicles['completeness_ratio'].max():.3f})"),
    ]
    for column, expected in VEHICLE_SCHEMA.items():
        checks.append((f"dtype_{column}", dtype_matches(vehicles[column], expected),
                       f"{column} is {expected} (actual {vehicles[column].dtype})"))
    return pd.DataFrame(checks, columns=["check", "passed", "detail"])


# ---------------------------------------------------------------------------
# Outputs: clean files and data dictionary
# ---------------------------------------------------------------------------

DATA_DICTIONARY = [
    ("trajectory_id", "string", "", "One vehicle trajectory: V<vehicle_id>_<local start time HHMMSS>"),
    ("vehicle_id", "int", "", "Source vehicle ID; reused across collection periods, so not unique"),
    ("collection_period", "category", "", "Burst of the sample the trajectory belongs to, with its local time span"),
    ("vehicle_class", "category", "", "auto, truck, or motorcycle (source v_class 2, 3, 1)"),
    ("vehicle_length_m", "float", "m", "Vehicle length"),
    ("start_time_local", "datetime", "America/Los_Angeles", "First sampled frame"),
    ("end_time_local", "datetime", "America/Los_Angeles", "Last sampled frame"),
    ("duration_s", "float", "s", "Time between first and last sampled frame"),
    ("n_sampled_frames", "int", "", "Frames kept after 1-per-second sampling"),
    ("total_frames_source", "int", "", "Source total_frames (0.1 s frames) for this trajectory"),
    ("completeness_ratio", "float", "", "n_sampled_frames / (total_frames_source / 10); ~1 means complete"),
    ("mean_speed_mps", "float", "m/s", "Mean instantaneous speed"),
    ("sd_accel_mps2", "float", "m/s^2", "Standard deviation of acceleration (source is clipped at +/-3.41)"),
    ("median_time_headway_s", "float", "s", "Median time headway on mainline lanes 1-5, sentinels removed"),
    ("median_space_headway_m", "float", "m", "Median front-to-front spacing on mainline lanes 1-5"),
    ("n_valid_time_headway", "int", "", "Frames contributing to median_time_headway_s"),
    ("most_common_lane", "int", "", "Modal lane_id (1 = leftmost ... 5 = rightmost; 6-8 auxiliary/ramps)"),
    ("n_lane_changes", "int", "", "Number of lane_id changes between consecutive sampled frames"),
    ("n_flagged_frames", "int", "", "Frames with any implausible-value flag"),
]


def save_clean_outputs(frames: pd.DataFrame, vehicles: pd.DataFrame, cleaning_log: pd.DataFrame,
                       validation_report: pd.DataFrame, clean_dir: Path = CLEAN_DIR) -> dict[str, Path]:
    clean_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "vehicles": clean_dir / "ngsim_us101_vehicles.csv",
        "frames": clean_dir / "ngsim_us101_frames_1hz.csv",
        "cleaning_log": clean_dir / "cleaning_log.csv",
        "validation_report": clean_dir / "validation_report.csv",
        "data_dictionary": clean_dir / "data_dictionary.csv",
    }
    vehicles.to_csv(paths["vehicles"], index=False)
    frames.to_csv(paths["frames"], index=False)
    cleaning_log.to_csv(paths["cleaning_log"], index=False)
    validation_report.to_csv(paths["validation_report"], index=False)
    pd.DataFrame(DATA_DICTIONARY, columns=["column", "type", "unit", "description"]).to_csv(
        paths["data_dictionary"], index=False)
    return paths


# ---------------------------------------------------------------------------
# Stage 4: analysis
# ---------------------------------------------------------------------------

def summarise_by_class(vehicles: pd.DataFrame) -> pd.DataFrame:
    """Descriptive statistics of speed and time headway for each vehicle class."""
    summary = vehicles.groupby("vehicle_class", observed=True).agg(
        n_vehicles=("trajectory_id", "size"),
        n_with_headway=("median_time_headway_s", "count"),
        mean_speed_mps=("mean_speed_mps", "mean"),
        median_time_headway_s=("median_time_headway_s", "median"),
        q25_time_headway_s=("median_time_headway_s", lambda values: values.quantile(0.25)),
        q75_time_headway_s=("median_time_headway_s", lambda values: values.quantile(0.75)),
        median_space_headway_m=("median_space_headway_m", "median"),
    )
    return summary.round(2)


def summarise_by_period(vehicles: pd.DataFrame) -> pd.DataFrame:
    """Descriptive statistics for each collection period."""
    summary = vehicles.groupby("collection_period", observed=True).agg(
        n_vehicles=("trajectory_id", "size"),
        mean_speed_mps=("mean_speed_mps", "mean"),
        sd_speed_mps=("mean_speed_mps", "std"),
        median_time_headway_s=("median_time_headway_s", "median"),
        median_space_headway_m=("median_space_headway_m", "median"),
        mean_lane_changes=("n_lane_changes", "mean"),
    )
    return summary.round(2)


def plot_headway_by_class(vehicles: pd.DataFrame, output_path: Path):
    import matplotlib.pyplot as plt

    classes = [label for label in ["auto", "truck", "motorcycle"]
               if vehicles.loc[vehicles["vehicle_class"] == label, "median_time_headway_s"].notna().any()]
    samples = [vehicles.loc[vehicles["vehicle_class"] == label, "median_time_headway_s"].dropna()
               for label in classes]
    figure, axis = plt.subplots(figsize=(7, 4.5))
    axis.boxplot(samples, showfliers=True)
    axis.set_xticks(range(1, len(classes) + 1),
                    [f"{label}\n(n = {len(sample)})" for label, sample in zip(classes, samples)])
    axis.set_ylabel("Median time headway per vehicle (s)")
    axis.set_title("US-101, 2005-06-15: time headway by vehicle class\n"
                   "(three ~2-minute bursts 07:49-08:22, mainline lanes, 1 Hz sample)")
    axis.grid(axis="y", alpha=0.3)
    figure.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output_path, dpi=150)
    return figure


def plot_speed_by_period(vehicles: pd.DataFrame, output_path: Path):
    import matplotlib.pyplot as plt

    periods = list(vehicles["collection_period"].cat.categories)
    samples = [vehicles.loc[vehicles["collection_period"] == period, "mean_speed_mps"] for period in periods]
    figure, axis = plt.subplots(figsize=(7, 4.5))
    axis.boxplot(samples, showfliers=True)
    axis.set_xticks(range(1, len(periods) + 1),
                    [f"{period}\n(n = {len(sample)})" for period, sample in zip(periods, samples)])
    axis.set_ylabel("Mean speed per vehicle (m/s)")
    axis.set_xlabel("Collection period (local time covered by the sample)")
    axis.set_title("US-101, 2005-06-15: vehicle mean speed by collection period")
    axis.grid(axis="y", alpha=0.3)
    figure.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output_path, dpi=150)
    return figure
