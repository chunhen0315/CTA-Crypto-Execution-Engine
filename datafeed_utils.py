import ast
import json
import hashlib
import os
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

import config_data


SNAPSHOT_RETENTION_DAYS = 30
DATA_REVISION_TOLERANCE = 1e-7
DATA_REVISION_TOLERANCE_PERCENT = 1.0
REVISION_IGNORED_COLUMNS = {"datetime", "start_time"}
ATOMIC_REPLACE_ATTEMPTS = 5
ATOMIC_REPLACE_DELAYS = (0.25, 0.5, 1.0, 2.0)


@dataclass
class FetchResult:
    success: bool
    topic: str
    mode: str
    attempts: int
    rows_received: int = 0
    rows_saved: int = 0
    latest_time: str | None = None
    actual_age_minutes: float | None = None
    conversion_failures: int = 0
    revision_rows: int = 0
    error: str | None = None

    def to_dict(self):
        return asdict(self)


@dataclass
class HistoricalRevisionSummary:
    changed_rows: int = 0
    changed_cells: int = 0
    earliest_change: str | None = None
    latest_change: str | None = None
    max_numeric_difference: float | None = None
    compared_snapshots: int = 0
    snapshots_with_changes: int = 0
    unreadable_snapshots: int = 0
    candidates: dict = field(default_factory=dict, repr=False)


def atomic_write_json(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    with temp_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2)
        file.flush()
        os.fsync(file.fileno())
    os.replace(temp_path, path)


def read_json(path, default):
    path = Path(path)
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as file:
            result = json.load(file)
        return result if isinstance(result, dict) else default
    except Exception:
        return default


def topic_filename(topic, asset, interval):
    endpoint = topic.split("|")[-1].split("?")[0].replace("/", "_")
    return f"{endpoint}_{asset}_{interval}.parquet"


ALPHA_LIBRARY_PATH = Path(__file__).resolve().with_name("alpha_lib.py")

PROVIDER_CONFIG = {
    "CQ_DATA_FILEPATH": ("cryptoquant", "CQ_BTC1h_topic"),
    "GN_DATA_FILEPATH": ("glassnode", "GN_BTC1h_topic"),
}


def _configured_topic_filenames(provider, config_name):
    configured = {}
    invalid = []
    topics = getattr(config_data, config_name, None)
    if not isinstance(topics, (list, tuple)):
        raise ValueError(
            f"config_data.{config_name} must be a list or tuple of topics"
        )

    for value in topics:
        topic = str(value or "").strip()
        if not topic:
            continue
        topic_provider = topic.split("|", 1)[0].strip().lower()
        if topic_provider != provider:
            invalid.append(topic)
            continue
        filename = topic_filename(
            topic,
            config_data.ASSET,
            config_data.INTERVAL,
        )
        configured[filename] = topic
    return configured, invalid


def _required_alpha_files(alpha_ids):
    tree = ast.parse(
        ALPHA_LIBRARY_PATH.read_text(encoding="utf-8"),
        filename=str(ALPHA_LIBRARY_PATH),
    )
    functions = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    required = {}
    errors = []
    for alpha_id in alpha_ids:
        function = functions.get(alpha_id)
        if function is None:
            errors.append(f"{alpha_id}: function is missing from alpha_lib.py")
            continue

        files = []
        for node in ast.walk(function):
            if not isinstance(node, ast.Call) or len(node.args) < 2:
                continue
            call = node.func
            if not (
                isinstance(call, ast.Attribute)
                and call.attr == "join"
                and isinstance(call.value, ast.Attribute)
                and call.value.attr == "path"
                and isinstance(call.value.value, ast.Name)
                and call.value.value.id == "os"
            ):
                continue
            folder_arg, filename_arg = node.args[:2]
            if not (
                isinstance(folder_arg, ast.Name)
                and folder_arg.id in PROVIDER_CONFIG
                and isinstance(filename_arg, ast.Constant)
                and isinstance(filename_arg.value, str)
                and filename_arg.value.endswith(".parquet")
            ):
                continue
            files.append((folder_arg.id, filename_arg.value))

        if not files:
            errors.append(f"{alpha_id}: no CQ/GN parquet inputs found in alpha_lib.py")
        required[alpha_id] = files
    return required, errors


def validate_alpha_data_topics():
    """Raise ValueError before data collection if an alpha topic is unavailable."""
    alpha_ids = list(getattr(config_data, "ACTIVE_ALPHAS", []))
    if not alpha_ids:
        raise ValueError("config_data.ACTIVE_ALPHAS must contain at least one alpha")
    if len(alpha_ids) != len(set(alpha_ids)):
        raise ValueError("config_data.ACTIVE_ALPHAS contains duplicate alpha IDs")

    configured_by_path = {}
    errors = []
    for path_variable, (provider, config_name) in PROVIDER_CONFIG.items():
        configured, invalid = _configured_topic_filenames(provider, config_name)
        configured_by_path[path_variable] = configured
        errors.extend(
            f"config_data.{config_name}: wrong provider for topic {topic!r}"
            for topic in invalid
        )

    required, alpha_errors = _required_alpha_files(alpha_ids)
    errors.extend(alpha_errors)

    for alpha_id, files in required.items():
        for path_variable, filename in files:
            if filename in configured_by_path[path_variable]:
                continue
            _, config_name = PROVIDER_CONFIG[path_variable]
            errors.append(
                f"{alpha_id}: required topic for {filename!r} is missing from "
                f"config_data.{config_name}"
            )

    if errors:
        details = "\n - ".join(errors)
        raise ValueError(
            "Data topic availability check failed; no data will be pulled:\n"
            f" - {details}"
        )

    return required


def read_existing_parquet(path):
    path = Path(path)
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    return prepare_timeseries(df)[0]


def snapshot_directory(folder_path, filename):
    return Path(folder_path) / "_snapshots" / Path(filename).stem


def snapshot_path(folder_path, filename, fetched_at, prefix="snapshot"):
    fetched_at = pd.Timestamp(fetched_at)
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.tz_localize("UTC")
    else:
        fetched_at = fetched_at.tz_convert("UTC")
    root = snapshot_directory(folder_path, filename)
    snapshot_name = f"{prefix}_{fetched_at.strftime('%Y%m%dT%H0000Z')}.parquet"
    return root / snapshot_name


def list_snapshot_files(folder_path, filename):
    root = snapshot_directory(folder_path, filename)
    if not root.exists():
        return []
    return sorted(root.rglob("*.parquet"))


def prune_snapshot_files(
    folder_path,
    filename,
    now=None,
    retention_days=SNAPSHOT_RETENTION_DAYS,
):
    root = snapshot_directory(folder_path, filename)
    if not root.exists():
        return 0

    now = pd.Timestamp.now(tz="UTC") if now is None else pd.Timestamp(now)
    if now.tzinfo is None:
        now = now.tz_localize("UTC")
    else:
        now = now.tz_convert("UTC")
    cutoff = now.timestamp() - (retention_days * 24 * 60 * 60)

    deleted = 0
    for path in root.rglob("*.parquet"):
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink()
                deleted += 1
        except FileNotFoundError:
            continue

    day_folders = sorted(
        (path for path in root.iterdir() if path.is_dir()),
        reverse=True,
    )
    for day_folder in day_folders:
        try:
            day_folder.rmdir()
        except OSError:
            pass
    return deleted


def revision_state_path(folder_path, filename):
    return (
        Path(folder_path)
        / "_revision_alert_state"
        / f"{Path(filename).stem}.json"
    )


def numeric_conversion(series):
    original_non_null = series.notna()
    converted = pd.to_numeric(series, errors="coerce")
    failures = int((original_non_null & converted.isna()).sum())
    return converted, failures


def normalize_cryptoquant_frame(df):
    frame = df.copy()
    if "datetime" not in frame.columns:
        raise ValueError("CryptoQuant response is missing datetime")
    frame["datetime"] = pd.to_datetime(frame["datetime"], utc=True, errors="coerce")

    failures = {}
    for column in frame.columns:
        if column == "datetime":
            continue
        frame[column], failures[column] = numeric_conversion(frame[column])
    return frame, failures


def normalize_mapping(value):
    if value is None:
        return None, 0
    try:
        if pd.isna(value):
            return None, 0
    except (TypeError, ValueError):
        pass

    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return None, 1
    if not isinstance(value, dict):
        return None, 1

    normalized = {}
    failures = 0
    for key, nested_value in value.items():
        if nested_value is None:
            normalized[str(key)] = None
            continue
        try:
            normalized[str(key)] = float(nested_value)
        except (TypeError, ValueError):
            normalized[str(key)] = None
            failures += 1
    return normalized, failures


def normalize_glassnode_frame(df):
    frame = df.copy()
    if "datetime" not in frame.columns:
        if "start_time" not in frame.columns:
            raise ValueError("Glassnode response is missing start_time/datetime")
        frame["datetime"] = pd.to_datetime(
            frame["start_time"],
            unit="ms",
            utc=True,
            errors="coerce",
        )
        frame = frame.drop(columns=["start_time"])
    else:
        frame["datetime"] = pd.to_datetime(
            frame["datetime"],
            utc=True,
            errors="coerce",
        )

    failures = {}
    for column in frame.columns:
        if column == "datetime":
            continue
        if column == "o":
            normalized_values = []
            failure_count = 0
            for value in frame[column]:
                normalized, value_failures = normalize_mapping(value)
                normalized_values.append(normalized)
                failure_count += value_failures
            frame[column] = normalized_values
            failures[column] = failure_count
        else:
            frame[column], failures[column] = numeric_conversion(frame[column])
    return frame, failures


def prepare_timeseries(df):
    frame = df.copy()
    if "datetime" not in frame.columns:
        raise ValueError("Dataset is missing datetime")
    # Parquet and provider responses may use different datetime resolutions
    # (us, ms, or ns). Standardize them so equal timestamps have equal labels.
    frame["datetime"] = pd.to_datetime(
        frame["datetime"], utc=True, errors="coerce"
    ).dt.as_unit("ns")
    invalid_datetime_rows = int(frame["datetime"].isna().sum())
    if invalid_datetime_rows:
        raise ValueError(
            f"Dataset contains {invalid_datetime_rows} invalid datetime rows"
        )

    frame = frame.sort_values("datetime")
    duplicate_rows = int(frame["datetime"].duplicated(keep="last").sum())
    if duplicate_rows:
        frame = frame.drop_duplicates("datetime", keep="last")
    frame = frame.reset_index(drop=True)
    return frame, duplicate_rows


def canonical_value(value):
    if isinstance(value, dict):
        normalized = {}
        for key, nested_value in value.items():
            if nested_value is None:
                continue
            try:
                if pd.isna(nested_value):
                    continue
            except (TypeError, ValueError):
                pass
            normalized[str(key)] = nested_value
        return json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    if isinstance(value, (list, tuple)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    if value is None:
        return "<null>"
    try:
        if pd.isna(value):
            return "<null>"
    except (TypeError, ValueError):
        pass
    return str(value)


def _short_value(value, limit=160):
    result = canonical_value(value)
    if len(result) <= limit:
        return result
    return f"{result[:limit - 3]}..."


def _record_revision_candidate(
    candidates,
    timestamp,
    column,
    old_value,
    new_value,
    numeric_difference=None,
    percentage_difference=None,
):
    timestamp_text = pd.Timestamp(timestamp).isoformat()
    key = f"{timestamp_text}|{column}"
    new_canonical = canonical_value(new_value)
    signature = hashlib.sha256(new_canonical.encode("utf-8")).hexdigest()
    candidate = candidates.get(key)
    if candidate is None:
        candidates[key] = {
            "key": key,
            "timestamp": timestamp_text,
            "column": str(column),
            "old_value": _short_value(old_value),
            "new_value": _short_value(new_value),
            "signature": signature,
            "max_numeric_difference": (
                float(numeric_difference)
                if numeric_difference is not None
                else None
            ),
            "max_percentage_difference": (
                float(percentage_difference)
                if percentage_difference is not None
                else None
            ),
        }
        return

    if numeric_difference is not None:
        previous = candidate.get("max_numeric_difference")
        if previous is None or numeric_difference > previous:
            candidate["max_numeric_difference"] = float(numeric_difference)
            candidate["old_value"] = _short_value(old_value)

    if percentage_difference is not None:
        previous = candidate.get("max_percentage_difference")
        if previous is None or percentage_difference > previous:
            candidate["max_percentage_difference"] = float(
                percentage_difference
            )


def _numeric_revision_metrics(
    old_value,
    new_value,
    absolute_tolerance,
    percentage_tolerance,
):
    old_value = float(old_value)
    new_value = float(new_value)

    if np.isnan(old_value) and np.isnan(new_value):
        return False, None, None
    if not np.isfinite(old_value) or not np.isfinite(new_value):
        return old_value != new_value, None, None

    difference = abs(new_value - old_value)
    if difference <= absolute_tolerance:
        return False, difference, 0.0

    if abs(old_value) <= absolute_tolerance:
        # Percentage change from a zero/near-zero baseline is undefined. Once
        # the absolute floor is exceeded, treat it as a material revision.
        return True, difference, np.inf

    percentage_difference = difference / abs(old_value) * 100.0
    return (
        percentage_difference > percentage_tolerance,
        difference,
        percentage_difference,
    )


def _mapping_revision_metrics(
    old_value,
    new_value,
    absolute_tolerance,
    percentage_tolerance,
):
    old_mapping, old_failures = normalize_mapping(old_value)
    new_mapping, new_failures = normalize_mapping(new_value)
    if old_failures or new_failures or old_mapping is None or new_mapping is None:
        return canonical_value(old_value) != canonical_value(new_value), None, None

    if set(old_mapping) != set(new_mapping):
        return True, None, None

    changed = False
    max_difference = None
    max_percentage_difference = None
    for key in old_mapping:
        old_nested = old_mapping[key]
        new_nested = new_mapping[key]
        if old_nested is None or new_nested is None:
            if old_nested != new_nested:
                return True, None, None
            continue

        nested_changed, difference, percentage_difference = (
            _numeric_revision_metrics(
                old_nested,
                new_nested,
                absolute_tolerance,
                percentage_tolerance,
            )
        )
        changed = changed or nested_changed
        if difference is not None and (
            max_difference is None or difference > max_difference
        ):
            max_difference = difference
        if percentage_difference is not None and (
            max_percentage_difference is None
            or percentage_difference > max_percentage_difference
        ):
            max_percentage_difference = percentage_difference

    return changed, max_difference, max_percentage_difference


def compare_with_historical_snapshots(
    new_df,
    snapshot_paths,
    existing_df=None,
    tolerance=DATA_REVISION_TOLERANCE,
    percentage_tolerance=DATA_REVISION_TOLERANCE_PERCENT,
):
    """Compare material revisions across overlapping retained data."""
    new_frame, _ = prepare_timeseries(new_df)
    new = new_frame.set_index("datetime")
    candidates = {}
    compared_snapshots = 0
    snapshots_with_changes = 0
    unreadable_snapshots = 0

    sources = []
    if existing_df is not None and not existing_df.empty:
        sources.append(("current_production", existing_df))
    sources.extend((str(path), path) for path in snapshot_paths)

    for source_name, source in sources:
        try:
            if isinstance(source, (str, Path)):
                old_frame = pd.read_parquet(source)
            else:
                old_frame = source
            old_frame, _ = prepare_timeseries(old_frame)
        except Exception:
            unreadable_snapshots += 1
            continue

        old = old_frame.set_index("datetime")
        common_index = old.index.intersection(new.index)
        common_columns = [
            column
            for column in old.columns.intersection(new.columns)
            if column not in REVISION_IGNORED_COLUMNS
        ]
        compared_snapshots += 1
        if common_index.empty or not common_columns:
            continue

        source_changed = False
        for column in common_columns:
            old_values = old.loc[common_index, column]
            new_values = new.loc[common_index, column]
            numeric = (
                pd.api.types.is_numeric_dtype(old_values)
                and pd.api.types.is_numeric_dtype(new_values)
            )

            if numeric:
                old_array = pd.to_numeric(
                    old_values,
                    errors="coerce",
                ).to_numpy(float)
                new_array = pd.to_numeric(
                    new_values,
                    errors="coerce",
                ).to_numpy(float)
                changed = ~np.isclose(
                    old_array,
                    new_array,
                    rtol=0,
                    atol=tolerance,
                    equal_nan=True,
                )
                finite_pairs = np.isfinite(old_array) & np.isfinite(new_array)
                absolute_differences = np.abs(new_array - old_array)
                percentage_differences = np.full(len(old_array), np.nan)
                regular_baselines = finite_pairs & (np.abs(old_array) > tolerance)
                percentage_differences[regular_baselines] = (
                    absolute_differences[regular_baselines]
                    / np.abs(old_array[regular_baselines])
                    * 100.0
                )
                near_zero_baselines = finite_pairs & ~regular_baselines
                percentage_differences[
                    near_zero_baselines & (absolute_differences > tolerance)
                ] = np.inf
                percentage_differences[
                    near_zero_baselines & (absolute_differences <= tolerance)
                ] = 0.0

                material = (
                    finite_pairs
                    & (absolute_differences > tolerance)
                    & (percentage_differences > percentage_tolerance)
                )
                non_finite_changes = changed & ~finite_pairs
                changed_positions = np.flatnonzero(material | non_finite_changes)
                for position in changed_positions:
                    old_value = old_array[position]
                    new_value = new_array[position]
                    numeric_difference = (
                        abs(old_value - new_value)
                        if np.isfinite(old_value) and np.isfinite(new_value)
                        else None
                    )
                    percentage_difference = (
                        percentage_differences[position]
                        if np.isfinite(percentage_differences[position])
                        or np.isinf(percentage_differences[position])
                        else None
                    )
                    _record_revision_candidate(
                        candidates,
                        common_index[position],
                        column,
                        old_value,
                        new_value,
                        numeric_difference,
                        percentage_difference,
                    )
                source_changed = source_changed or bool(len(changed_positions))
            else:
                # Compare positionally after selecting the shared, ordered
                # timestamps. Pandas otherwise requires identical index dtype
                # metadata (for example datetime64[us] vs datetime64[ms]) even
                # when the timestamp labels themselves represent the same time.
                old_canonical = old_values.map(canonical_value).to_numpy(
                    dtype=object
                )
                new_canonical = new_values.map(canonical_value).to_numpy(
                    dtype=object
                )
                changed_positions = []
                change_metrics = {}
                for position, (old_value, new_value) in enumerate(
                    zip(old_values, new_values)
                ):
                    if isinstance(old_value, dict) or isinstance(new_value, dict):
                        changed, difference, percentage_difference = (
                            _mapping_revision_metrics(
                                old_value,
                                new_value,
                                tolerance,
                                percentage_tolerance,
                            )
                        )
                    else:
                        changed = old_canonical[position] != new_canonical[position]
                        difference = None
                        percentage_difference = None
                    if changed:
                        changed_positions.append(position)
                        change_metrics[position] = (
                            difference,
                            percentage_difference,
                        )

                for position in changed_positions:
                    difference, percentage_difference = change_metrics[position]
                    _record_revision_candidate(
                        candidates,
                        common_index[position],
                        column,
                        old_values.iloc[position],
                        new_values.iloc[position],
                        difference,
                        percentage_difference,
                    )
                source_changed = source_changed or bool(len(changed_positions))

        if source_changed:
            snapshots_with_changes += 1

    changed_timestamps = sorted(
        {candidate["timestamp"] for candidate in candidates.values()}
    )
    numeric_differences = [
        candidate["max_numeric_difference"]
        for candidate in candidates.values()
        if candidate["max_numeric_difference"] is not None
    ]
    return HistoricalRevisionSummary(
        changed_rows=len(changed_timestamps),
        changed_cells=len(candidates),
        earliest_change=changed_timestamps[0] if changed_timestamps else None,
        latest_change=changed_timestamps[-1] if changed_timestamps else None,
        max_numeric_difference=(
            max(numeric_differences) if numeric_differences else None
        ),
        compared_snapshots=compared_snapshots,
        snapshots_with_changes=snapshots_with_changes,
        unreadable_snapshots=unreadable_snapshots,
        candidates=candidates,
    )


def prepare_revision_alert_state(summary, state_path):
    state = read_json(state_path, {"version": 1, "active": {}})
    previous = state.get("active", {})
    if not isinstance(previous, dict):
        previous = {}

    pending = [
        candidate
        for key, candidate in summary.candidates.items()
        if previous.get(key) != candidate["signature"]
    ]
    next_state = {
        "version": 1,
        "updated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "active": {
            key: candidate["signature"]
            for key, candidate in summary.candidates.items()
        },
    }
    return pending, next_state


def save_revision_alert_state(state_path, state):
    atomic_write_json(state_path, state)


def format_revision_alert(
    topic,
    summary,
    pending,
    tolerance,
    percentage_tolerance,
):
    numeric_differences = [
        candidate["max_numeric_difference"]
        for candidate in pending
        if candidate.get("max_numeric_difference") is not None
    ]
    percentage_differences = [
        candidate["max_percentage_difference"]
        for candidate in pending
        if candidate.get("max_percentage_difference") is not None
    ]
    max_data_difference = (
        max(numeric_differences) if numeric_differences else None
    )
    max_percentage_change = (
        max(percentage_differences) if percentage_differences else None
    )

    def format_metric(value, suffix=""):
        if value is None:
            return "n/a"
        if np.isinf(value):
            return "inf"
        return f"{float(value):.12g}{suffix}"

    samples = []
    for candidate in pending[:10]:
        difference = candidate.get("max_numeric_difference")
        difference_text = (
            f" diff={difference:.12g}" if difference is not None else ""
        )
        percentage_difference = candidate.get("max_percentage_difference")
        if percentage_difference is None:
            percentage_text = ""
        elif np.isinf(percentage_difference):
            percentage_text = " pct_change=inf"
        else:
            percentage_text = f" pct_change={percentage_difference:.6g}%"
        samples.append(
            f"{candidate['timestamp']} {candidate['column']} "
            f"old={candidate['old_value']} new={candidate['new_value']}"
            f"{difference_text}{percentage_text}"
        )
    sample_text = " | ".join(samples)
    return (
        f"DATA REVISION ALERT topic={topic} "
        f"new_revision_cells={len(pending)} "
        f"all_changed_rows={summary.changed_rows} "
        f"all_changed_cells={summary.changed_cells} "
        f"compared_snapshots={summary.compared_snapshots} "
        f"snapshots_with_changes={summary.snapshots_with_changes} "
        f"absolute_tolerance={tolerance} "
        f"percentage_tolerance={percentage_tolerance}% "
        f"max_data_diff={format_metric(max_data_difference)} "
        f"max_pct_chg={format_metric(max_percentage_change, '%')} "
        f"samples=[{sample_text}]"
    )


def validate_frame(df, now=None):
    if df.empty:
        raise ValueError("Validated dataset is empty")
    if "datetime" not in df.columns:
        raise ValueError("Validated dataset is missing datetime")
    if df["datetime"].isna().any():
        raise ValueError("Validated dataset contains invalid datetime")
    if df["datetime"].duplicated().any():
        raise ValueError("Validated dataset contains duplicate datetime rows")
    if not df["datetime"].is_monotonic_increasing:
        raise ValueError("Validated dataset is not ordered by datetime")

    data_columns = [column for column in df.columns if column != "datetime"]
    if not data_columns:
        raise ValueError("Validated dataset has no metric columns")
    if not any(df[column].notna().any() for column in data_columns):
        raise ValueError("All metric columns are empty")

    now = pd.Timestamp.now(tz="UTC") if now is None else pd.Timestamp(now)
    if now.tzinfo is None:
        now = now.tz_localize("UTC")
    else:
        now = now.tz_convert("UTC")
    latest = df["datetime"].max()
    if latest > now + pd.Timedelta(hours=2):
        raise ValueError(f"Latest datetime is unexpectedly in the future: {latest}")
    return latest


def validate_parquet_roundtrip(path, expected_df):
    reloaded = pd.read_parquet(path)
    reloaded, _ = prepare_timeseries(reloaded)
    validate_frame(reloaded)
    if len(reloaded) != len(expected_df):
        raise ValueError(
            f"Parquet row-count mismatch: expected={len(expected_df)}, "
            f"actual={len(reloaded)}"
        )
    if list(reloaded.columns) != list(expected_df.columns):
        raise ValueError("Parquet column order/schema changed during roundtrip")
    if reloaded["datetime"].max() != expected_df["datetime"].max():
        raise ValueError("Parquet latest datetime changed during roundtrip")


def atomic_write_parquet(df, file_path):
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = file_path.with_name(
        f".{file_path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    try:
        df.to_parquet(temp_path, index=False)
        validate_parquet_roundtrip(temp_path, df)

        last_error = None
        for attempt in range(ATOMIC_REPLACE_ATTEMPTS):
            try:
                os.replace(temp_path, file_path)
                return
            except PermissionError as exc:
                last_error = exc
                if attempt >= len(ATOMIC_REPLACE_DELAYS):
                    break
                time.sleep(ATOMIC_REPLACE_DELAYS[attempt])
        raise PermissionError(
            f"Unable to atomically replace {file_path} after "
            f"{ATOMIC_REPLACE_ATTEMPTS} attempts: {last_error}"
        )
    finally:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass
