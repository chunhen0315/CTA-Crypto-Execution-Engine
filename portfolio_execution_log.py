import csv
import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import config_data


FIELDNAMES = [
    "event_id",
    "generated_at_utc",
    "expected_signal_time_utc",
    "status",
    "mode",
    "execution_quality",
    "fresh_alpha_count",
    "total_alpha_count",
    "fresh_weight",
    "delayed_alphas_json",
    "delay_details_json",
    "continuous_delay_counts_json",
    "daily_delay_counts_json",
    "alerts_json",
    "alpha_signal_times_json",
    "alpha_signal_received_time_json",
    "alpha_latest_times_json",
    "alpha_positions_json",
    "alpha_signal_files_json",
    "net_signal",
    "target_position_btc",
    "actual_position_before_btc",
    "order_quantity_btc",
    "order_id",
    "actual_position_after_btc",
    "error",
]


def utc_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def utc_iso(value):
    parsed = utc_datetime(value)
    return parsed.isoformat() if parsed is not None else ""


def signal_check_window(now_utc, max_wait_minutes, hour_diff=1):
    now_utc = utc_datetime(now_utc)
    hour_start = now_utc.replace(minute=0, second=0, microsecond=0)
    expected_signal_time = hour_start - timedelta(hours=int(hour_diff))
    deadline = hour_start + timedelta(minutes=int(max_wait_minutes))
    return expected_signal_time, deadline


def read_receipt_state(path=None):
    path = path or config_data.ALPHA_SIGNAL_RECEIPT_STATE_FILE
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as file:
            state = json.load(file)
        return state if isinstance(state, dict) else {}
    except Exception:
        return {}


def write_receipt_state(state, path=None):
    path = path or config_data.ALPHA_SIGNAL_RECEIPT_STATE_FILE
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    temp_path = f"{path}.{uuid.uuid4().hex}.tmp"
    try:
        with open(temp_path, "w", encoding="utf-8") as file:
            json.dump(state, file, indent=2, sort_keys=True)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temp_path, path)
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass


def normalize_receipt_ledger(state):
    if state.get("version") == 2 and isinstance(state.get("signals"), dict):
        state.setdefault("latest_signal_time_by_alpha", {})
        return state

    migrated = {
        "version": 2,
        "signals": {},
        "latest_signal_time_by_alpha": {},
    }
    expected_signal_time = state.get("expected_signal_time_utc")
    old_alphas = state.get("alphas", {})
    if expected_signal_time and isinstance(old_alphas, dict):
        expected_receipts = {}
        for name, details in old_alphas.items():
            if not isinstance(details, dict):
                continue
            received_at = details.get("received_at_utc")
            signal_time = details.get("signal_time_utc") or expected_signal_time
            if received_at:
                expected_receipts[name] = received_at
            if signal_time:
                migrated["latest_signal_time_by_alpha"][name] = signal_time
        if expected_receipts:
            migrated["signals"][expected_signal_time] = expected_receipts
    return migrated


def update_alpha_signal_receipts(
    expected_signal_time,
    results,
    observed_at=None,
    path=None,
):
    """Persist first observations by signal timestamp and alpha."""
    expected_iso = utc_iso(expected_signal_time)
    if observed_at is None:
        observed_at = datetime.now(timezone.utc)
    observed_at = utc_datetime(observed_at)
    original_state = read_receipt_state(path)
    state = normalize_receipt_ledger(original_state)
    changed = state != original_state
    signals = state.setdefault("signals", {})
    latest_by_alpha = state.setdefault("latest_signal_time_by_alpha", {})

    for name, result in results.items():
        poll_observed_at = (
            result.get("poll_observed_time")
            or result.get("signal_received_time")
            or observed_at
        )
        poll_observed_at = utc_datetime(poll_observed_at)
        signal_time = result.get("signal_time")
        valid_expected_signal = (
            not result.get("delayed", True)
            and signal_time is not None
            and utc_iso(signal_time) == expected_iso
        )
        candidate_times = []
        if valid_expected_signal:
            candidate_times.append(utc_datetime(signal_time))

        if "latest_valid_signal_time" in result:
            latest_time = result.get("latest_valid_signal_time")
        else:
            latest_time = result.get("latest_time")
        if latest_time is not None:
            latest_time = utc_datetime(latest_time)
            previous_latest_raw = latest_by_alpha.get(name)
            previous_latest = (
                utc_datetime(previous_latest_raw)
                if previous_latest_raw
                else None
            )
            available_times = result.get("available_signal_times")
            if previous_latest is None:
                candidate_times.append(latest_time)
            elif latest_time > previous_latest:
                if available_times is None:
                    candidate_times.append(latest_time)
                else:
                    for available_time in available_times:
                        try:
                            parsed_time = utc_datetime(available_time)
                        except (TypeError, ValueError):
                            continue
                        if previous_latest < parsed_time <= latest_time:
                            candidate_times.append(parsed_time)

            if previous_latest is None or latest_time > previous_latest:
                latest_by_alpha[name] = utc_iso(latest_time)
                changed = True

        for candidate_time in candidate_times:
            signal_iso = utc_iso(candidate_time)
            alpha_receipts = signals.setdefault(signal_iso, {})
            if name not in alpha_receipts:
                alpha_receipts[name] = utc_iso(poll_observed_at)
                changed = True

        received_at_raw = signals.get(expected_iso, {}).get(name)
        result["signal_received_time"] = (
            utc_datetime(received_at_raw) if received_at_raw else None
        )

    retention_days = int(config_data.ALPHA_SIGNAL_RECEIPT_RETENTION_DAYS)
    retention_cutoff = observed_at - timedelta(days=retention_days)
    for signal_iso in list(signals):
        try:
            expired = utc_datetime(signal_iso) < retention_cutoff
        except (TypeError, ValueError):
            expired = True
        if expired:
            signals.pop(signal_iso, None)
            changed = True

    if changed:
        state["updated_at_utc"] = utc_iso(observed_at)
        write_receipt_state(state, path)

    return {
        name: result.get("signal_received_time")
        for name, result in results.items()
    }


def read_events(path=None):
    path = path or config_data.PORTFOLIO_EXECUTION_FILE
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", newline="", encoding="utf-8") as file:
            return list(csv.DictReader(file))
    except Exception:
        return []


def event_exists(expected_signal_time, events=None):
    expected_iso = utc_iso(expected_signal_time)
    events = read_events() if events is None else events
    return any(row.get("expected_signal_time_utc") == expected_iso for row in events)


def json_list(value):
    if isinstance(value, list):
        return value
    try:
        loaded = json.loads(value or "[]")
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return loaded if isinstance(loaded, list) else []


def delay_counts(events, expected_signal_time, delayed_alphas, timezone_name):
    expected_signal_time = utc_datetime(expected_signal_time)
    delayed_alphas = list(dict.fromkeys(delayed_alphas))
    delay_by_hour = {}
    daily_counts = {alpha_id: 0 for alpha_id in delayed_alphas}
    local_tz = ZoneInfo(timezone_name)
    current_local_date = datetime.now(timezone.utc).astimezone(local_tz).date()

    for row in events:
        row_time_raw = row.get("expected_signal_time_utc")
        if not row_time_raw:
            continue
        try:
            row_time = utc_datetime(row_time_raw)
        except (TypeError, ValueError):
            continue
        row_delayed = set(json_list(row.get("delayed_alphas_json")))
        delay_by_hour[row_time] = row_delayed

        generated_raw = row.get("generated_at_utc")
        try:
            generated = utc_datetime(generated_raw)
        except (TypeError, ValueError):
            generated = None
        if generated is not None and generated.astimezone(local_tz).date() == current_local_date:
            for alpha_id in delayed_alphas:
                if alpha_id in row_delayed:
                    daily_counts[alpha_id] += 1

    current_set = set(delayed_alphas)
    delay_by_hour[expected_signal_time] = current_set
    for alpha_id in delayed_alphas:
        daily_counts[alpha_id] += 1

    continuous_counts = {}
    for alpha_id in delayed_alphas:
        count = 0
        cursor = expected_signal_time
        while alpha_id in delay_by_hour.get(cursor, set()):
            count += 1
            cursor -= timedelta(hours=1)
        continuous_counts[alpha_id] = count

    return continuous_counts, daily_counts


def ensure_current_schema(path):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return

    with open(path, "r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        existing_fieldnames = reader.fieldnames or []
        if existing_fieldnames == FIELDNAMES:
            return
        rows = list(reader)

    temp_path = f"{path}.{uuid.uuid4().hex}.tmp"
    try:
        with open(temp_path, "w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=FIELDNAMES)
            writer.writeheader()
            for row in rows:
                writer.writerow({field: row.get(field, "") for field in FIELDNAMES})
            file.flush()
            os.fsync(file.fileno())
        os.replace(temp_path, path)
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass


def append_event(row, path=None):
    path = path or config_data.PORTFOLIO_EXECUTION_FILE
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    ensure_current_schema(path)
    output = {field: row.get(field, "") for field in FIELDNAMES}
    output["event_id"] = output["event_id"] or uuid.uuid4().hex
    output["generated_at_utc"] = output["generated_at_utc"] or datetime.now(timezone.utc).isoformat()

    write_header = not os.path.exists(path) or os.path.getsize(path) == 0
    with open(path, "a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDNAMES)
        if write_header:
            writer.writeheader()
        writer.writerow(output)
        file.flush()
        os.fsync(file.fileno())
    return output


def update_event(event_id, updates, path=None):
    """Atomically update an existing event without adding another CSV row."""
    path = path or config_data.PORTFOLIO_EXECUTION_FILE
    ensure_current_schema(path)
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    with open(path, "r", newline="", encoding="utf-8") as file:
        rows = list(csv.DictReader(file))

    updated_event = None
    for index, row in enumerate(rows):
        if row.get("event_id") != event_id:
            continue
        updated_event = {field: row.get(field, "") for field in FIELDNAMES}
        for field in FIELDNAMES:
            if field in updates:
                updated_event[field] = updates[field]
        updated_event["event_id"] = event_id
        rows[index] = updated_event
        break

    if updated_event is None:
        raise KeyError(f"Execution event not found: {event_id}")

    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    temp_path = f"{path}.{uuid.uuid4().hex}.tmp"
    try:
        with open(temp_path, "w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(rows)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temp_path, path)
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass

    return updated_event
