import math
import os
import warnings

import alpha_lib
import config_data
import pandas as pd
from logger import setup_logger

warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=pd.errors.SettingWithCopyWarning)


output_dir = config_data.ALPHA_SIGNAL_PATH
logger = setup_logger("alpha_signal.log")

ALPHA_FUNCTIONS = {
    alpha_id: getattr(alpha_lib, alpha_id)
    for alpha_id in config_data.ACTIVE_ALPHAS
}


def get_alpha_signal(name, func):
    try:
        df_result = func()
        signal = float(df_result['pos'].iloc[-1])
    except Exception as e:
        logger.exception(f"{name} signal calculation failed; defaulting this alpha to 0: {e}")
        return 0

    if not math.isfinite(signal):
        logger.warning(f"{name} produced non-finite signal {signal!r}; defaulting this alpha to 0")
        return 0

    return signal


def as_utc_timestamp(value):
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def save_alpha_signal_csv(name, df_result):
    os.makedirs(output_dir, exist_ok=True)
    output_df = alpha_lib.slice_alpha_output(df_result)
    csv_filename = os.path.join(output_dir, f"{name}_signal.csv")
    alpha_lib.atomic_write_csv(output_df, csv_filename, index=True)
    return csv_filename


def get_alpha_result(name, func, expected_signal_time):
    expected_signal_time = as_utc_timestamp(expected_signal_time)
    result = {
        "alpha_id": name,
        "expected_signal_time": expected_signal_time,
        "signal_time": None,
        "signal_received_time": None,
        "poll_observed_time": None,
        "available_signal_times": None,
        "latest_time": None,
        "latest_valid_signal_time": None,
        "signal": None,
        "position": None,
        "signal_file": os.path.join(output_dir, f"{name}_signal.csv"),
        "delayed": True,
        "reason": "",
    }

    try:
        df_result = func()
        if df_result is None or df_result.empty:
            raise ValueError("alpha returned no rows")

        df_result = df_result.copy()
        df_result.index = pd.to_datetime(df_result.index, utc=True)
        df_result = df_result.sort_index()
        result["signal_file"] = save_alpha_signal_csv(name, df_result)
        result["latest_time"] = as_utc_timestamp(df_result.index.max())
        result["poll_observed_time"] = pd.Timestamp.now(tz="UTC")

        signal_values = pd.to_numeric(df_result["signal"], errors="coerce")
        position_values = pd.to_numeric(df_result["pos"], errors="coerce")
        valid_signal_rows = signal_values.map(math.isfinite) & position_values.map(
            math.isfinite
        )
        result["available_signal_times"] = df_result.index[valid_signal_rows]
        if valid_signal_rows.any():
            result["latest_valid_signal_time"] = as_utc_timestamp(
                df_result.index[valid_signal_rows].max()
            )

        matching = df_result.loc[df_result.index == expected_signal_time]
        if matching.empty:
            result["reason"] = (
                f"expected {expected_signal_time.isoformat()} missing; "
                f"latest={result['latest_time'].isoformat()}"
            )
            return result

        row = matching.iloc[-1]
        signal = float(row["signal"])
        position = float(row["pos"])
        if not math.isfinite(signal) or not math.isfinite(position):
            raise ValueError(f"non-finite signal/position: signal={signal}, position={position}")

        result.update(
            {
                "signal_time": expected_signal_time,
                "signal_received_time": result["poll_observed_time"],
                "signal": signal,
                "position": position,
                "delayed": False,
                "reason": "",
            }
        )
    except Exception as exc:
        result["reason"] = str(exc)
        logger.exception(
            f"{name} aligned signal calculation failed for "
            f"{expected_signal_time.isoformat()}: {exc}"
        )

    return result


def calculate_aligned_signals(expected_signal_time):
    expected_signal_time = as_utc_timestamp(expected_signal_time)
    results = {
        name: get_alpha_result(name, func, expected_signal_time)
        for name, func in ALPHA_FUNCTIONS.items()
    }
    delayed_alphas = [
        name for name, result in results.items() if result["delayed"]
    ]
    aligned = not delayed_alphas
    positions = {
        name: result["position"]
        for name, result in results.items()
        if not result["delayed"]
    }
    total_alpha_count = len(results)
    fresh_alpha_count = len(positions)
    fresh_weight = (
        fresh_alpha_count / total_alpha_count
        if total_alpha_count
        else 0.0
    )
    available_net_signal = sum(positions.values())
    net_signal = available_net_signal if aligned else None

    if aligned:
        logger.info(
            f"Aligned net signal for {expected_signal_time.isoformat()}: "
            f"net={net_signal}; alpha positions={positions}"
        )
    else:
        logger.warning(
            f"Signal alignment delayed for {expected_signal_time.isoformat()}: "
            f"delayed_alphas={delayed_alphas}"
        )

    return {
        "expected_signal_time": expected_signal_time,
        "aligned": aligned,
        "delayed_alphas": delayed_alphas,
        "results": results,
        "positions": positions,
        "total_alpha_count": total_alpha_count,
        "fresh_alpha_count": fresh_alpha_count,
        "fresh_weight": fresh_weight,
        "available_net_signal": available_net_signal,
        "degraded_execution": False,
        "net_signal": net_signal,
    }


def main():
    signals = {}
    net_signal = 0

    for name, func in ALPHA_FUNCTIONS.items():
        signal = get_alpha_signal(name, func)
        signals[name] = signal
        net_signal += signal

    logger.info(f"Net signal: {net_signal}; alpha signals: {signals}")
    return net_signal


def save_all_alpha_signals():
    os.makedirs(output_dir, exist_ok=True)

    for name, func in ALPHA_FUNCTIONS.items():
        try:
            df_result = func()
            df_result = alpha_lib.slice_alpha_output(df_result)
            csv_filename = os.path.join(output_dir, f"{name}_signal.csv")
            df_result.to_csv(csv_filename)
            print(f"Saved {name} results to {csv_filename}")
        except Exception as e:
            logger.exception(f"Error processing {name}: {e}")
            print(f"Error processing {name}: {e}")


if __name__ == "__main__":
    main()
    save_all_alpha_signals()
