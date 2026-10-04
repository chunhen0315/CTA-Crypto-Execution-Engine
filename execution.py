import asyncio
import bybit_ccxt
import alpha_signal
import config_data
import config_exe
import json
import math
import os
import paper_broker
from datetime import datetime, timedelta, timezone
from pandas import Timestamp

import portfolio_execution_log
from logger import setup_logger

SCRIPT_NAME = os.path.basename(__file__).replace(".py", "")
logger = setup_logger(f"{SCRIPT_NAME}.log")

POSITION_TOLERANCE = 1e-12


def json_text(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def timestamp_text(value):
    if value is None:
        return ""
    return pd_timestamp(value).isoformat()


def pd_timestamp(value):
    timestamp = Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def all_alpha_receipts_by_deadline(aligned_result, deadline):
    results = aligned_result["results"]
    if not results:
        return False
    for result in results.values():
        received_at = result.get("signal_received_time")
        if received_at is None or pd_timestamp(received_at) > pd_timestamp(deadline):
            return False
    return True


def signal_decision_phase(check_started_at, deadline, aligned_result):
    check_started_at = pd_timestamp(check_started_at)
    deadline = pd_timestamp(deadline)
    tolerance = timedelta(
        seconds=int(config_exe.SIGNAL_DEADLINE_TOLERANCE_SECONDS)
    )
    execute_early = bool(config_exe.EXECUTE_EARLY_WHEN_FULLY_ALIGNED)
    if aligned_result["aligned"] and execute_early and (
        check_started_at <= deadline + tolerance
        or all_alpha_receipts_by_deadline(aligned_result, deadline)
    ):
        return "EXECUTE"
    if check_started_at >= deadline:
        fresh_weight = float(aligned_result.get("fresh_weight", 0.0))
        minimum_fresh_weight = float(config_exe.MIN_FRESH_WEIGHT)
        if not 0.0 <= minimum_fresh_weight <= 1.0:
            raise ValueError(
                f"MIN_FRESH_WEIGHT must be between 0 and 1, got "
                f"{minimum_fresh_weight}"
            )
        if fresh_weight >= minimum_fresh_weight:
            return "EXECUTE_DEGRADED"
        return "EXPIRED"
    return "WAIT"


def prepare_degraded_execution(aligned_result):
    """Build a degraded target from the currently fresh alpha positions."""
    degraded_result = dict(aligned_result)
    net_signal = float(aligned_result["available_net_signal"])
    fresh_weight = float(aligned_result.get("fresh_weight", 0.0))
    if bool(config_exe.RENORMALIZE_FRESH_SIGNALS) and fresh_weight > 0:
        net_signal /= fresh_weight
    degraded_result["net_signal"] = net_signal
    degraded_result["degraded_execution"] = True
    return degraded_result


def seconds_until_next_signal_poll(now_utc):
    now_utc = portfolio_execution_log.utc_datetime(now_utc)
    interval = max(1, int(config_exe.MAIN_REFRESH_INTERVAL))
    now_epoch = now_utc.timestamp()
    next_boundary_epoch = ((int(now_epoch) // interval) + 1) * interval
    next_poll = datetime.fromtimestamp(next_boundary_epoch, tz=timezone.utc)
    _, deadline = portfolio_execution_log.signal_check_window(
        now_utc,
        config_exe.SIGNAL_MAX_WAIT_MINUTES,
        config_data.CQ_HOUR_DIFF,
    )
    if now_utc < deadline < next_poll:
        next_poll = deadline
    return max(0.1, (next_poll - now_utc).total_seconds())


async def fetch_actual_position(exchange):
    if config_exe.MODE == "paper":
        return paper_broker.fetch_position_size()
    return await bybit_ccxt.fetch_position_size(exchange)


def decision_fields(aligned_result, expected_signal_time):
    results = aligned_result["results"]
    return {
        "expected_signal_time_utc": timestamp_text(expected_signal_time),
        "mode": config_exe.MODE,
        "execution_quality": (
            "DEGRADED" if aligned_result.get("degraded_execution") else "FULL"
        ),
        "fresh_alpha_count": aligned_result.get("fresh_alpha_count", ""),
        "total_alpha_count": aligned_result.get("total_alpha_count", ""),
        "fresh_weight": aligned_result.get("fresh_weight", ""),
        "delayed_alphas_json": json_text(aligned_result["delayed_alphas"]),
        "delay_details_json": json_text(
            {
                name: result["reason"]
                for name, result in results.items()
                if result["delayed"]
            }
        ),
        "alpha_signal_times_json": json_text(
            {
                name: timestamp_text(result["signal_time"])
                for name, result in results.items()
            }
        ),
        "alpha_signal_received_time_json": json_text(
            {
                name: timestamp_text(result.get("signal_received_time"))
                for name, result in results.items()
            }
        ),
        "alpha_latest_times_json": json_text(
            {
                name: timestamp_text(result["latest_time"])
                for name, result in results.items()
            }
        ),
        "alpha_positions_json": json_text(aligned_result["positions"]),
        # "alpha_signal_files_json": json_text(
        #     {
        #         name: result["signal_file"]
        #         for name, result in results.items()
        #     }
        # ),
        "net_signal": "" if aligned_result["net_signal"] is None else aligned_result["net_signal"],
    }


def delay_alerts(continuous_counts, daily_counts):
    alerts = []
    continuous_threshold = int(config_exe.CONTINUOUS_DELAY_ALERT_HOURS)
    daily_threshold = int(config_exe.DAILY_DELAY_ALERT_COUNT)

    for alpha_id, count in continuous_counts.items():
        if count == continuous_threshold:
            message = (
                "ALPHA CONTINUOUS DATA DELAY ALERT | "
                f"alpha={alpha_id} delayed for {count} consecutive hourly checks"
            )
            alerts.append(message)

    for alpha_id, count in daily_counts.items():
        if count == daily_threshold + 1:
            message = (
                "ALPHA DAILY DATA DELAY ALERT | "
                f"alpha={alpha_id} accumulated {count} delayed hourly checks today "
                f"({config_exe.DELAY_ALERT_TIMEZONE})"
            )
            alerts.append(message)

    return alerts


async def record_delayed_decision(exchange, aligned_result, expected_signal_time, events):
    continuous_counts, daily_counts = portfolio_execution_log.delay_counts(
        events,
        expected_signal_time,
        aligned_result["delayed_alphas"],
        config_exe.DELAY_ALERT_TIMEZONE,
    )
    alerts = delay_alerts(continuous_counts, daily_counts)
    try:
        actual_position = await fetch_actual_position(exchange)
    except Exception as exc:
        logger.warning(f"Unable to read position while signals are delayed: {exc}")
        actual_position = ""

    row = {
        **decision_fields(aligned_result, expected_signal_time),
        "status": "SIGNAL_DEADLINE_EXPIRED",
        "continuous_delay_counts_json": json_text(continuous_counts),
        "daily_delay_counts_json": json_text(daily_counts),
        "alerts_json": json_text(alerts),
        "actual_position_before_btc": actual_position,
        "actual_position_after_btc": actual_position,
    }
    portfolio_execution_log.append_event(row)
    logger.warning(
        f"Hourly trade skipped for {timestamp_text(expected_signal_time)}; "
        f"waiting for aligned alphas={aligned_result['delayed_alphas']}"
    )
    for message in alerts:
        logger.error(message)


async def record_late_aligned_decision(
    exchange,
    aligned_result,
    expected_signal_time,
    deadline,
):
    try:
        actual_position = await fetch_actual_position(exchange)
    except Exception as exc:
        logger.warning(f"Unable to read position for late aligned signal: {exc}")
        actual_position = ""

    message = (
        "All alpha signals aligned after the maximum wait deadline; "
        f"deadline={timestamp_text(deadline)}"
    )
    portfolio_execution_log.append_event(
        {
            **decision_fields(aligned_result, expected_signal_time),
            "status": "MISSED_SIGNAL_DEADLINE",
            "actual_position_before_btc": actual_position,
            "actual_position_after_btc": actual_position,
            "error": message,
        }
    )
    logger.warning(
        f"Hourly trade skipped for {timestamp_text(expected_signal_time)}: {message}"
    )


async def execute_aligned_decision(
    exchange,
    aligned_result,
    expected_signal_time,
    record_event=True,
):
    base_row = decision_fields(aligned_result, expected_signal_time)
    degraded = bool(aligned_result.get("degraded_execution"))
    try:
        target_position = float(aligned_result["net_signal"]) * float(config_exe.single_pos)
    except (TypeError, ValueError) as exc:
        if record_event:
            portfolio_execution_log.append_event(
                {
                    **base_row,
                    "status": "INVALID_TARGET",
                    "error": str(exc),
                }
            )
        logger.error(f"Invalid aligned portfolio target: {exc}")
        return None

    if not math.isfinite(target_position):
        message = f"Non-finite aligned portfolio target: {target_position!r}"
        if record_event:
            portfolio_execution_log.append_event(
                {**base_row, "status": "INVALID_TARGET", "error": message}
            )
        logger.error(message)
        return None

    try:
        position_before = await fetch_actual_position(exchange)
        if position_before is None or not math.isfinite(float(position_before)):
            raise ValueError(f"invalid position value {position_before!r}")
        position_before = float(position_before)
    except Exception as exc:
        if record_event:
            portfolio_execution_log.append_event(
                {
                    **base_row,
                    "status": "POSITION_READ_FAILED",
                    "target_position_btc": target_position,
                    "error": str(exc),
                }
            )
        logger.error(f"Unable to fetch actual position before aligned trade: {exc}")
        return None

    order_quantity = target_position - position_before
    if abs(order_quantity) <= POSITION_TOLERANCE:
        if record_event:
            portfolio_execution_log.append_event(
                {
                    **base_row,
                    "status": (
                        "DEGRADED_NO_POSITION_CHANGE"
                        if degraded
                        else "NO_POSITION_CHANGE"
                    ),
                    "target_position_btc": target_position,
                    "actual_position_before_btc": position_before,
                    "order_quantity_btc": 0.0,
                    "actual_position_after_btc": position_before,
                }
            )
        logger.info(
            f"Aligned portfolio already at target for {timestamp_text(expected_signal_time)}: "
            f"position={position_before}"
        )
        return position_before

    event_id = ""
    if record_event:
        pending_row = portfolio_execution_log.append_event(
            {
                **base_row,
                "status": "DEGRADED_ORDER_PENDING" if degraded else "ORDER_PENDING",
                "target_position_btc": target_position,
                "actual_position_before_btc": position_before,
                "order_quantity_btc": order_quantity,
            }
        )
        event_id = pending_row["event_id"]
    execution_result = None
    execution_error = ""
    try:
        if config_exe.MODE == "paper":
            execution_result = await paper_broker.execute_market(target_position)
        else:
            execution_result = await bybit_ccxt.execute_market(exchange, target_position)
    except Exception as exc:
        execution_error = str(exc)
        logger.exception(f"Error executing aligned order: {exc}")

    order_id = ""
    if isinstance(execution_result, dict):
        order_id = execution_result.get("id", "")

    try:
        position_after = await fetch_actual_position(exchange)
        if position_after is None:
            raise ValueError("position fetch returned None")
        position_after = float(position_after)
    except Exception as exc:
        position_after = ""
        execution_error = execution_error or str(exc)

    matched = (
        position_after != ""
        and abs(float(position_after) - target_position) <= POSITION_TOLERANCE
    )
    if degraded:
        status = "DEGRADED_EXECUTION" if matched else "DEGRADED_POSITION_MISMATCH"
    else:
        status = "EXECUTED" if matched else "POSITION_MISMATCH"
    if record_event:
        portfolio_execution_log.update_event(
            event_id,
            {
                **base_row,
                "status": status,
                "target_position_btc": target_position,
                "actual_position_before_btc": position_before,
                "order_quantity_btc": order_quantity,
                "order_id": order_id,
                "actual_position_after_btc": position_after,
                "error": execution_error,
            },
        )

    if matched:
        logger.info(
            f"{'Degraded' if degraded else 'Aligned'} portfolio trade completed for "
            f"{timestamp_text(expected_signal_time)}: "
            f"target={target_position}, position_after={position_after}"
        )
        return float(position_after)

    logger.error(
        f"Aligned portfolio position mismatch for {timestamp_text(expected_signal_time)}: "
        f"target={target_position}, position_after={position_after}, error={execution_error}"
    )
    return None

# ========================= ON_TICK LOOP =========================
async def on_tick(exchange, last_pos_size=None, now_utc=None):
    if config_exe.MODE != "paper":
        try:
            kill_switch_activated = await bybit_ccxt.check_kill_switch(exchange)
        except Exception as e:
            logger.error(f"Error checking kill switch: {e}")
            return False, last_pos_size

        if kill_switch_activated:
            logger.warning("Kill switch activated. Stopping bot.")
            return False, None

    check_started_at = portfolio_execution_log.utc_datetime(
        now_utc if now_utc is not None else datetime.now(timezone.utc)
    )
    expected_signal_time, deadline = portfolio_execution_log.signal_check_window(
        check_started_at,
        config_exe.SIGNAL_MAX_WAIT_MINUTES,
        config_data.CQ_HOUR_DIFF,
    )

    events = portfolio_execution_log.read_events()
    hour_event_exists = portfolio_execution_log.event_exists(
        expected_signal_time,
        events,
    )
    if hour_event_exists:
        logger.info(
            f"Hourly decision already recorded for {timestamp_text(expected_signal_time)}; "
            "late alpha recovery will be used from the next signal hour"
        )
        return True, last_pos_size

    try:
        aligned_result = alpha_signal.calculate_aligned_signals(expected_signal_time)
    except Exception as exc:
        logger.exception(f"Signal calculation failed for hourly check: {exc}")
        if not hour_event_exists and check_started_at >= deadline:
            portfolio_execution_log.append_event(
                {
                    "expected_signal_time_utc": timestamp_text(expected_signal_time),
                    "status": "SIGNAL_CALCULATION_FAILED",
                    "mode": config_exe.MODE,
                    "error": str(exc),
                }
            )
        return True, last_pos_size

    try:
        receipt_times = portfolio_execution_log.update_alpha_signal_receipts(
            expected_signal_time,
            aligned_result["results"],
            observed_at=check_started_at,
        )
        logger.info(
            f"Alpha signal receipt progress for {timestamp_text(expected_signal_time)}: "
            f"{json_text({name: timestamp_text(value) for name, value in receipt_times.items()})}"
        )
    except Exception as exc:
        logger.exception(f"Unable to persist alpha signal receipt times: {exc}")

    phase = signal_decision_phase(check_started_at, deadline, aligned_result)
    if phase == "WAIT":
        logger.info(
            f"Waiting for alpha alignment for {timestamp_text(expected_signal_time)}; "
            f"deadline={timestamp_text(deadline)}; "
            f"delayed_alphas={aligned_result['delayed_alphas']}"
        )
        return True, last_pos_size

    if phase == "EXPIRED":
        if aligned_result["aligned"]:
            await record_late_aligned_decision(
                exchange,
                aligned_result,
                expected_signal_time,
                deadline,
            )
        else:
            await record_delayed_decision(
                exchange,
                aligned_result,
                expected_signal_time,
                events,
            )
        return True, last_pos_size

    if phase == "EXECUTE_DEGRADED":
        aligned_result = prepare_degraded_execution(aligned_result)
        logger.warning(
            f"Executing degraded portfolio for {timestamp_text(expected_signal_time)}: "
            f"fresh_weight={aligned_result['fresh_weight']:.2%}, "
            f"fresh={aligned_result['fresh_alpha_count']}/"
            f"{aligned_result['total_alpha_count']}, "
            f"omitted={aligned_result['delayed_alphas']}, "
            f"renormalized={bool(config_exe.RENORMALIZE_FRESH_SIGNALS)}"
        )

    try:
        position_after = await execute_aligned_decision(
            exchange,
            aligned_result,
            expected_signal_time,
            record_event=not hour_event_exists,
        )
        if position_after is not None:
            last_pos_size = position_after
    except Exception as exc:
        logger.exception(f"Unexpected aligned execution failure: {exc}")
        if not portfolio_execution_log.event_exists(expected_signal_time):
            portfolio_execution_log.append_event(
                {
                    **decision_fields(aligned_result, expected_signal_time),
                    "status": "ORDER_FAILED",
                    "error": str(exc),
                }
            )

    return True, last_pos_size

# ========================= MAIN LOOP =========================
async def main():
    exchange = None
    last_pos_size = None

    try:
        logger.info(f"Starting execution in {config_exe.MODE} mode")

        if config_exe.MODE == "paper":
            await paper_broker.initialize_paper_account()
        else:
            logger.info("Initializing exchange...")
            exchange = await bybit_ccxt.initialize_exchange()
            if exchange:
                logger.info("Exchange initialized successfully.")
            else:
                logger.error("Failed to initialize exchange.")
                return

        logger.info("Starting on_tick loop...")
        while True:
            continue_loop, last_pos_size = await on_tick(exchange, last_pos_size)
            if not continue_loop:
                break
            await asyncio.sleep(
                seconds_until_next_signal_poll(datetime.now(timezone.utc))
            )

    except KeyboardInterrupt:
        logger.warning("Bot stopped by user.")
    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        if exchange:
            await exchange.close()
            logger.info("Exchange connection closed.")

# ========================= ENTRY POINT =========================
if __name__ == "__main__":
    asyncio.run(main())
