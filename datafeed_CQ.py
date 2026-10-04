"""Live CryptoQuant BTC 1h data feed."""

import asyncio
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import cybotrade_datasource
import pandas as pd
from dotenv import load_dotenv

import config_data
from datafeed_utils import (
    FetchResult,
    atomic_write_parquet,
    compare_with_historical_snapshots,
    format_revision_alert,
    list_snapshot_files,
    normalize_cryptoquant_frame,
    prepare_revision_alert_state,
    prepare_timeseries,
    prune_snapshot_files,
    read_existing_parquet,
    revision_state_path,
    save_revision_alert_state,
    snapshot_path,
    topic_filename,
    validate_alpha_data_topics,
    validate_frame,
)
from logger import setup_logger


REFRESH_INTERVAL = config_data.REFRESH_INTERVAL
MAX_TIME_DIFFERENCE = config_data.MAX_TIME_DIFFERENCE
DELAY_COLLECT = config_data.CQ_DELAY_COLLECT
API_KEY = config_data.API_KEY

FETCH_ATTEMPTS_PER_POLL = 1
FAILED_FETCH_ALERT_THRESHOLD = 3
QUERY_TIMEOUT_SECONDS = 300
MAX_CONCURRENT_FETCHES = 3
DELAYED_RETRY_MINUTES = config_data.DELAYED_DATA_RETRY_MINUTES
RETRY_DEADLINE_MINUTE = config_data.DATA_RETRY_DEADLINE_MINUTE
SNAPSHOT_RETENTION_DAYS = config_data.DATA_SNAPSHOT_RETENTION_DAYS
DATA_REVISION_TOLERANCE = config_data.DATA_REVISION_TOLERANCE
DATA_REVISION_TOLERANCE_PERCENT = config_data.DATA_REVISION_TOLERANCE_PERCENT

SCRIPT_NAME = os.path.basename(__file__).replace(".py", "")
logger = setup_logger(f"{SCRIPT_NAME}.log")

load_dotenv()
pd.set_option("display.max_columns", None)


def _positive_counts(counts):
    return {column: count for column, count in counts.items() if count}


async def fetch_and_save_data(
    topic,
    folder_path,
    logger,
    semaphore,
):
    """Fetch a fixed DATA_LEN window and replace, rather than merge, live data."""
    topic_name = topic["topic"]
    filename = topic_filename(
        topic_name,
        config_data.ASSET,
        config_data.INTERVAL,
    )
    file_path = Path(folder_path) / filename

    try:
        existing_df = read_existing_parquet(file_path)
    except Exception as exc:
        logger.warning(
            "Existing production parquet is unreadable for %s. It will not be "
            "used for revision comparison: %s",
            topic_name,
            exc,
        )
        existing_df = None

    last_error = None
    rows_received = 0
    conversion_failure_total = 0
    # One provider request per poll. The outer recovery loop owns the
    # five-minute cadence and the three-failure Telegram threshold.
    max_attempts = FETCH_ATTEMPTS_PER_POLL
    for attempt in range(1, max_attempts + 1):
        try:
            async with semaphore:
                query_end = datetime.now(timezone.utc)
                query_start = query_end - timedelta(hours=config_data.DATA_LEN)
                logger.info(
                    "Querying topic=%s mode=fixed_window data_len_hours=%s "
                    "start=%s end=%s attempt=%s/%s",
                    topic_name,
                    config_data.DATA_LEN,
                    query_start.isoformat(),
                    query_end.isoformat(),
                    attempt,
                    max_attempts,
                )
                data = await asyncio.wait_for(
                    cybotrade_datasource.query_paginated(
                        api_key=API_KEY,
                        topic=topic_name,
                        start_time=query_start,
                        end_time=query_end,
                    ),
                    timeout=QUERY_TIMEOUT_SECONDS,
                )
                if not data:
                    raise ValueError("Provider returned no data")

                rows_received = len(data)
                new_df, conversion_failures = normalize_cryptoquant_frame(
                    pd.DataFrame(data)
                )
                conversion_failure_counts = _positive_counts(conversion_failures)
                conversion_failure_total = sum(conversion_failure_counts.values())
                if conversion_failure_counts:
                    logger.warning(
                        "Numeric conversion failures for %s: total=%s by_column=%s",
                        topic_name,
                        conversion_failure_total,
                        conversion_failure_counts,
                    )

                new_df, duplicate_rows = prepare_timeseries(new_df)
                if duplicate_rows:
                    logger.warning(
                        "Removed %s duplicate datetime rows for %s",
                        duplicate_rows,
                        topic_name,
                    )

                latest_time = validate_frame(new_df, now=query_end)
                actual_age = pd.Timestamp.now(tz="UTC") - latest_time
                actual_age_minutes = actual_age.total_seconds() / 60
                maximum_age_minutes = max(
                    MAX_TIME_DIFFERENCE,
                    config_data.CQ_HOUR_DIFF * 60 + RETRY_DEADLINE_MINUTE,
                )
                if actual_age > pd.Timedelta(minutes=maximum_age_minutes):
                    raise ValueError(
                        "Latest provider data is stale: "
                        f"latest={latest_time.isoformat()}, "
                        f"actual_age_minutes={actual_age_minutes:.2f}, "
                        f"maximum={maximum_age_minutes}"
                    )

                deleted_snapshots = prune_snapshot_files(
                    folder_path,
                    filename,
                    now=query_end,
                    retention_days=SNAPSHOT_RETENTION_DAYS,
                )
                historical_paths = list_snapshot_files(folder_path, filename)

                if existing_df is not None and not historical_paths:
                    legacy_path = snapshot_path(
                        folder_path,
                        filename,
                        query_end,
                        prefix="legacy",
                    )
                    atomic_write_parquet(existing_df, legacy_path)
                    historical_paths.append(legacy_path)

                revisions = compare_with_historical_snapshots(
                    new_df,
                    historical_paths,
                    existing_df=existing_df,
                    tolerance=DATA_REVISION_TOLERANCE,
                    percentage_tolerance=DATA_REVISION_TOLERANCE_PERCENT,
                )
                alert_state_path = revision_state_path(folder_path, filename)
                pending_alerts, next_alert_state = prepare_revision_alert_state(
                    revisions,
                    alert_state_path,
                )

                archive_path = snapshot_path(
                    folder_path,
                    filename,
                    query_end,
                )
                atomic_write_parquet(new_df, archive_path)
                atomic_write_parquet(new_df, file_path)

            try:
                save_revision_alert_state(alert_state_path, next_alert_state)
            except Exception as state_exc:
                logger.warning(
                    "Live data was saved but revision alert state could not be "
                    "updated for %s: %s",
                    topic_name,
                    state_exc,
                )

            if revisions.unreadable_snapshots:
                logger.warning(
                    "Skipped %s unreadable historical snapshots for %s",
                    revisions.unreadable_snapshots,
                    topic_name,
                )
            if pending_alerts:
                logger.warning(
                    format_revision_alert(
                        topic_name,
                        revisions,
                        pending_alerts,
                        DATA_REVISION_TOLERANCE,
                        DATA_REVISION_TOLERANCE_PERCENT,
                    )
                )
            elif revisions.changed_rows:
                logger.info(
                    "Known historical revisions remain for %s: rows=%s cells=%s "
                    "compared_snapshots=%s; duplicate Telegram alert suppressed",
                    topic_name,
                    revisions.changed_rows,
                    revisions.changed_cells,
                    revisions.compared_snapshots,
                )

            logger.info(
                "Refresh succeeded for %s: mode=fixed_window received=%s saved=%s "
                "latest=%s actual_age_minutes=%.2f archive=%s "
                "deleted_expired_snapshots=%s",
                topic_name,
                rows_received,
                len(new_df),
                latest_time.isoformat(),
                actual_age_minutes,
                archive_path,
                deleted_snapshots,
            )
            return FetchResult(
                success=True,
                topic=topic_name,
                mode="fixed_window",
                attempts=attempt,
                rows_received=rows_received,
                rows_saved=len(new_df),
                latest_time=latest_time.isoformat(),
                actual_age_minutes=round(actual_age_minutes, 3),
                conversion_failures=conversion_failure_total,
                revision_rows=revisions.changed_rows,
            )
        except Exception as exc:
            last_error = exc
            logger.warning(
                "Delayed-data recovery attempt failed for %s: %s",
                topic_name,
                exc,
            )

    return FetchResult(
        success=False,
        topic=topic_name,
        mode="fixed_window",
        attempts=max_attempts,
        rows_received=rows_received,
        conversion_failures=conversion_failure_total,
        error=str(last_error),
    )


async def retry_until_data_deadline(
    result,
    cycle_hour,
    topic,
    folder_path,
    logger,
    semaphore,
):
    """Retry every five minutes and alert once after three failed polls."""
    deadline = cycle_hour.replace(
        minute=RETRY_DEADLINE_MINUTE,
        second=0,
        microsecond=0,
    )
    failed_attempts = 0 if result.success else int(result.attempts or 1)
    if failed_attempts == FAILED_FETCH_ALERT_THRESHOLD:
        logger.error(
            "Data unavailable after %s five-minute attempts for %s: %s",
            FAILED_FETCH_ALERT_THRESHOLD,
            topic["topic"],
            result.error,
        )
    while not result.success:
        now = datetime.now(timezone.utc)
        next_retry = now + timedelta(minutes=DELAYED_RETRY_MINUTES)
        if next_retry >= deadline:
            logger.warning(
                "Data remains unavailable for %s at the HH:%02d deadline; "
                "execution will use only fresh alpha signals",
                topic["topic"],
                RETRY_DEADLINE_MINUTE,
            )
            return result

        logger.info(
            "Delayed data for %s will be fetched again at %s UTC",
            topic["topic"],
            next_retry,
        )
        await asyncio.sleep(max(0, (next_retry - now).total_seconds()))
        result = await fetch_and_save_data(
            topic,
            folder_path,
            logger,
            semaphore,
        )
        if result.success:
            logger.info(
                "Delayed data recovered before HH:%02d for %s",
                RETRY_DEADLINE_MINUTE,
                topic["topic"],
            )
        else:
            failed_attempts += int(result.attempts or 1)
            if failed_attempts == FAILED_FETCH_ALERT_THRESHOLD:
                logger.error(
                    "Data unavailable after %s five-minute attempts for %s: %s",
                    FAILED_FETCH_ALERT_THRESHOLD,
                    topic["topic"],
                    result.error,
                )
    return result


async def run_data_loop(topic, folder_path, logger, semaphore):
    Path(folder_path).mkdir(parents=True, exist_ok=True)
    logger.info("Starting continuous data fetching for %s", topic["topic"])

    first_result = await fetch_and_save_data(
        topic,
        folder_path,
        logger,
        semaphore,
    )
    first_cycle_hour = datetime.now(timezone.utc).replace(
        minute=0,
        second=0,
        microsecond=0,
    )
    first_result = await retry_until_data_deadline(
        first_result,
        first_cycle_hour,
        topic,
        folder_path,
        logger,
        semaphore,
    )
    logger.info("Initial refresh result: %s", first_result.to_dict())

    while True:
        try:
            now = datetime.now(timezone.utc)
            next_hour = now.replace(
                minute=0,
                second=0,
                microsecond=0,
            ) + timedelta(hours=REFRESH_INTERVAL)
            next_refresh_time = next_hour + timedelta(minutes=DELAY_COLLECT)
            wait_seconds = max(0, (next_refresh_time - now).total_seconds())
            logger.info(
                "Next refresh for %s scheduled at %s UTC (wait %.0f seconds)",
                topic["topic"],
                next_refresh_time,
                wait_seconds,
            )
            await asyncio.sleep(wait_seconds)
            result = await fetch_and_save_data(
                topic,
                folder_path,
                logger,
                semaphore,
            )
            result = await retry_until_data_deadline(
                result,
                next_hour,
                topic,
                folder_path,
                logger,
                semaphore,
            )
            logger.info("Scheduled refresh result: %s", result.to_dict())
        except Exception as exc:
            logger.error(
                "Unexpected error in data fetching loop for %s: %s",
                topic["topic"],
                exc,
                exc_info=True,
            )
            await asyncio.sleep(60)


async def run_strategy(topic, topic_id, folder_path, logger, semaphore):
    try:
        print(f"Starting topic: {topic_id}")
        await run_data_loop(
            topic,
            folder_path,
            logger,
            semaphore,
        )
    except Exception as exc:
        logger.error(
            "Error in strategy %s: %s",
            topic_id,
            exc,
            exc_info=True,
        )


async def run_all_strategies(topics_with_names_and_paths, logger):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_FETCHES)
    tasks = [
        run_strategy(topic, name, folder_path, logger, semaphore)
        for name, topic, folder_path in topics_with_names_and_paths
    ]
    await asyncio.gather(*tasks)


def prepare_topics():
    validate_alpha_data_topics()
    topics = []
    missing = []
    for i, topic_string in enumerate(config_data.CQ_BTC1h_topic):
        if topic_string:
            topics.append(
                (
                    f"CQ_topic_{i + 1}",
                    {"topic": topic_string},
                    config_data.CQ_FOLDER_PATH,
                )
            )
        else:
            missing.append(topic_string)

    if missing:
        raise ValueError(f"Missing topic: {missing}")
    return topics


async def main():
    topics = prepare_topics()
    print("Running topics from CQ_BTC1h_topic")
    await run_all_strategies(topics, logger)


if __name__ == "__main__":
    asyncio.run(main())
