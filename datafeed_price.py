import asyncio
import argparse
import os
import re
from datetime import datetime, timezone, timedelta

import cybotrade_datasource
import pandas as pd
from dotenv import load_dotenv

import config_data
from datafeed_utils import validate_alpha_data_topics
from logger import setup_logger

load_dotenv()

REFRESH_INTERVAL = config_data.REFRESH_INTERVAL
RE_REFRESH_INTERVAL = config_data.RE_REFRESH_INTERVAL
MAX_TIME_DIFFERENCE = config_data.MAX_TIME_DIFFERENCE
DELAY_COLLECT = config_data.PRICE_DELAY_COLLECT
API_KEY = config_data.API_KEY

SCRIPT_NAME = os.path.basename(__file__).replace(".py", "")
logger = setup_logger(f"{SCRIPT_NAME}.log")
DEFAULT_BACKTEST_PRICE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "backtest_CQ", "monitor", "price")
)


def extract_topic_parts(topic):
    topic_parts = topic.split("|")
    if len(topic_parts) < 2:
        fallback_name = topic.split("|")[-1].replace("/", "_").split("?")[0]
        return fallback_name, config_data.PRICE_SOURCE_INTERVAL, f"{config_data.ASSET}USDT"

    exchange_part = topic_parts[0].split("/")[-1]
    query_part = topic_parts[1]

    symbol_match = re.search(r"symbol=([A-Z0-9]+)", query_part)
    interval_match = re.search(r"interval=([A-Za-z0-9]+)", query_part)

    symbol_part = symbol_match.group(1) if symbol_match else query_part.split("?")[0]
    interval_part = interval_match.group(1) if interval_match else config_data.PRICE_SOURCE_INTERVAL
    return exchange_part, interval_part, symbol_part


def build_aligned_price_df(df):
    price_df = df.copy()
    if "datetime" not in price_df.columns and "start_time" in price_df.columns:
        price_df["datetime"] = pd.to_datetime(price_df["start_time"], unit="ms", utc=True)
    else:
        price_df["datetime"] = pd.to_datetime(price_df["datetime"], utc=True)

    price_df = price_df.set_index("datetime").sort_index()
    price_df = price_df[["close"]].astype(float)

    aligned_df = price_df.resample(config_data.PRICE_OUTPUT_INTERVAL).last()
    aligned_df = aligned_df.dropna(subset=["close"])
    aligned_df.index.name = "datetime"
    return aligned_df


def rebuild_aligned_price_from_csv(source_file_path, folder_path, exchange_part, source_interval, symbol_part):
    os.makedirs(folder_path, exist_ok=True)
    raw_file_path = os.path.join(folder_path, f"{exchange_part}_{source_interval}_{symbol_part}.csv")
    aligned_file_path = os.path.join(
        folder_path,
        f"{exchange_part}_{config_data.PRICE_OUTPUT_INTERVAL}_{symbol_part}.csv",
    )

    df = pd.read_csv(source_file_path)
    aligned_df = build_aligned_price_df(df)

    df.to_csv(raw_file_path, index=False)
    aligned_df.to_csv(aligned_file_path, index=True)

    print(f"Source price:  {source_file_path}")
    print(f"Raw 1m saved:  {raw_file_path}")
    print(f"Aligned saved: {aligned_file_path}")
    print(
        f"Aligned from {aligned_df.index.min()} to {aligned_df.index.max()} "
        f"using shift=0, resample={config_data.PRICE_OUTPUT_INTERVAL}. "
        "Alpha-specific shifts are applied in alpha_lib.py."
    )


async def fetch_and_save_data(topic, folder_path):
    topic_string = topic.get("topic")
    end_time = datetime.now(timezone.utc)
    price_data_len_hours = getattr(config_data, "PRICE_DATA_LEN_HOURS", config_data.DATA_LEN)
    start_time = end_time - timedelta(hours=price_data_len_hours)

    try:
        data = await cybotrade_datasource.query_paginated(
            api_key=API_KEY,
            topic=topic_string,
            start_time=start_time,
            end_time=end_time,
        )
        if not data:
            logger.warning(f"No data received for {topic_string}. Response: {data}")
            return

        exchange_part, source_interval, symbol_part = extract_topic_parts(topic_string)
        raw_file_path = os.path.join(folder_path, f"{exchange_part}_{source_interval}_{symbol_part}.csv")
        aligned_file_path = os.path.join(
            folder_path,
            f"{exchange_part}_{config_data.PRICE_OUTPUT_INTERVAL}_{symbol_part}.csv",
        )

        df = pd.DataFrame(data)
        df["start_time"] = pd.to_datetime(df["start_time"], unit="ms", utc=True)
        df = df.rename(columns={"start_time": "datetime"})
        df = df.sort_values("datetime")

        df.to_csv(raw_file_path, index=False)
        aligned_df = build_aligned_price_df(df)
        aligned_df.to_csv(aligned_file_path, index=True)

        logger.info(f"Querying topic: {topic_string}")
        logger.info(
            f"Received {len(data)} records over {price_data_len_hours} hours. "
            f"Saved raw to {raw_file_path}"
        )
        logger.info(
            f"Saved aligned price to {aligned_file_path} "
            f"(source_interval={source_interval}, "
            f"output_interval={config_data.PRICE_OUTPUT_INTERVAL}, "
            "shift=0; alpha-specific shifts are applied in alpha_lib.py)"
        )

        latest_data_time = aligned_df.index.max()
        current_time = pd.Timestamp.now(tz="UTC")
        time_difference = current_time - latest_data_time

        logger.info(f"Latest aligned data time: {latest_data_time} (UTC)")
        logger.info(f"Current time: {current_time} (UTC)")
        logger.info(f"Time difference: {time_difference}")

        if time_difference > timedelta(minutes=MAX_TIME_DIFFERENCE):
            logger.warning(
                f"Time difference > {MAX_TIME_DIFFERENCE} min, "
                f"will refresh again in {RE_REFRESH_INTERVAL} seconds..."
            )
            await asyncio.sleep(RE_REFRESH_INTERVAL)
            await fetch_and_save_data(topic, folder_path)

    except Exception as e:
        logger.error(f"An error occurred for {topic_string}: {e}", exc_info=True)


async def run_data_loop(topic, folder_path):
    os.makedirs(folder_path, exist_ok=True)
    logger.info(f"Starting continuous price fetching process for {folder_path}...")

    await fetch_and_save_data(topic, folder_path)
    logger.info("First run completed.")

    while True:
        try:
            now = datetime.now(timezone.utc)
            next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=REFRESH_INTERVAL)
            next_refresh_time = next_hour + timedelta(minutes=DELAY_COLLECT)
            wait_seconds = (next_refresh_time - now).total_seconds()

            hours, remainder = divmod(wait_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)

            logger.info(f"Next refresh scheduled at: {next_refresh_time} (UTC)")
            logger.info(
                f"Waiting for {int(hours):02d}:{int(minutes):02d}:{int(seconds):02d} "
                f"(HH:MM:SS) until next refresh..."
            )
            await asyncio.sleep(wait_seconds)
            await fetch_and_save_data(topic, folder_path)

        except Exception as e:
            logger.error(f"Error in price fetching loop: {e}", exc_info=True)
            await asyncio.sleep(60)


async def run_strategy(topic, topic_id, folder_path):
    try:
        print(f"Starting topic: {topic_id}")
        await run_data_loop(topic, folder_path)
    except Exception as e:
        logger.error(f"Error in strategy {topic_id}: {e}", exc_info=True)


async def run_all_strategies(topics_with_names_and_paths):
    tasks = [
        run_strategy(topic, name, folder_path)
        for name, topic, folder_path in topics_with_names_and_paths
    ]
    await asyncio.gather(*tasks)


def prepare_topics():
    validate_alpha_data_topics()
    topics = []
    missing = []

    for i, topic_string in enumerate(config_data.PRICE_topic):
        if topic_string:
            topics.append((f"PRICE_topic_{i + 1}", {"topic": topic_string}, config_data.PRICE_FOLDER_PATH))
        else:
            missing.append(topic_string)

    if missing:
        raise ValueError(f"Missing topic: {missing}")

    return topics


async def main():
    topics = prepare_topics()
    print("Running topics from PRICE_topic")
    await run_all_strategies(topics)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch or rebuild aligned production price files.")
    parser.add_argument(
        "--rebuild-from-backtest-price",
        action="store_true",
        help="Rebuild production 1h price from backtest_CQ 1m price CSV using configured shift/resample.",
    )
    parser.add_argument("--asset", default=config_data.ASSET)
    args = parser.parse_args()

    if args.rebuild_from_backtest_price:
        asset = args.asset.upper()
        source_file = os.path.join(DEFAULT_BACKTEST_PRICE_DIR, f"bybit_1m_{asset}USDT.csv")
        rebuild_aligned_price_from_csv(
            source_file,
            config_data.PRICE_FOLDER_PATH,
            "bybit-linear",
            config_data.PRICE_SOURCE_INTERVAL,
            f"{asset}USDT",
        )
    else:
        asyncio.run(main())
