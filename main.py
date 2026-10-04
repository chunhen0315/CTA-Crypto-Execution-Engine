import asyncio
import csv
import sys
import os
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timedelta, timezone

import shutil
import subprocess
import config_data
import config_exe
from datafeed_utils import validate_alpha_data_topics
from logger import setup_logger

# ========================= CONFIGURATION =========================
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

EXECUTION_DELAY = 15

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

datafeed_files = [
    os.path.join(BASE_DIR, "datafeed_price.py"),
    os.path.join(BASE_DIR, "datafeed_CQ.py"),
    os.path.join(BASE_DIR, "datafeed_GN.py"),
]

main_file = os.path.join(BASE_DIR, "execution.py")
telegram_father_bot_file = os.path.join(BASE_DIR, "telegram_father_bot.py")

PRICE_FILE = os.path.join(
    config_data.PRICE_FOLDER_PATH,
    f"bybit-linear_1h_{config_exe.base}{config_exe.quote}.csv",
)

# ========================= LOGGING SETUP =========================
SCRIPT_NAME = os.path.basename(__file__).replace(".py", "")
logger = setup_logger(f"{SCRIPT_NAME}.log")



# =============================================================================
# Process Runner
# =============================================================================

def parse_price_datetime(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def get_latest_price_time():
    if not os.path.exists(PRICE_FILE):
        return None

    latest_time = None
    with open(PRICE_FILE, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("datetime") and row.get("close"):
                latest_time = row["datetime"]

    parsed_time = parse_price_datetime(latest_time)
    if parsed_time is not None and parsed_time.tzinfo is None:
        parsed_time = parsed_time.replace(tzinfo=timezone.utc)
    return parsed_time


def is_price_fresh():
    latest_time = get_latest_price_time()
    if latest_time is None:
        return False

    age_hours = (datetime.now(timezone.utc) - latest_time).total_seconds() / 3600
    max_age_hours = float(config_exe.PAPER_MAX_PRICE_AGE_HOURS)
    if age_hours <= max_age_hours:
        logger.info(f"Price feed is fresh: latest_time={latest_time}, age_hours={age_hours:.2f}")
        return True

    logger.warning(
        f"Waiting for fresh price feed: latest_time={latest_time}, "
        f"age_hours={age_hours:.2f}, max_age_hours={max_age_hours}"
    )
    return False


async def wait_for_fresh_price():
    timeout_seconds = int(config_exe.PRICE_FRESHNESS_WAIT_TIMEOUT)
    poll_seconds = int(config_exe.PRICE_FRESHNESS_POLL_SECONDS)
    deadline = datetime.now(timezone.utc) + timedelta(seconds=timeout_seconds)

    while datetime.now(timezone.utc) < deadline:
        if is_price_fresh():
            return True
        await asyncio.sleep(poll_seconds)

    logger.warning(f"Price feed did not become fresh within {timeout_seconds} seconds")
    return False

async def run_python_file(file_path, terminal_title=None):
    if not os.path.exists(file_path):
        logger.warning(f"File does not exist: {file_path}")
        return None

    try:
        logger.info(f"Starting: {file_path}")

        if sys.platform.startswith("win"):
            if terminal_title:
                cmd = f'start "{terminal_title}" cmd /k python "{file_path}"'
            else:
                cmd = f'start cmd /k python "{file_path}"'
            process = await asyncio.create_subprocess_shell(cmd)

        elif sys.platform == "darwin":
            script = (
                f"osascript -e "
                f"'tell app \"Terminal\" to do script "
                f"\"cd {os.getcwd()} && python {file_path}\"'"
            )
            process = await asyncio.create_subprocess_shell(script)

        else:  # Linux
            cmd = f"python3 {file_path}"
            process = await asyncio.create_subprocess_shell(cmd)

        logger.info(f"Process started: {file_path} (pid={process.pid})")
        return process

    except Exception as e:
        logger.warning(f"Error starting {file_path}: {e}")
        return None


# =============================================================================
# Main Logic
# =============================================================================

async def main():
    required_topics = validate_alpha_data_topics()
    logger.info(
        "Data topic availability check passed for %s enabled alphas",
        len(required_topics),
    )
    print("Launcher started")

    telegram_process = await run_python_file(
        telegram_father_bot_file,
        "Telegram Father Bot",
    )
    if telegram_process:
        logger.info("Telegram father bot process started")
        await asyncio.sleep(2)
    else:
        logger.warning(
            "Telegram father bot failed to start; trading launcher will continue"
        )

    datafeed_tasks = []
    for i, file_path in enumerate(datafeed_files):
        terminal_title = f"Terminal {i + 1} - {os.path.basename(file_path)}"
        task = asyncio.create_task(run_python_file(file_path, terminal_title))
        datafeed_tasks.append(task)

    datafeed_processes = await asyncio.gather(*datafeed_tasks)
    valid_datafeed_processes = [p for p in datafeed_processes if p]

    if not valid_datafeed_processes:
        logger.warning("No datafeed processes started successfully")
        return

    logger.info(f"Started {len(valid_datafeed_processes)} datafeed processes")
    logger.info(f"Waiting {EXECUTION_DELAY} seconds before starting main execution")
    await asyncio.sleep(EXECUTION_DELAY)

    if not await wait_for_fresh_price():
        logger.warning("Main execution not started because price feed is stale")
        return

    main_process = await run_python_file(
        main_file, f"Main Terminal - {os.path.basename(main_file)}"
    )

    if not main_process:
        logger.warning("Failed to start main program")
        return

    logger.info("Main program started")
    logger.info("Press Ctrl+C to stop all processes")

    all_processes = (
        ([telegram_process] if telegram_process else [])
        + valid_datafeed_processes
        + [main_process]
    )

    try:
        await asyncio.gather(*[p.wait() for p in all_processes if p])
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received, stopping all processes")
        for process in all_processes:
            if process and process.returncode is None:
                process.terminate()
                logger.info(f"Terminated process pid={process.pid}")


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    asyncio.run(main())
