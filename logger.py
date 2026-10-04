import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from logging.handlers import TimedRotatingFileHandler


LOG_RETENTION_DAYS = 120


def _component_name(log_filename: str):
    stem = os.path.splitext(os.path.basename(log_filename))[0]
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", stem).strip("._") or "application"


def _remove_expired_rotations(log_dir, log_filename):
    cutoff_date = (
        datetime.now(timezone.utc).date() - timedelta(days=LOG_RETENTION_DAYS)
    )
    pattern = re.compile(
        rf"^{re.escape(os.path.basename(log_filename))}\.(\d{{4}}-\d{{2}}-\d{{2}})$"
    )
    for filename in os.listdir(log_dir):
        match = pattern.match(filename)
        if not match:
            continue
        try:
            rotation_date = datetime.strptime(match.group(1), "%Y-%m-%d").date()
            if rotation_date < cutoff_date:
                os.remove(os.path.join(log_dir, filename))
        except (OSError, ValueError):
            continue


def setup_logger(log_filename: str):
    log_root = os.path.join(os.path.dirname(__file__), "logs")
    component = _component_name(log_filename)
    log_dir = os.path.join(log_root, component)
    os.makedirs(log_dir, exist_ok=True)
    _remove_expired_rotations(log_dir, log_filename)

    log_file = os.path.join(log_dir, os.path.basename(log_filename))

    logger = logging.getLogger(log_filename)  # unique logger per file
    logger.setLevel(logging.INFO)
    logger.propagate = False  # prevent double logs

    # Avoid adding handlers twice
    if logger.handlers:
        return logger

    file_handler = TimedRotatingFileHandler(
        log_file,
        when="d",
        interval=1,
        backupCount=LOG_RETENTION_DAYS,
        encoding="utf-8",
        utc=True,
        delay=True,
    )
    file_handler.suffix = "%Y-%m-%d"

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )
    formatter.converter = time.gmtime

    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

