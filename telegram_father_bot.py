import argparse
import json
import os
import re
import secrets
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from dotenv import load_dotenv

from logger import setup_logger


BASE_DIR = Path(__file__).resolve().parent
LOG_ROOT = BASE_DIR / "logs"
RULES_FILE = BASE_DIR / "telegram_alert_rules.json"
STATE_DIR = BASE_DIR / "data" / "telegram_father_bot"
STATE_FILE = STATE_DIR / "watcher_state.json"
LOCK_FILE = STATE_DIR / "father_bot.lock"
SELF_LOG_FOLDER = "telegram_father_bot"
REVISION_ALERT_PHRASE = "DATA REVISION ALERT"
REVISION_ALERT_SOURCES = {"datafeed_cq", "datafeed_gn"}
ORDER_ALERT_RULES = {
    "Market buy order executed:",
    "Market sell order executed:",
    "Paper trade:",
}
REVISION_DATA_DIFF_RE = re.compile(r"\bmax_data_diff=(?P<value>\S+)")
REVISION_PCT_CHANGE_RE = re.compile(r"\bmax_pct_chg=(?P<value>\S+)")
LOG_LINE_RE = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:,\d+)?)\s+\|\s+"
    r"(?P<level>[A-Z]+)\s+\|\s+(?P<message>.*)$"
)

load_dotenv(BASE_DIR / ".env")
logger = setup_logger("telegram_father_bot.log")


def acquire_instance_lock():
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    lock_file = LOCK_FILE.open("a+", encoding="utf-8")
    lock_file.seek(0, os.SEEK_END)
    if lock_file.tell() == 0:
        lock_file.write(" ")
        lock_file.flush()
    lock_file.seek(0)

    try:
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (OSError, BlockingIOError) as exc:
        lock_file.close()
        raise RuntimeError("Telegram father bot is already running") from exc

    lock_file.seek(0)
    lock_file.truncate()
    lock_file.write(str(os.getpid()))
    lock_file.flush()
    return lock_file


def atomic_write_json(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    with temp_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2)
        file.flush()
        os.fsync(file.fileno())
    os.replace(temp_path, path)


def load_json(path, default):
    path = Path(path)
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as file:
            loaded = json.load(file)
        return loaded if isinstance(loaded, dict) else default
    except Exception as exc:
        logger.error(f"Unable to read Telegram watcher state: {exc}")
        return default


def load_rules():
    with RULES_FILE.open("r", encoding="utf-8") as file:
        rules = json.load(file)

    rules["immediate_levels"] = {
        str(item).upper() for item in rules.get("immediate_levels", [])
    }
    rules["immediate_phrases"] = [
        str(item) for item in rules.get("immediate_phrases", [])
    ]
    rules["repeated_phrases"] = [
        str(item) for item in rules.get("repeated_phrases", [])
    ]
    return rules


def default_state():
    return {
        "version": 1,
        "initialized": False,
        "chat_id": None,
        "telegram_update_offset": 0,
        "files": {},
        "pending_messages": [],
        "dedupe": {},
        "warning_hits": {},
    }


def redact_text(value, token, max_characters):
    text = str(value)
    if token:
        text = text.replace(token, "<redacted-token>")
    text = re.sub(
        r"https://api\.telegram\.org/bot[^/\s]+",
        "https://api.telegram.org/bot<redacted-token>",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"(?i)\b(api[_-]?key|api[_-]?secret|secret|authorization|token)"
        r"\s*[:=]\s*[^\s,;]+",
        r"\1=<redacted>",
        text,
    )
    if len(text) > max_characters:
        text = f"{text[:max_characters - 30]}\n... <message truncated>"
    return text


def normalized_alert_key(source, rule, message):
    stable_message = re.sub(r"https?://\S+", "<url>", message)
    stable_message = re.sub(r"\b[0-9a-f]{16,}\b", "<id>", stable_message, flags=re.I)
    stable_message = re.sub(r"\b\d+(?:\.\d+)?\b", "<number>", stable_message)
    return f"{source}|{rule}|{stable_message[:240].lower()}"


def extract_revision_metrics(message):
    data_difference = REVISION_DATA_DIFF_RE.search(message)
    percentage_change = REVISION_PCT_CHANGE_RE.search(message)
    cleaned_message = REVISION_DATA_DIFF_RE.sub("", message)
    cleaned_message = REVISION_PCT_CHANGE_RE.sub("", cleaned_message)
    cleaned_message = re.sub(r" {2,}", " ", cleaned_message).strip()
    return (
        cleaned_message,
        data_difference.group("value") if data_difference else None,
        percentage_change.group("value") if percentage_change else None,
    )


def source_uses_revision_bot(source):
    component = str(source).replace("\\", "/").split("/", maxsplit=1)[0]
    return component.lower() in REVISION_ALERT_SOURCES


def queued_alert_uses_revision_bot(text):
    if REVISION_ALERT_PHRASE.lower() in str(text).lower():
        return True
    source_match = re.search(r"(?im)^Source:\s*(?P<source>[^\r\n]+)", str(text))
    return bool(
        source_match
        and source_uses_revision_bot(source_match.group("source").strip())
    )


class TelegramClient:
    def __init__(self, token, timeout_seconds):
        self.token = token
        self.timeout_seconds = float(timeout_seconds)

    def request(self, method, payload=None):
        if not self.token:
            raise RuntimeError("TELEGRAM_BOT_TOKEN is not configured")

        encoded = urllib.parse.urlencode(payload or {}).encode("utf-8")
        request = urllib.request.Request(
            f"https://api.telegram.org/bot{self.token}/{method}",
            data=encoded,
            method="POST",
        )
        try:
            with urllib.request.urlopen(
                request,
                timeout=self.timeout_seconds,
            ) as response:
                body = response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"Telegram {method} returned HTTP {exc.code}: {body[:300]}"
            ) from exc
        except Exception as exc:
            raise RuntimeError(f"Telegram {method} request failed: {exc}") from exc

        result = json.loads(body)
        if not result.get("ok"):
            raise RuntimeError(
                f"Telegram {method} rejected the request: "
                f"{result.get('description', 'unknown error')}"
            )
        return result.get("result")

    def send_message(self, chat_id, text):
        return self.request(
            "sendMessage",
            {
                "chat_id": str(chat_id),
                "text": text,
                "disable_web_page_preview": "true",
            },
        )

    def get_updates(self, offset):
        return self.request(
            "getUpdates",
            {
                "offset": int(offset),
                "timeout": 0,
                "allowed_updates": json.dumps(["message"]),
            },
        )


class LogAlertWatcher:
    def __init__(
        self,
        token,
        pair_code,
        configured_chat_id=None,
        revision_token=None,
        revision_chat_id=None,
    ):
        self.token = token
        self.pair_code = pair_code
        self.sender_only = bool(configured_chat_id)
        self.rules = load_rules()
        self.state = load_json(STATE_FILE, default_state())
        for key, value in default_state().items():
            self.state.setdefault(key, value)

        if configured_chat_id:
            self.state["chat_id"] = str(configured_chat_id)

        self.client = TelegramClient(
            token,
            self.rules.get("telegram_timeout_seconds", 10),
        )
        self.revision_token = revision_token or ""
        self.revision_chat_id = (
            str(revision_chat_id) if revision_chat_id else None
        )
        self.revision_client = (
            TelegramClient(
                self.revision_token,
                self.rules.get("telegram_timeout_seconds", 10),
            )
            if self.revision_token
            else None
        )
        self.hostname = socket.gethostname()
        self.max_message_characters = int(
            self.rules.get("max_message_characters", 3500)
        )
        self.internal_log_times = {}

    def save_state(self):
        atomic_write_json(STATE_FILE, self.state)

    def state_fingerprint(self):
        return json.dumps(self.state, sort_keys=True, separators=(",", ":"))

    def throttled_log(self, key, level, message, cooldown_seconds=300):
        now = time.time()
        if now - float(self.internal_log_times.get(key, 0) or 0) < cooldown_seconds:
            return
        self.internal_log_times[key] = now
        getattr(logger, level)(message)

    def log_paths(self):
        if not LOG_ROOT.exists():
            return []

        paths = []
        for path in LOG_ROOT.rglob("*.log*"):
            if not path.is_file():
                continue
            try:
                relative = path.relative_to(LOG_ROOT)
            except ValueError:
                continue
            if SELF_LOG_FOLDER in relative.parts:
                continue
            paths.append(path)
        return sorted(paths)

    @staticmethod
    def file_identity(path):
        stat = path.stat()
        inode = getattr(stat, "st_ino", 0)
        device = getattr(stat, "st_dev", 0)
        if inode:
            return f"{device}:{inode}"
        return f"path:{path.resolve()}"

    def initialize_offsets(self):
        if self.state.get("initialized"):
            return

        now = time.time()
        for path in self.log_paths():
            try:
                stat = path.stat()
                identity = self.file_identity(path)
            except OSError:
                continue
            self.state["files"][identity] = {
                "path": str(path),
                "offset": stat.st_size,
                "last_seen": now,
            }

        self.state["initialized"] = True
        self.save_state()
        logger.info(
            "Telegram log watcher initialized at the end of "
            f"{len(self.state['files'])} existing raw log files"
        )

    def read_complete_lines(self, path, offset):
        size = path.stat().st_size
        if size < offset:
            offset = 0
        if size == offset:
            return [], offset

        with path.open("rb") as file:
            file.seek(offset)
            data = file.read()

        newline_index = data.rfind(b"\n")
        if newline_index < 0:
            return [], offset

        completed = data[: newline_index + 1]
        new_offset = offset + newline_index + 1
        lines = completed.decode("utf-8", errors="replace").splitlines()
        return lines, new_offset

    def match_rule(self, level, message):
        if level.upper() in self.rules["immediate_levels"]:
            return "immediate", f"level:{level.upper()}"

        lowered = message.lower()
        for phrase in self.rules["immediate_phrases"]:
            if phrase.lower() in lowered:
                return "immediate", phrase
        for phrase in self.rules["repeated_phrases"]:
            if phrase.lower() in lowered:
                return "repeated", phrase
        return None, None

    def repeated_rule_ready(self, key, now):
        window = int(self.rules.get("repeated_window_seconds", 900))
        threshold = int(self.rules.get("repeated_threshold", 3))
        hits = [
            float(item)
            for item in self.state["warning_hits"].get(key, [])
            if now - float(item) <= window
        ]
        hits.append(now)
        self.state["warning_hits"][key] = hits
        return len(hits) >= threshold, len(hits)

    def enqueue_alert(
        self,
        source,
        timestamp,
        level,
        message,
        rule,
        repeat_count=1,
        route="primary",
    ):
        now = time.time()
        dedupe_key = normalized_alert_key(f"{route}:{source}", rule, message)
        cooldown = (
            0
            if rule in ORDER_ALERT_RULES
            else int(self.rules.get("dedupe_cooldown_seconds", 600))
        )
        last_sent_or_queued = float(self.state["dedupe"].get(dedupe_key, 0) or 0)
        if cooldown and now - last_sent_or_queued < cooldown:
            return

        if cooldown:
            self.state["dedupe"][dedupe_key] = now
        priority = "CRITICAL" if level.upper() in {"ERROR", "CRITICAL"} else "ALERT"
        repeat_text = f"\nOccurrences in window: {repeat_count}" if repeat_count > 1 else ""
        metric_text = ""
        display_message = message
        if route == "revision":
            display_message, max_data_diff, max_pct_chg = (
                extract_revision_metrics(message)
            )
            if max_data_diff is not None:
                metric_text += f"\nmax_data_diff: {max_data_diff}"
            if max_pct_chg is not None:
                metric_text += f"\nmax_pct_chg: {max_pct_chg}"
        alert_text = (
            f"[{priority}] Alphora production\n"
            f"Server: {self.hostname}\n"
            f"Source: {source}\n"
            f"Time: {timestamp} UTC\n"
            f"Level: {level}\n"
            f"Rule: {rule}{repeat_text}{metric_text}\n\n"
            f"{display_message}"
        )
        alert_text = redact_text(
            alert_text,
            self.token,
            self.max_message_characters,
        )
        alert_text = redact_text(
            alert_text,
            self.revision_token,
            self.max_message_characters,
        )
        self.state["pending_messages"].append(
            {
                "id": secrets.token_hex(8),
                "text": alert_text,
                "route": route,
                "attempts": 0,
                "next_attempt": now,
                "created_at": now,
            }
        )
        if len(self.state["pending_messages"]) > 1000:
            self.state["pending_messages"] = self.state["pending_messages"][-1000:]
            logger.error("Telegram pending alert queue exceeded 1000 messages")

    def process_log_line(self, source, line):
        match = LOG_LINE_RE.match(line)
        if not match:
            return

        timestamp = match.group("timestamp")
        level = match.group("level")
        message = match.group("message")
        is_revision_message = (
            REVISION_ALERT_PHRASE.lower() in message.lower()
        )
        route = (
            "revision"
            if is_revision_message or source_uses_revision_bot(source)
            else "primary"
        )
        if is_revision_message:
            alert_type, rule = "immediate", REVISION_ALERT_PHRASE
        else:
            alert_type, rule = self.match_rule(level, message)
        if not alert_type:
            return

        if route == "revision" and (
            not self.revision_client or not self.revision_chat_id
        ):
            self.throttled_log(
                "revision-telegram-not-configured",
                "error",
                "A datafeed alert was detected, but the separate data revision "
                "Telegram bot is not configured. Set "
                "DATA_REVISION_TELEGRAM_BOT_TOKEN and ensure a Telegram chat "
                "ID is available.",
            )
            return

        repeat_count = 1
        if alert_type == "repeated":
            repeat_key = normalized_alert_key(source, rule, message)
            ready, repeat_count = self.repeated_rule_ready(repeat_key, time.time())
            if not ready:
                return

        self.enqueue_alert(
            source=source,
            timestamp=timestamp,
            level=level,
            message=message,
            rule=rule,
            repeat_count=repeat_count,
            route=route,
        )

    def scan_logs(self):
        now = time.time()
        seen_identities = set()
        files_state = self.state["files"]

        for path in self.log_paths():
            try:
                identity = self.file_identity(path)
                seen_identities.add(identity)
                entry = files_state.setdefault(
                    identity,
                    {
                        "path": str(path),
                        "offset": 0,
                        "last_seen": now,
                    },
                )
                lines, new_offset = self.read_complete_lines(
                    path,
                    int(entry.get("offset", 0)),
                )
                entry.update(
                    {
                        "path": str(path),
                        "offset": new_offset,
                        "last_seen": (
                            now
                            if now - float(entry.get("last_seen", 0) or 0) >= 3600
                            else entry.get("last_seen", now)
                        ),
                    }
                )
            except Exception as exc:
                self.throttled_log(
                    f"log-read:{path}",
                    "error",
                    f"Unable to read watched log {path.name}: {exc}",
                )
                continue

            source = str(path.relative_to(LOG_ROOT))
            for line in lines:
                self.process_log_line(source, line)

        state_retention_seconds = 130 * 24 * 3600
        for identity in list(files_state):
            last_seen = float(files_state[identity].get("last_seen", 0) or 0)
            if identity not in seen_identities and now - last_seen > state_retention_seconds:
                del files_state[identity]

        dedupe_retention = max(
            int(self.rules.get("dedupe_cooldown_seconds", 600)) * 2,
            3600,
        )
        self.state["dedupe"] = {
            key: value
            for key, value in self.state["dedupe"].items()
            if now - float(value) <= dedupe_retention
        }

    def poll_pairing(self):
        try:
            updates = self.client.get_updates(
                self.state.get("telegram_update_offset", 0)
            )
        except Exception as exc:
            self.throttled_log(
                "telegram-poll",
                "warning",
                redact_text(
                    f"Unable to poll Telegram updates: {exc}",
                    self.token,
                    1000,
                ),
            )
            return

        for update in updates or []:
            update_id = int(update.get("update_id", 0))
            self.state["telegram_update_offset"] = max(
                int(self.state.get("telegram_update_offset", 0)),
                update_id + 1,
            )
            message = update.get("message") or {}
            chat = message.get("chat") or {}
            chat_id = chat.get("id")
            text = str(message.get("text") or "").strip()
            if not chat_id or not text:
                continue

            command = text.split()[0].split("@")[0].lower()
            if command == "/start":
                supplied_code = text.split(maxsplit=1)[1].strip() if len(text.split(maxsplit=1)) == 2 else ""
                if self.pair_code and secrets.compare_digest(
                    supplied_code.upper(),
                    self.pair_code.upper(),
                ):
                    self.state["chat_id"] = str(chat_id)
                    self.client.send_message(
                        chat_id,
                        (
                            "Alphora father bot paired successfully.\n"
                            "Raw-log keyword alerts are now enabled."
                        ),
                    )
                    logger.info("Telegram destination paired successfully")
                elif str(chat_id) == str(self.state.get("chat_id") or ""):
                    self.client.send_message(
                        chat_id,
                        "Alphora father bot is already paired and running.",
                    )
                else:
                    self.client.send_message(
                        chat_id,
                        "A valid pairing code is required.",
                    )
            elif command == "/status" and str(chat_id) == str(
                self.state.get("chat_id") or ""
            ):
                self.client.send_message(
                    chat_id,
                    (
                        "Alphora father bot is running.\n"
                        f"Watched files: {len(self.state['files'])}\n"
                        f"Pending alerts: {len(self.state['pending_messages'])}"
                    ),
                )

    def flush_pending(self):
        chat_id = self.state.get("chat_id")
        if not chat_id and not self.revision_chat_id:
            return

        now = time.time()
        remaining = []
        for item in self.state["pending_messages"]:
            if float(item.get("next_attempt", 0) or 0) > now:
                remaining.append(item)
                continue

            route = item.get("route") or "primary"
            if queued_alert_uses_revision_bot(item.get("text", "")):
                route = "revision"
            if route == "revision":
                client = self.revision_client
                destination = self.revision_chat_id
                delivery_token = self.revision_token
            else:
                client = self.client
                destination = chat_id
                delivery_token = self.token

            if not client or not destination:
                remaining.append(item)
                self.throttled_log(
                    f"telegram-delivery-not-configured:{route}",
                    "error",
                    f"Telegram alert route '{route}' is not configured; "
                    "message remains queued.",
                )
                continue
            try:
                client.send_message(destination, item["text"])
            except Exception as exc:
                attempts = int(item.get("attempts", 0)) + 1
                item["attempts"] = attempts
                item["next_attempt"] = now + min(300, 2 ** min(attempts, 8))
                remaining.append(item)
                self.throttled_log(
                    "telegram-delivery",
                    "warning",
                    redact_text(
                        f"Telegram {route} alert delivery failed; queued for retry: {exc}",
                        delivery_token,
                        1000,
                    ),
                )
        self.state["pending_messages"] = remaining

    def run_once(self):
        self.initialize_offsets()
        before = self.state_fingerprint()
        if not self.sender_only:
            self.poll_pairing()
        self.scan_logs()
        self.flush_pending()
        if self.state_fingerprint() != before:
            self.save_state()

    def run_forever(self):
        self.initialize_offsets()
        paired = bool(self.state.get("chat_id"))
        logger.info(
            "Telegram father bot started: "
            f"paired={paired}, sender_only={self.sender_only}, "
            f"revision_bot_configured={bool(self.revision_client and self.revision_chat_id)}, "
            f"scan_interval_seconds="
            f"{self.rules.get('scan_interval_seconds', 2)}"
        )
        while True:
            try:
                before = self.state_fingerprint()
                if not self.sender_only:
                    self.poll_pairing()
                self.scan_logs()
                self.flush_pending()
                if self.state_fingerprint() != before:
                    self.save_state()
            except Exception as exc:
                logger.exception(f"Unexpected Telegram father bot loop error: {exc}")
            time.sleep(float(self.rules.get("scan_interval_seconds", 2)))


def parse_args():
    parser = argparse.ArgumentParser(
        description="Watch Alphora raw logs and send Telegram keyword alerts."
    )
    parser.add_argument(
        "--check-config",
        action="store_true",
        help="Verify the Telegram token with getMe, then exit.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Perform one pairing/log/queue pass, then exit.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    pair_code = os.getenv("TELEGRAM_PAIR_CODE", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip() or None
    revision_token = os.getenv(
        "DATA_REVISION_TELEGRAM_BOT_TOKEN",
        "",
    ).strip()
    revision_chat_id = (
        os.getenv("DATA_REVISION_TELEGRAM_CHAT_ID", "").strip()
        or chat_id
    )

    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is missing from .env")
    if not chat_id and not pair_code:
        raise RuntimeError(
            "Set TELEGRAM_CHAT_ID or TELEGRAM_PAIR_CODE in .env"
        )

    watcher = LogAlertWatcher(
        token,
        pair_code,
        chat_id,
        revision_token=revision_token,
        revision_chat_id=revision_chat_id,
    )
    if args.check_config:
        bot = watcher.client.request("getMe")
        username = bot.get("username", "unknown")
        print(f"Telegram bot connection OK: @{username}")
        if watcher.revision_client:
            revision_bot = watcher.revision_client.request("getMe")
            revision_username = revision_bot.get("username", "unknown")
            print(
                "Data revision Telegram bot connection OK: "
                f"@{revision_username}"
            )
        else:
            print(
                "Data revision Telegram bot is not configured: set "
                "DATA_REVISION_TELEGRAM_BOT_TOKEN in .env"
            )
        return

    instance_lock = acquire_instance_lock()
    if args.once:
        watcher.run_once()
        return
    watcher.run_forever()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Telegram father bot stopped by user")
