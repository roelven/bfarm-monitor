#!/usr/bin/env python3
"""
BfArM Lieferengpass Monitor
Tracks drug supply shortage reports from the BfArM CSV endpoint
and sends notifications when changes are detected for watched medications.
"""

import csv
import hashlib
import io
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CSV_URL = "https://anwendungen.pharmnet-bund.de/lieferengpassmeldungen/public/csv"
STATE_FILE = Path(os.getenv("STATE_FILE", "/data/state.json"))
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "21600"))  # 6 hours default
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

# Notification config
NOTIFY_METHOD = os.getenv("NOTIFY_METHOD", "ntfy")  # ntfy | gotify | smtp | telegram | signal
NTFY_URL = os.getenv("NTFY_URL", "https://ntfy.sh")
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "bfarm-monitor")
NTFY_TOKEN = os.getenv("NTFY_TOKEN", "")
GOTIFY_URL = os.getenv("GOTIFY_URL", "")
GOTIFY_TOKEN = os.getenv("GOTIFY_TOKEN", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
SMTP_FROM = os.getenv("SMTP_FROM", "")
SMTP_TO = os.getenv("SMTP_TO", "")
SIGNAL_API_URL = os.getenv("SIGNAL_API_URL", "http://signal-api:8080")
SIGNAL_SENDER = os.getenv("SIGNAL_SENDER", "")
SIGNAL_RECIPIENTS = json.loads(os.getenv("SIGNAL_RECIPIENTS", "[]"))

# Medications to watch — loaded from WATCH_LIST env var (set in .env).
# Each entry: {"label": str, "match_wirkstoffe": [str], "match_name": [str]}
WATCH_LIST: list[dict] = json.loads(os.getenv("WATCH_LIST", "[]"))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("bfarm-monitor")

# ---------------------------------------------------------------------------
# CSV Fetching & Parsing
# ---------------------------------------------------------------------------

# Reference list of expected CSV columns (informational; actual header drives mapping).
CSV_FIELDS = [
    "PZN", "ENR", "Bearbeitungsnummer", "Referenzierte Erstmeldung",
    "Meldungsart", "Beginn", "Ende", "Datum der letzten Meldung",
    "Art des Grundes", "Arzneimittelbezeichnung", "ATC Code", "Wirkstoffe",
    "Krankenhausrelevant", "Zulassungsinhaber", "Telefon", "E-Mail",
    "Grund", "Anmerkung zum Grund", "Alternativpraeparat",
    "Datum der Erstmeldung", "Info an Fachkreise", "Darreichungsform",
    "Klassifikation",
]


def fetch_csv() -> list[dict]:
    """Download and parse the BfArM CSV, return list of row dicts."""
    log.info("Fetching CSV from %s", CSV_URL)
    resp = requests.get(CSV_URL, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    # BfArM serves this as latin-1 encoded CSV with semicolon delimiter
    text = resp.content.decode("latin-1")
    reader = csv.reader(io.StringIO(text), delimiter=";")

    header = next(reader, None)
    if not header:
        log.warning("Empty CSV received")
        return []

    rows = []
    for raw_row in reader:
        # Pad short rows so zip covers all header columns
        if len(raw_row) < len(header):
            raw_row.extend([""] * (len(header) - len(raw_row)))
        rows.append(dict(zip(header, raw_row[: len(header)])))

    log.info("Parsed %d shortage reports", len(rows))
    return rows


def matches_watch(row: dict, watch: dict) -> bool:
    """Check if a CSV row matches a watch list entry."""
    name = row.get("Arzneimittelbezeichnung", "").lower()
    wirkstoffe = row.get("Wirkstoffe", "").lower()

    for term in watch.get("match_wirkstoffe", []):
        if term.lower() in wirkstoffe:
            return True
    for term in watch.get("match_name", []):
        if term.lower() in name:
            return True
    return False


def find_matches(rows: list[dict]) -> dict[str, list[dict]]:
    """Return {watch_label: [matching_rows]} for all watch list entries."""
    results: dict[str, list[dict]] = {}
    for watch in WATCH_LIST:
        label = watch["label"]
        matched = [r for r in rows if matches_watch(r, watch)]
        results[label] = matched
    return results


# ---------------------------------------------------------------------------
# State Management
# ---------------------------------------------------------------------------


def row_fingerprint(row: dict) -> str:
    """Create a fingerprint for a shortage row; changes when any key field changes."""
    key_fields = [
        row.get("PZN", ""),
        row.get("ENR", ""),
        row.get("Bearbeitungsnummer", ""),
        row.get("Meldungsart", ""),
        row.get("Beginn", ""),
        row.get("Ende", ""),
        row.get("Datum der letzten Meldung", ""),
        row.get("Art des Grundes", ""),
    ]
    return hashlib.sha256("|".join(key_fields).encode()).hexdigest()[:16]


def row_stable_key(row: dict) -> str:
    """Stable identity for a shortage record, excluding mutable fields like dates.
    Used to detect when an existing shortage is updated rather than replaced."""
    return "|".join([
        row.get("Bearbeitungsnummer", ""),
        row.get("PZN", ""),
        row.get("ENR", ""),
    ])


def load_state() -> dict:
    """Load previous state from disk."""
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except (json.JSONDecodeError, OSError) as e:
            log.warning("Failed to load state: %s", e)
    return {"fingerprints": {}, "last_check": None}


def save_state(state: dict):
    """Persist state to disk."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False))


def diff_matches(
    current: dict[str, list[dict]], state: dict
) -> tuple[dict[str, list[dict]], dict[str, list[dict]], dict[str, list[dict]]]:
    """
    Compare current matches against saved state.
    Returns (new, changed, resolved) dicts keyed by watch label.

    A record is 'changed' when its stable key (Bearbeitungsnummer/PZN/ENR)
    matches a previous record but its fingerprint differs — e.g. an updated
    end date. Without this distinction every update would fire as new+resolved.
    """
    old_fps = state.get("fingerprints", {})
    new_entries: dict[str, list[dict]] = {}
    changed_entries: dict[str, list[dict]] = {}
    resolved_entries: dict[str, list[dict]] = {}

    for label, rows in current.items():
        curr_fp_to_row = {row_fingerprint(r): r for r in rows}
        prev_fp_to_row: dict[str, dict] = old_fps.get(label, {})

        curr_fp_set = set(curr_fp_to_row.keys())
        removed_fps = set(prev_fp_to_row.keys()) - curr_fp_set

        # Index previous records by stable key for change detection
        prev_key_to_fp = {
            row_stable_key(r): fp for fp, r in prev_fp_to_row.items()
        }

        for fp in curr_fp_set - set(prev_fp_to_row.keys()):
            row = curr_fp_to_row[fp]
            old_fp = prev_key_to_fp.get(row_stable_key(row))
            if old_fp in removed_fps:
                # Same identity, different fingerprint → updated record
                changed_entries.setdefault(label, []).append(row)
                removed_fps.discard(old_fp)
            else:
                new_entries.setdefault(label, []).append(row)

        if removed_fps:
            resolved_entries[label] = [prev_fp_to_row[fp] for fp in removed_fps]

    return new_entries, changed_entries, resolved_entries


# ---------------------------------------------------------------------------
# Notifications
# ---------------------------------------------------------------------------


def format_notification(
    new: dict[str, list[dict]],
    changed: dict[str, list[dict]],
    resolved: dict[str, list[dict]],
) -> Optional[tuple[str, str]]:
    """Format notification title and body. Returns None if nothing to report."""
    parts = []

    for label, rows in new.items():
        for r in rows:
            parts.append(
                f"🔴 NEW SHORTAGE: {label}\n"
                f"  Product: {r.get('Arzneimittelbezeichnung', '?')}\n"
                f"  PZN: {r.get('PZN', '?')}\n"
                f"  From: {r.get('Beginn', '?')} → Until: {r.get('Ende', '?')}\n"
                f"  Reason: {r.get('Art des Grundes', '?')}\n"
                f"  Manufacturer: {r.get('Zulassungsinhaber', '?')}"
            )

    for label, rows in changed.items():
        for r in rows:
            parts.append(
                f"🟡 SHORTAGE UPDATED: {label}\n"
                f"  Product: {r.get('Arzneimittelbezeichnung', '?')}\n"
                f"  PZN: {r.get('PZN', '?')}\n"
                f"  From: {r.get('Beginn', '?')} → Until: {r.get('Ende', '?')}\n"
                f"  Reason: {r.get('Art des Grundes', '?')}\n"
                f"  Manufacturer: {r.get('Zulassungsinhaber', '?')}"
            )

    for label, rows in resolved.items():
        for r in rows:
            parts.append(
                f"🟢 SHORTAGE RESOLVED: {label}\n"
                f"  Product: {r.get('Arzneimittelbezeichnung', '?')}\n"
                f"  PZN: {r.get('PZN', '?')}"
            )

    if not parts:
        return None

    n_new = sum(len(v) for v in new.values())
    n_changed = sum(len(v) for v in changed.values())
    n_resolved = sum(len(v) for v in resolved.values())
    title_parts = []
    if n_new:
        title_parts.append(f"{n_new} new")
    if n_changed:
        title_parts.append(f"{n_changed} updated")
    if n_resolved:
        title_parts.append(f"{n_resolved} resolved")
    title = "BfArM Drug Shortage: " + ", ".join(title_parts)

    body = "\n\n".join(parts)
    return title, body


def send_notification(title: str, body: str):
    """Send notification via configured method."""
    log.info("Sending notification: %s", title)

    try:
        if NOTIFY_METHOD == "signal":
            _send_signal(title, body)
        elif NOTIFY_METHOD == "ntfy":
            _send_ntfy(title, body)
        elif NOTIFY_METHOD == "gotify":
            _send_gotify(title, body)
        elif NOTIFY_METHOD == "telegram":
            _send_telegram(title, body)
        elif NOTIFY_METHOD == "smtp":
            _send_smtp(title, body)
        else:
            log.error("Unknown notification method: %s", NOTIFY_METHOD)
    except Exception as e:
        log.error("Failed to send notification: %s", e)


def _send_signal(title: str, body: str):
    message = f"*{title}*\n\n{body}"
    payload = {
        "message": message,
        "number": SIGNAL_SENDER,
        "recipients": SIGNAL_RECIPIENTS,
    }
    resp = requests.post(
        f"{SIGNAL_API_URL}/v2/send",
        json=payload,
        timeout=15,
    )
    resp.raise_for_status()
    log.info("Signal notification sent to %d recipient(s)", len(SIGNAL_RECIPIENTS))


def _send_ntfy(title: str, body: str):
    headers = {"Title": title, "Priority": "high", "Tags": "pill,warning"}
    if NTFY_TOKEN:
        headers["Authorization"] = f"Bearer {NTFY_TOKEN}"
    resp = requests.post(
        f"{NTFY_URL}/{NTFY_TOPIC}",
        data=body.encode("utf-8"),
        headers=headers,
        timeout=10,
    )
    resp.raise_for_status()
    log.info("Ntfy notification sent")


def _send_gotify(title: str, body: str):
    resp = requests.post(
        f"{GOTIFY_URL}/message",
        json={"title": title, "message": body, "priority": 8},
        headers={"X-Gotify-Key": GOTIFY_TOKEN},
        timeout=10,
    )
    resp.raise_for_status()
    log.info("Gotify notification sent")


def _send_telegram(title: str, body: str):
    text = f"*{title}*\n\n{body}"
    resp = requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown",
        },
        timeout=10,
    )
    # Avoid raise_for_status() here: its exception message embeds the URL,
    # which contains the bot token in plain text.
    if not resp.ok:
        raise RuntimeError(f"Telegram API error {resp.status_code}: {resp.text[:200]}")
    log.info("Telegram notification sent")


def _send_smtp(title: str, body: str):
    import smtplib
    from email.mime.text import MIMEText

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = title
    msg["From"] = SMTP_FROM
    msg["To"] = SMTP_TO

    if SMTP_PORT == 465:
        # Direct SSL — used by some providers instead of STARTTLS
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as server:
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
    else:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
    log.info("Email notification sent")


# ---------------------------------------------------------------------------
# Main Loop
# ---------------------------------------------------------------------------


def check_once() -> bool:
    """Run a single check cycle. Returns True if notifications were sent."""
    state = load_state()

    try:
        rows = fetch_csv()
    except requests.RequestException as e:
        log.error("Failed to fetch CSV: %s", e)
        return False

    current_matches = find_matches(rows)
    total_matched = sum(len(v) for v in current_matches.values())
    log.info(
        "Found %d matching rows across %d watch entries",
        total_matched,
        len(WATCH_LIST),
    )

    for label, matched in current_matches.items():
        if matched:
            log.info("  %s: %d active shortage(s)", label, len(matched))
        else:
            log.info("  %s: no active shortages", label)

    # First run: just save state, don't alert
    if state.get("last_check") is None:
        log.info("First run — saving baseline state (no alerts)")
        state["fingerprints"] = {
            label: {row_fingerprint(r): r for r in rows_list}
            for label, rows_list in current_matches.items()
        }
        state["last_check"] = datetime.now(timezone.utc).isoformat()
        save_state(state)

        if total_matched > 0:
            log.warning(
                "⚠️  Existing shortages found on first run (not alerting):"
            )
            for label, matched in current_matches.items():
                for r in matched:
                    log.warning(
                        "  - %s: %s (PZN %s, %s→%s)",
                        label,
                        r.get("Arzneimittelbezeichnung"),
                        r.get("PZN"),
                        r.get("Beginn"),
                        r.get("Ende"),
                    )
        return False

    new, changed, resolved = diff_matches(current_matches, state)
    notification = format_notification(new, changed, resolved)

    if notification:
        title, body = notification
        send_notification(title, body)
        log.info("Changes detected and notification sent")
    else:
        log.info("No changes detected")

    # Update state
    state["fingerprints"] = {
        label: {row_fingerprint(r): r for r in rows_list}
        for label, rows_list in current_matches.items()
    }
    state["last_check"] = datetime.now(timezone.utc).isoformat()
    save_state(state)

    return notification is not None


def main():
    log.info("=" * 60)
    log.info("BfArM Lieferengpass Monitor starting")
    if not WATCH_LIST:
        log.warning("WATCH_LIST is empty — no medications configured. Set WATCH_LIST in .env.")
    log.info("Watching %d medication(s):", len(WATCH_LIST))
    for w in WATCH_LIST:
        log.info("  - %s", w["label"])
    log.info("Check interval: %ds (%dh)", CHECK_INTERVAL, CHECK_INTERVAL // 3600)
    log.info("Notification method: %s", NOTIFY_METHOD)
    log.info("=" * 60)

    while True:
        try:
            check_once()
        except Exception as e:
            log.exception("Unexpected error during check: %s", e)

        log.info("Next check in %d seconds", CHECK_INTERVAL)
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
