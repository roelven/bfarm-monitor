# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A single-file Python service that monitors the BfArM (German Federal Institute for Drugs) drug shortage database and sends push notifications when shortages appear or resolve for watched medications. The entire logic lives in `monitor.py`.

## Running locally

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in WATCH_LIST and credentials
python monitor.py
```

## Tests

```bash
pip install -r requirements-dev.txt
pytest            # run all tests
pytest -v -k diff # run only tests matching "diff"
```

## Docker

```bash
docker compose up -d          # build and start
docker compose logs -f        # follow logs
docker compose down -v        # stop and wipe state (forces fresh baseline on next start)
```

## Sensitive configuration

Medications (`WATCH_LIST`) and notification credentials live in `.env` (gitignored). `.env.example` is the committed template. `docker-compose.yml` loads `.env` via `env_file` and only contains non-sensitive tunables (`CHECK_INTERVAL`, `LOG_LEVEL`, `REQUEST_TIMEOUT`).

## Architecture

`monitor.py` is a single polling loop with five concerns:

1. **Fetch** — downloads the BfArM CSV (semicolon-delimited, Latin-1 encoded) from a public endpoint
2. **Match** — filters rows against `WATCH_LIST` using case-insensitive substring matching on `Wirkstoffe` (active ingredient) and `Arzneimittelbezeichnung` (drug name) columns
3. **Diff** — compares current matches against the previous run using SHA-256 fingerprints of key row fields; first run saves baseline without alerting. A separate `row_stable_key` (Bearbeitungsnummer/PZN/ENR) detects updates to existing records so they fire as "changed" rather than a spurious new+resolved pair
4. **Notify** — dispatches notifications via one of: Signal (via signal-cli-rest-api), ntfy, Gotify, Telegram, or SMTP
5. **State** — persists fingerprint state to `/data/state.json` between runs

## Configuration (all via environment variables)

| Variable | Default | Notes |
|---|---|---|
| `CHECK_INTERVAL` | `21600` | Seconds between checks (6h) |
| `LOG_LEVEL` | `INFO` | |
| `STATE_FILE` | `/data/state.json` | Persist across restarts via Docker volume |
| `NOTIFY_METHOD` | `ntfy` | `signal` \| `ntfy` \| `gotify` \| `telegram` \| `smtp` |
| `WATCH_LIST` | 3 ADHD meds | JSON array of `{label, match_wirkstoffe, match_name}` objects |

See `docker-compose.yml` for the full list of notification-specific variables per method.

## Key behaviours to preserve

- **First run is silent**: on first run with no saved state, the monitor saves the current state as baseline and logs (but does not alert on) existing shortages.
- **Fingerprint-based diffing**: a row's fingerprint covers PZN, ENR, Bearbeitungsnummer, Meldungsart, Beginn, Ende, last-update date, and reason type — changes to any of these fields appear as a new entry.
- **Broad matching by design**: matching on `Wirkstoffe` (active ingredient) intentionally catches all brands with the same active ingredient, not just the one named in `label`.
