# BfArM Lieferengpass Monitor

Monitors the [BfArM drug shortage database](https://anwendungen.pharmnet-bund.de/lieferengpassmeldungen/faces/public/meldungen.xhtml) for supply chain issues affecting specific medications. Sends push notifications when shortages are reported or resolved.

## How it works

1. Every 6 hours (configurable), downloads the full CSV from BfArM
2. Filters for rows matching watched active ingredients or drug names
3. Compares against previous state (fingerprint-based diffing)
4. On first run: saves baseline state, logs existing shortages, no alerts
5. Subsequent runs: alerts on new shortages or resolved shortages

## Quick start

```bash
cp .env.example .env
# Edit .env: set WATCH_LIST and your notification credentials
docker compose up -d
docker compose logs -f  # watch the first run
```

## Notification setup

Set `NOTIFY_METHOD` in `docker-compose.yml` and configure the corresponding variables.

### Signal (via signal-cli-rest-api)

Requires a running [signal-cli-rest-api](https://github.com/bbernhard/signal-cli-rest-api) instance with a linked/registered number. If it's running on the Docker host, the default `host.docker.internal` URL works out of the box.

```yaml
NOTIFY_METHOD: "signal"
SIGNAL_API_URL: "http://host.docker.internal:8080"  # adjust port if needed
SIGNAL_SENDER: "+491701234567"                       # your registered signal-cli number
SIGNAL_RECIPIENTS: '["+491709876543"]'               # JSON array of recipient numbers
```

### ntfy
```yaml
NOTIFY_METHOD: "ntfy"
NTFY_URL: "https://ntfy.sh"          # or your self-hosted instance
NTFY_TOPIC: "my-secret-topic-name"   # pick something unique
```
Then subscribe to the topic in the ntfy app on your phone.

### Gotify
```yaml
NOTIFY_METHOD: "gotify"
GOTIFY_URL: "http://gotify:80"
GOTIFY_TOKEN: "your-app-token"
```

### Telegram
```yaml
NOTIFY_METHOD: "telegram"
TELEGRAM_BOT_TOKEN: "123456:ABC-DEF..."
TELEGRAM_CHAT_ID: "your-chat-id"
```

### Email (SMTP)
```yaml
NOTIFY_METHOD: "smtp"
SMTP_HOST: "smtp.example.com"
SMTP_PORT: "587"
SMTP_USER: "user"
SMTP_PASS: "pass"
SMTP_FROM: "alerts@example.com"
SMTP_TO: "you@example.com"
```

## Customizing watched medications

Override via the `WATCH_LIST` environment variable (JSON):

```yaml
WATCH_LIST: |
  [
    {
      "label": "My Med 10mg",
      "match_wirkstoffe": ["some-active-ingredient"],
      "match_name": ["brand-name"]
    }
  ]
```

Matching is case-insensitive substring matching against the `Wirkstoffe` and `Arzneimittelbezeichnung` CSV columns.

## State & persistence

State is stored in `/data/state.json` (Docker volume `bfarm-data`). To force a fresh baseline, remove the volume:

```bash
docker compose down -v
docker compose up -d
```

## Data source

- **Endpoint:** `https://anwendungen.pharmnet-bund.de/lieferengpassmeldungen/public/csv`
- **Format:** Semicolon-delimited CSV, Latin-1 encoded
- **Update frequency:** Near real-time (minutes after pharma companies submit reports)
- **No authentication required**
