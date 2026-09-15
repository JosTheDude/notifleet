# Notifleet

[![CI](https://github.com/JosTheDude/notifleet/actions/workflows/ci.yml/badge.svg)](https://github.com/JosTheDude/notifleet/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A small, self-hosted notification router. Applications send one authenticated JSON request; Notifleet queues it durably and fans it out to the destinations in a named route.

**One Go service with TOML configuration.** No database server, Redis, frontend, or account system. Runs continuously on your own machine, NAS, or VPS, either as a native Linux/macOS process or in Docker. The only application library dependency is `go-toml/v2`, compiled into the binary.

## Supported destinations

| Provider | Configuration | Cost considerations |
| --- | --- | --- |
| Discord | Incoming webhook URL | Free; Discord rate limits apply |
| Slack | Incoming webhook URL | Works with free workspaces, subject to Slack limits and app policies |
| Pushover | Application token + user/group key | API quota includes 10,000 messages/month per account; **Pushover clients require a license after the trial** |
| Telegram | Bot token + chat ID | Standard bot messaging is free and rate-limited; paid broadcasting is not enabled |
| ntfy | Server URL + topic, optional token | Public service has free-tier limits; self-hosting is also supported |

Routes **fan out to every listed destination**, rather than round-robin between platforms. Multiple destinations can use the same provider, such as different Discord channels.

## Project layout

Standard Go layout: a thin entrypoint over independently testable internal packages, none of which are importable outside this module.

```text
cmd/notifleet/     entrypoint: flags, HTTP listener, worker/feed lifecycle, shutdown
internal/config/   TOML loading, defaults, secret resolution, validation
internal/server/   JSON API: auth, rate limiting, message validation, receipts
internal/providers/  per-destination requests/responses, hardened HTTP client
internal/queue/    durable job storage, delivery worker, retries
internal/feeds/    RSS/Atom polling and feed-to-fleet notification
configs/           config.example.toml reference
.github/workflows/ CI: gofmt, vet, race tests, build, govulncheck, Docker build
```

Each package under `internal/` has its own `*_test.go` suite alongside it. See `AGENTS.md` for the package dependency graph and what each layer owns.

## Start with Docker Compose

1. Prepare configuration and secrets:

   ```sh
   cp configs/config.example.toml config.toml
   cp .env.example .env
   chmod 600 .env
   openssl rand -hex 32
   ```

2. Put the generated value in `NOTIFLEET_API_KEY` in `.env`. Set `DISCORD_WEBHOOK_URL` to your Discord webhook URL. The minimal example only enables Discord. Create that webhook in your Discord server's **Integrations → Webhooks** settings.

3. Build and start:

   ```sh
   docker compose up -d --build
   docker compose ps
   curl --fail http://127.0.0.1:8080/healthz
   ```

Startup validates the configuration before accepting requests. To validate separately without starting the service, run `docker compose run --rm --build --no-deps notifleet -config /config/config.toml -check`. For startup errors, use `docker compose logs --tail=100 notifleet`. Stop it with `docker compose down`; the queue volume is preserved.

Compose binds the host port to **loopback only**, runs a non-root, shell-free container with a read-only root filesystem, drops all capabilities, and persists the queue in `notifleet-data`. Config is mounted read-only. The daemon must be running to use Docker.

**Before exposing it remotely, put it behind a TLS-terminating reverse proxy or a private VPN.** Never send bearer keys over public plain HTTP. The service itself speaks HTTP; outbound provider connections require verified HTTPS. For example, an existing host-level Caddy server can use:

```caddyfile
notify.example.com {
    reverse_proxy 127.0.0.1:8080
}
```

Configure your proxy/firewall for connection and request limits, and do not log authorization headers or request bodies. If the proxy is another container, use a private Docker network instead of its loopback interface.

## Send a notification

Export `NOTIFLEET_API_KEY` in your calling application's environment (the CLI does not automatically load `.env`):

```sh
curl --fail-with-body http://127.0.0.1:8080/v1/notify \
  -H "Authorization: Bearer $NOTIFLEET_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"route":"default","title":"Backup complete","message":"Database backup finished successfully."}'
```

```json
{"id":"6b3e82ab13014fbdaa5cfc9c32e953d5","status":"queued"}
```

`202 Accepted` means the payload and destination selection have been written and synced to disk—not that every provider has received the message. The response's `Location` header points to its receipt:

```sh
curl --fail-with-body http://127.0.0.1:8080/v1/notifications/NOTIFICATION_ID \
  -H "Authorization: Bearer $NOTIFLEET_API_KEY"
```

Receipts report `pending`, `delivered`, `partial`, or `failed`, with each destination's state, attempt count, and sanitized result code. `delivered` means **accepted by the provider**, not read by a person. Receipts never return message contents, endpoint URLs, or credentials.

### Webhook endpoint

An application that supports a configurable webhook body and an authorization header can instead post to:

```text
POST /v1/webhooks/default
Authorization: Bearer <key>
Content-Type: application/json

{"title":"Build finished","message":"Build 42 passed."}
```

The path fixes the route. A conflicting route in the body is rejected. These are **Notifleet-format webhooks**, not arbitrary GitHub, Stripe, or other vendor-specific event envelopes. Those need an upstream adapter that maps the event to `title` and `message`; no unauthenticated URL-secret endpoint is provided.

### RSS and Atom feeds

Notifleet polls RSS 2.0 and Atom feeds and pushes new entries into a fleet. Notifleet does not expose an outbound feed of its own notifications — only polling in.

```toml
[feeds.blog]
url = "https://example.com/feed.xml"
fleet = "default"       # an existing [routes] target
poll_seconds = 900      # 60-86400
# max_items = 10        # cap notifications enqueued per poll (default 20)
```

- `url` must be HTTPS. It may include a query string (common for feed endpoints) but not credentials or a fragment.
- `fleet` must name an existing route; that route's destinations receive one notification per new entry.
- Each feed polls independently on its own `poll_seconds` interval. The first poll after a feed's dedup state is created (fresh install, or its `<data_dir>/feeds/<name>.json` removed) only records existing entries — it does not notify a backfill of everything already in the feed. Only entries seen after that are delivered.
- Deduplication is by entry GUID/id, falling back to link, then title, and persists to `<data_dir>/feeds/<name>.json` so restarts do not resend old entries.
- Entry titles and bodies are truncated and have HTML tags stripped before being sent through the normal `/v1/notify` validation and per-provider length limits; entries that still fail validation are skipped and logged rather than retried forever.
- Feed fetches use the same public-IP-only, TLS 1.2+, no-redirect HTTP client as provider delivery; there is no private-network option for feeds.

If you need to consume a feed some other way (custom filtering, transformation, non-RSS format), an external watcher can still POST directly to `/v1/notify` using the JSON contract below.

### Request contract

- `message`: required, nonblank UTF-8 text. No attachments, HTML mode, or arbitrary provider payloads.
- `title`: optional, at most 250 Unicode characters.
- `route`: optional; defaults to `default`, which must be configured if used.
- Body limit: 32 KiB. Unknown JSON fields, trailing JSON values, invalid UTF-8, NULs, and over-limit messages are rejected before queueing.
- Discord: at most 2,000 UTF-16 units including title and separator; Slack: 3,000; Telegram: 4,096. Non-BMP characters count as two units. Pushover: 1,024 message characters. ntfy: the generated JSON payload must fit in 4,096 bytes. No silent truncation.
- Discord mentions are disabled. Slack uses plain-text blocks and an escaped notification fallback; link unfurling is disabled. Telegram does not enable markup or link previews.

| HTTP status | Meaning |
| --- | --- |
| 202 | Durably queued |
| 400 | Malformed JSON/body or conflicting route |
| 401 | Missing/invalid bearer key |
| 404 | Unknown receipt or endpoint |
| 405 | Wrong HTTP method |
| 413 | Request body too large |
| 415 | `Content-Type: application/json` required |
| 422 | Invalid message, route, or provider length limit |
| 429 | Shared API rate limit reached; observe `Retry-After` |
| 503 | Queue capacity or storage failure; not confirmed accepted |

`GET /healthz` is the only unauthenticated resource. It checks process/storage state, not provider credentials, delivery latency, or provider uptime. It is exempt from the API rate limit. All keys grant equal access to all routes and receipts; this is a **single-trust-domain service**, not multi-tenant hosting.

## Configure providers and routes

Use `configs/config.example.toml` as a reference: it ships with one active Discord destination/route and every other option — server settings, the other providers, and feeds — present but commented out with an explanation of what it does and its valid range. Uncomment only what you need into `config.toml`, fill in secrets/settings, and add destination names to routes:

```toml
[routes]
default = ["discord", "slack"]
urgent = ["discord", "phone", "telegram"]
personal = ["ntfy"]
```

Configuration is TOML only; old JSON configuration files must be converted, not just renamed. API requests, receipts, and queue records remain JSON. Keep top-level settings (such as `api_keys` and `max_attempts`) before any table headers. Comments are supported; unknown fields, duplicate keys/tables, invalid types, and files larger than 1 MiB are rejected. Existing queue records remain compatible when destination values are unchanged.

The example includes placeholder chat IDs and topics; replace them. Names use 1–64 letters, digits, underscores, or hyphens. Empty routes, unknown destinations, duplicates within a route, and unknown providers fail startup.

Secrets (`api_keys`, `webhook_url`, `token`, `user`) **must** be environment references such as `env:DISCORD_WEBHOOK_URL`. They are resolved after TOML decoding, without shell expansion. Missing secrets fail startup. Do not put actual tokens in TOML or commit `.env`; Compose loads `.env`, while native execution uses exported environment variables.

- **Discord:** use `https://discord.com/api/webhooks/...`. Discord is called with `wait=true` to wait for message acceptance.
- **Slack:** create an app with Incoming Webhooks enabled, authorize it for a channel, and use its `https://hooks.slack.com/services/...` URL.
- **Pushover:** register an application at [pushover.net](https://pushover.net/apps/build), then use its app token and your user/group key. Delivery uses normal priority; emergency acknowledgement workflows are not implemented.
- **Telegram:** create a bot with BotFather. Start a conversation with it or add it to the intended group/channel with posting permission, then set the chat ID. The ID is a quoted string, for example `chat_id = "-10042"`; negative group/channel IDs are supported.
- **ntfy:** use `https://ntfy.sh` or your own HTTPS server root. Set `topic` and optionally `token = "env:NTFY_TOKEN"`. **Public ntfy topics are not private or access-controlled merely because the name is random.** Use authenticated self-hosting or a protected topic for sensitive messages. A random, unguessable topic is the minimum for nonsensitive public-service use.

For self-hosted ntfy on a private network, explicitly set `allow_private_network = true` on **that destination only**. HTTPS with a trusted certificate on port 443 is still required. This disables the outbound IP restriction for that destination; the administrator must trust its hostname and DNS. Private access is not available for other providers. URL credentials, redirects, query strings, and non-root ntfy paths are rejected or not followed.

### Server settings

| Setting | Default | Range/meaning |
| --- | --- | --- |
| `listen` | `127.0.0.1:8080` | HTTP bind address; Docker example uses `0.0.0.0:8080` inside the container |
| `data_dir` | `./data` | Dedicated local queue directory; Docker example uses `/data` |
| `api_keys` | Required | 1–16 env references; values 32–512 bytes, no whitespace; generate random keys |
| `max_jobs` | 10000 | 1–100000; includes pending jobs **and retained terminal receipts** |
| `max_attempts` | 5 | 1–10 attempts per destination |
| `retention_hours` | 24 | 1–720 hours after all destinations finish |
| `requests_per_minute` | 120 | 1–60000; shared token bucket across all keys and authenticated endpoints, with this many burst tokens |

Configuration is loaded only at startup. Use `-check` to validate without opening the queue or contacting providers, then restart. For Compose secret changes use `docker compose up -d --force-recreate`—a plain restart does not reload environment values.

For API key rotation, temporarily list both old and new env references, recreate the service, migrate callers, then remove the old reference and recreate again. Provider destination configuration is fingerprinted when a job is accepted. Changing any destination field (including credentials), or removing it, causes its pending deliveries to fail with `destination_changed` rather than send to a potentially different recipient. **Drain pending jobs before changing provider settings.** Changing route membership only affects new jobs.

## Delivery, storage, and operations

- One worker sends one provider request at a time, with a 15-second timeout. A slow attempt can delay other destinations, but does not hold the ingress/status lock. This is designed for modest personal/team notification volume, not high-throughput messaging or high availability.
- Network failures, HTTP 408/429, and 5xx responses retry with exponential backoff and jitter. Other errors are terminal. `Retry-After` dates/seconds and Discord/Telegram retry fields are honored, capped at 24 hours. A 429 pauses other queued requests to the same destination configuration; cooldowns survive restart.
- Successful destinations are not retried merely because another destination failed. Provider bodies and URLs are not copied into error logs or receipts.
- Delivery is **at least once within a bounded retry budget**, not guaranteed or exactly once. If the provider accepts a message but its response or the local completion write is lost, a retry may duplicate it. Attempts are saved before sending, so interrupted attempts consume budget. No ordering guarantee is made across destinations or retries.
- Ingress has **no idempotency key**. Retrying a POST after an ambiguous connection failure may create another job. Store the returned ID and poll its receipt when available. A storage error around the final sync can leave an ambiguously accepted job; operators should inspect storage before retrying en masse.
- Completed payloads and receipts expire together after retention. Pending jobs are never purged for age. Failed jobs are retained for inspection, not automatically replayed; submit a new request after correcting the cause, accounting for destinations that already succeeded.
- The queue is bounded but stored and indexed in memory. Size limits and retention must fit your disk/RAM. A full queue returns 503 even if the capacity is occupied by completed receipts. Lower retention or raise capacity deliberately; do not delete live queue files to make space.
- The data directory uses mode 0700; queue records use 0600 and atomic rename with file/directory sync. A file lock prevents two processes from sharing it. Use a dedicated **local filesystem**, not NFS or a shared volume; do not run multiple replicas against one directory.
- Queue files contain plaintext notification content and destination fingerprints, but not the provider secrets themselves. Use encrypted storage and protected backups for sensitive messages. Do not let untrusted users modify the configuration or queue directory.
- On SIGTERM/SIGINT, the HTTP server stops accepting requests and the worker stops; queued work resumes after restart. Corrupt records fail startup instead of being silently discarded. Storage write failures mark the service unhealthy and stop processing.
- Structured JSON logs report job IDs, destination names, state, attempt, and sanitized error codes. Inspect them with `docker compose logs --tail=100 notifleet`. Monitor disk/RAM and terminal failures in addition to container health.

To back up, stop the service, back up the data volume plus configuration/secrets separately, then restart. Restore while stopped. Restoring old queue state may resend already-delivered notifications. `docker compose down` preserves data; **`docker compose down -v` destroys the queue volume**.

## Run and test without Docker

Requires Go 1.26.7+ and Linux/macOS. The minimum includes standard-library security fixes; older Go installations with automatic toolchain selection enabled will download it. For native use, set `listen` to `127.0.0.1:8080` and `data_dir` to `./data` in your copied configuration. Export the configured environment variables, then:

```sh
go test -race -cover ./...
go vet ./...
go run golang.org/x/vuln/cmd/govulncheck@latest ./...
go build -trimpath -o notifleet ./cmd/notifleet
./notifleet -config config.toml -check
./notifleet -config config.toml
```

Tests use controlled HTTP/TLS servers and mock provider responses; they do not send real notifications. They exercise authentication, validation, fan-out, receipts, private-address blocking, redirect refusal, rate limits, retention, restart recovery, configuration changes, and storage failures. Real-provider smoke tests require your credentials and a test destination.

Security controls reduce risk; they are not a security audit or a guarantee against defects. Keep Go and container images patched, protect the host and secrets, and perform a deployment-specific review before Internet exposure.

Provider references: [Discord](https://docs.discord.com/developers/resources/webhook#execute-webhook), [Slack](https://docs.slack.dev/messaging/sending-messages-using-incoming-webhooks/), [Pushover](https://pushover.net/api), [Telegram](https://core.telegram.org/bots/api#sendmessage), [ntfy](https://docs.ntfy.sh/publish/).
