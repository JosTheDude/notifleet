# Notifleet — agent handoff

Applies to the entire repository. This is portable project context for coding assistants; read `README.md` for the full API, configuration, and operations contract.

## Purpose and scope

Notifleet is a simple, configurable, self-hosted notification API/webhook service. Applications submit a message to a named route; the service fans out to every configured destination in that route. It is not round-robin load balancing.

- Supported destinations: Discord, Slack, Pushover, Telegram, and ntfy (including self-hosted ntfy).
- Runs continuously on a machine/NAS/VPS, natively or with Docker Compose.
- Intended for modest personal/team volume in one trust domain, with one process and one durable queue directory.
- User priorities: simplicity, clean implementation, easy configuration, security, and efficient use of time/credits.
- User explicitly prefers **TOML configuration**. API requests, receipts, and queue records remain **JSON**.
- RSS/Atom feed polling is implemented (`feeds.go`, `[feeds.*]` config): each feed polls an HTTPS RSS 2.0 or Atom URL on its own interval and enqueues one notification per new item to a "fleet" (an existing `[routes]` target). Terminology: **feeds** are inbound notification sources, **fleets** are the existing route/destination fan-out targets they push into — this is additive, not a rename of `routes`/`destinations`.
- No frontend, user accounts, multi-tenancy, Redis, external database, arbitrary vendor webhook adapters, attachments, or high-availability cluster. Add scope only when requested.

## Stack and ownership

Go 1.26.7+; module `notifleet`, single `package main`. The only application library dependency is `github.com/pelletier/go-toml/v2`, pinned in `go.mod`/`go.sum`.

| File | Responsibility |
| --- | --- |
| `main.go` | CLI flags, HTTP listener, worker lifecycle, healthcheck command, graceful shutdown |
| `config.go` | Strict TOML loading, defaults, env secret resolution, destination/route validation, destination fingerprints |
| `server.go` | JSON API, bearer auth, rate limiting, message validation, sanitized receipts |
| `providers.go` | Provider payloads/responses, outbound HTTPS client, IP restrictions, retry hints |
| `queue.go` | Atomic disk persistence, exclusive directory lock, retention, single delivery worker, retry budgets/cooldowns |
| `feeds.go` | RSS/Atom fetch + parse, per-feed dedup state on disk, polling scheduler, feed-to-fleet enqueue |
| `*_test.go` | Configuration, API, provider, queue, security-boundary and local HTTP/TLS tests |
| `config.example.toml` | Single config reference: minimal active Discord setup, every other option (server settings, other providers, feeds) present but commented out with explanation |
| `Dockerfile`, `compose.yaml` | Non-root container and local persistent deployment |
| `.env.example` | Secret variable names, never real credentials |

## Public interface

- `POST /v1/notify`: `{"route":"default","title":"optional","message":"required"}`.
- `POST /v1/webhooks/{route}`: same body, route fixed by path; conflicting body route is rejected.
- `GET /v1/notifications/{id}`: authenticated receipt; states `pending`, `delivered`, `partial`, `failed`.
- `GET /healthz`: unauthenticated process/storage health, not provider availability.
- Other endpoints require `Authorization: Bearer <key>`. All configured keys have equal access.
- `202` means durably queued, not delivered. Provider acceptance does not prove a human read the message.
- CLI: `-config config.toml` (default), `-check` (validate without delivery), `-healthcheck`.

## Invariants to preserve

- Secrets use `env:VARIABLE` references, resolved after TOML parsing. Native execution requires exported env vars; Compose reads `.env`. Never log/commit credentials or notification content.
- Reject unknown TOML fields, duplicate keys/tables, invalid types, missing secrets, bad routes and oversized config. Keep the 1 MiB config and 32 KiB request limits and provider-specific message limits; do not silently truncate.
- Callers select configured routes, never arbitrary destination URLs. Preserve HTTPS certificate validation, host validation, dial-time IP checks, disabled environment proxies and redirect refusal.
- Private network access is opt-in only for a trusted self-hosted ntfy destination; HTTPS remains required.
- Preserve Discord mention suppression and Slack plain-text/escaped delivery behavior.
- Queue acceptance requires atomic write plus file/directory sync. Exclusive locking prevents concurrent writers. Storage failures stop delivery rather than continue with unrecorded state.
- Delivery has bounded retries and possible duplicates after ambiguous failures. No ingress idempotency keys or exactly-once guarantee. Successful destinations are not resent because another destination fails.
- Retry budgets and 429 cooldowns survive restart. Do not redirect pending notifications when destination configuration changes: fingerprints protect against this.
- Preserve `Destination` JSON tags/serialization: existing queue fingerprints depend on them even though configuration is TOML.
- Feed fetches reuse the same dial-time public-IP-only, TLS 1.2+, no-redirect delivery client as provider sends (`newDeliveryClient(false)`); feeds have no `allow_private_network` escape hatch. `feedURL` requires HTTPS with no embedded credentials/fragment (query strings and non-443 ports are allowed, unlike `secureURL`, since real feed URLs commonly use them).
- A feed's first poll after (re)creating its state only primes the seen-item set; it must not notify a backfill of pre-existing items. Items are marked seen only after a successful `q.enqueue`, so a crash mid-poll can duplicate a notification on the next poll but never silently drops one. Feed dedup state persists per feed at `<data_dir>/feeds/<name>.json` via the same atomic temp-file/rename/fsync pattern as queue jobs.
- Queue files contain plaintext message content. Retention deletes terminal jobs; pending work must not be silently purged. Never delete live data to fix a test or deployment problem.
- Compose keeps a persistent volume, localhost-only host port, read-only root/config, non-root user, dropped capabilities, and no-new-privileges. Remote exposure requires HTTPS proxy/VPN.

## Quick start

```sh
cp config.example.toml config.toml
cp .env.example .env
chmod 600 .env
openssl rand -hex 32
# Put the generated value in NOTIFLEET_API_KEY in .env.
# Fill DISCORD_WEBHOOK_URL, or configure other destinations/routes in TOML.
docker compose up -d --build
curl --fail http://127.0.0.1:8080/healthz
docker compose logs --tail=100 notifleet
```

`docker compose down` preserves the queue; `down -v` destroys it. Secret changes require container recreation. For native execution, use `listen = "127.0.0.1:8080"`, `data_dir = "./data"`, export secrets, then `go run . -check` or `go run .`.

## Development and verification

Keep changes small and within the owning module. Do not add frameworks, abstractions, or services without a concrete need. Preserve unrelated work. Update examples/docs with configuration or API changes. Do not send real notifications, expose services publicly, or deploy/push without authorization.

```sh
gofmt -w <changed-go-files>
go test -race -cover ./...
go vet ./...
go mod verify
go run golang.org/x/vuln/cmd/govulncheck@latest ./...
go build ./...
```

Use mock providers or local HTTP/TLS fixtures. Test wrong inputs and boundary cases, not just happy paths. Configuration changes must preserve JSON ingress and existing queue compatibility. Dependency changes require updated checksums and Docker build inputs; `.dockerignore` intentionally excludes secrets/config/data.

## Current handoff status and limitations

TOML migration is complete. Race-enabled tests, vet, module verification, native TOML startup/healthcheck/JSON acceptance, Compose definition validation, and Linux amd64/arm64 builds passed. The last vulnerability scan reported no vulnerabilities; rerun after changes rather than treating this as permanent assurance.

Docker runtime startup was **not verified** because the local Docker daemon was stopped. Real-provider sends were **not verified** without credentials. No production deployment has been performed. Do not claim these checks passed.

One worker sends sequentially; a slow provider can delay others. Queue state is held in memory as well as on disk. Pushover clients require a paid license after trial; public ntfy topics are not access-controlled merely because their names are random. See README before changing delivery semantics or operational limits.
