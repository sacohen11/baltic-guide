# Operations

## Service responsibilities

- `ingestor`: maintains the external NASA GCN Kafka subscription and commits offsets after durable storage.
- `worker`: publishes/consumes internal Kafka, runs leased agent jobs and the heartbeat scheduler.
- `api`: serves authenticated REST, A2A, SSE and the built frontend.
- `postgres`: durable source history, tasks, agent memory, outbox and graph checkpoints.
- `kafka`: internal routing with a 14-day topic retention policy.
- `caddy` (HTTPS profile): TLS and reverse proxy for public backend deployment.

Use `docker compose logs -f ingestor worker api`, `docker compose ps`, and `/readyz`. `Live streams` distinguishes a quiet subscribed source from a disconnected consumer. A recent empty poll is a connection heartbeat; the last successful source message is tracked separately. Consumer health reports partition lag when available. `/metrics` exposes job queue latency, execution time and outcome counters.

## Restart and recovery

`docker compose restart worker ingestor` preserves all state. Keep `GCN_GROUP_ID` and volumes stable. Expired agent leases are reclaimed automatically; outbox publication retries after failures. Never advance/reset offsets to work around a parser or database error without first reviewing retained raw records.

Malformed or unsupported messages are retained and quarantined, allowing the external partition to progress. Fix the parser against a captured raw record and add a regression test. Reprocessing quarantined rows is intentionally an operator procedure: use `parse_record`, then emit a normalized observation through `Service.emit` with a new replay event ID. Do not edit facts or fabricate original source timestamps. No unauthenticated ingestion endpoint exists.

## Backups and retention

Back up PostgreSQL regularly and test restores. For example, run `pg_dump` inside the database container and direct its output to a protected backup file. Encrypt/off-site-copy backups because they include observer credential hashes and task questions. Database backups retain raw payloads, evidence, agent state, inbox/outbox and source offsets recorded with raw receipts. Broker consumer-group offsets live in Kafka separately.

Raw records, evidence, completed jobs and graph checkpoints currently retain indefinitely. Monitor disk use and define an archive/retention policy before long-running high-volume ingestion. The application deliberately does not silently delete scientific history. For high availability, replace the single-node database/broker with managed or replicated services and configure the internal Kafka TLS/SASL settings. Use PostgreSQL for concurrent production workers; SQLite is for isolated development/tests.

## Secrets and browser access

Keep `.env` private and out of Git. Use a separate long admin token and random database password. Observer credentials are restricted to their own tasks, preferences, and messages. Scientific source/case data are shared between authenticated observers. All HTTPS users must trust the frontend because runtime access tokens live in browser session storage.

Set `CORS_ORIGINS` to exact frontend origins, not URLs containing paths. For Pages, `https://sacohen11.github.io` is the origin even when the app lives at `/baltic-guide/`. Set `PUBLIC_URL` to the HTTPS backend origin so A2A cards advertise reachable endpoints. Reverse proxies must permit long-lived SSE and disable response buffering.

API rate limiting is per API process. If deploying multiple API replicas or exposing this to untrusted users, enforce a shared edge rate limit. TLS is supplied by Caddy in the provided public deployment profile. Do not expose internal Kafka/PostgreSQL ports to the internet.

## Deployment verification

1. CI: backend tests and lint, frontend tests/build, Docker build, and a real Kafka/PostgreSQL restart/retraction integration test.
2. Pages: successful separate deployment job with its reported URL. A source-code push is not itself proof of a Pages deployment.
3. Backend: `/healthz` succeeds, then `/readyz` succeeds once workers and the external consumer are connected.
4. Open the dashboard and confirm source subscriptions and broker lag. Wait for a real alert or explicitly replay available historical GCN offsets under a new consumer group.
5. Confirm the case links to the original retained Kafka message, and model interpretation (if configured) is separated from the source evidence.

Without a backend host and valid GCN credentials, a deployed frontend can offer the connection screen but cannot be described as a live observatory.
