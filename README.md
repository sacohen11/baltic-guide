# Baltic Guide

A working event-driven application for tourist guides: **30 city agents, three country agents, and one Baltic coordinator**, with a live React dashboard, persistent LangGraph workflows, Kafka transport, and A2A task endpoints.

An agent is a durable identity and state, executed by shared workers. Idle agents consume no model tokens. Warm consumers keep listening; incoming observations wake the appropriate agents without waiting for their maintenance heartbeat.

## Run the application

Requires Python 3.12+, [uv](https://docs.astral.sh/uv/), and Node 24+.

```sh
cp .env.example .env
uv sync --frozen --extra dev
cd frontend
npm ci
npm run build
cd ..
uv run baltic demo
uv run baltic serve
```

Open **http://localhost:8000**. The seeded scenario is explicitly illustrative, uses example.org source links, and needs no external credentials. The app also has a “Load scenario” button. Try the cancellation scenario, inspect a city agent, or ask the coordinator to verify a question across selected cities.

Alternatively, with Docker:

```sh
docker compose -f compose.demo.yaml up --build
```

Open the dashboard and load the scenario. Stop with `docker compose -f compose.demo.yaml down`; the named volume preserves data. The local mode uses SQLite and a transactional local event transport. It runs the same agent workflows and schemas as Kafka mode, but is not a Kafka emulator.

## What is implemented

- Configuration-based roster of 34 agents, 30 dedicated city topics, national inputs, and country/Baltic findings.
- Kafka consumers, transactional inbox/outbox, manual offset commits, duplicate suppression, per-agent leases, fencing, retries, and dead letters.
- Separate FastAPI, worker, and connector processes; Postgres and Kafka Docker deployment.
- Three LangGraph workflows with persistent checkpoints, scoped factual memory, bounded delegation, and optional model assessments and answers.
- RSS/Atom, JSON-LD events, mapped JSON APIs, and non-recurring ICS connectors with source health, conditional requests, size limits, and backoff.
- Versioned evidence, source conflicts, out-of-order protection, cancellations, event expiry, and recommendation retraction.
- Date-overlap theme detection: at least three cities for a country theme or two countries for a Baltic theme.
- Official A2A SDK HTTP endpoints for every agent: discovery, send/get/list/cancel tasks, streaming updates, structured result artifacts.
- Guide-specific preferences, token authentication, live SSE inbox, source links, save/acknowledge/dismiss/mute/verification actions, and reconnect cursors.
- Health/readiness endpoints, Prometheus metrics, optional OpenTelemetry export, tests, CI, and deployment manifests.

## Enable real information

The shipped source registry includes enabled LSM culture/transport/weather, ERR News, and LRT English feeds. Scheduled fetching is off in the demo to keep the scenario isolated. Set `ENABLE_CONNECTORS=true` and restart the embedded server, or run `uv run baltic connectors` as a separate process. Source rows show actual fetch health and freshness.

The LSM culture, ERR News, and LRT English URLs were fetched and parsed during implementation. This proves those endpoints returned usable feeds at that time; it does not establish complete coverage of all cities. News city mentions can wake city agents, with a visible “mentioned in source” qualification. They are never treated as confirmed event venues or used as event-theme evidence.

City calendars and the Visit Estonia API are registered but disabled until publisher access and response parsing are configured. Use [source onboarding](docs/sources.md) to add authoritative local calendars. You can also submit validated observations through the ingestion API. The 30-city roster is the seed set from the application plan; it is not a newly audited official population ranking.

## Configure model reasoning

Set `LLM_MODEL`, `LLM_API_KEY`, and optionally `LLM_BASE_URL` in `.env`. The adapter accepts a compatible `/chat/completions` endpoint. No model is selected implicitly.

The model provides a bounded agent assessment only when its evidence snapshot changes, and answers guide questions. Evidence-backed cards and theme eligibility use deterministic rules. A shared daily token reservation limit (`LLM_DAILY_TOKEN_BUDGET`) prevents unbounded calls; reservations are deliberately conservative and are not billing measurements. A failed model call or exhausted budget falls back to an evidence-only answer. Idle unchanged heartbeats do not call the model. Source content is passed as untrusted evidence; agents have no shell, booking, or messaging tools.

## Kafka and Postgres deployment

```sh
cp .env.example .env
# Set distinct random ADMIN_TOKEN (24+ characters) and POSTGRES_PASSWORD in .env.
# Use a URL-safe generated database password, e.g. secrets.token_hex(32).
docker compose up --build -d
# Optional scheduled live ingestion:
docker compose --profile live up -d
```

The production Compose file forces `DEMO_MODE=false`, `TRANSPORT=kafka`, and a separate worker. Enter the admin token in the dashboard, or create individual guides using `POST /api/guides`. The returned guide token is shown once and stored only as a hash server-side. The browser retains the entered token in session storage.

This Compose deployment is one broker and one database, suitable for a single host pilot. For high availability use managed/redundant Kafka and Postgres, TLS, backups, an HTTPS gateway, and the Kubernetes template in `deploy/k8s/`. Configure `PUBLIC_URL` for externally usable A2A Agent Cards. See [operations](docs/operations.md) for exact deployment and recovery guidance.

## Development and verification

```sh
uv run ruff check backend scripts
uv run pytest -q
cd frontend
npm test
npm run build
```

Backend tests cover event delivery, replay/deduplication, cancellation/retraction, time overlap, exclusive leases and fencing, restart, tenant isolation, official SDK round trips, live background workers, bounded delegation, and connector parsing. Frontend tests exercise filtering, preferences, card actions, queries, and agent inspection.

The optional integration test uses real Kafka and Postgres:

```sh
TEST_DATABASE_URL=postgresql+psycopg://USER:PASSWORD@localhost:5432/baltic_test \
TEST_KAFKA_BOOTSTRAP=localhost:19092 \
uv run pytest -q backend/tests/test_integration.py
```

**Use a disposable database**: that test resets application tables, and requires `test` in the database name. CI runs it with dedicated service containers. Docker/Kafka/Postgres integration could not be executed in the implementation workspace because those runtimes were unavailable. Local background-worker tests use real SQLite transactions and the local transport. Model behavior is tested through a mock compatible endpoint; no paid provider call is claimed.

See [architecture](docs/architecture.md), [API examples](docs/api.md), and [operations](docs/operations.md). OpenAPI is served at `/docs`; JSON Schemas are checked into `config/schemas.json`.

## Boundaries

This is a runnable application with deployment code, not an already hosted service. It does not claim universal municipal coverage, semantic/geospatial accuracy, a measured sub-second latency SLO, or external organizer verification. “Verify” checks the agents' currently stored source evidence. A2A supports new tasks rather than task continuation, and does not advertise push notifications. Current evidence is archived in the database; a separate object store, external schema registry, OIDC identity provider, and semantic vector search are not required or included. See the operations guide for capacity and retention limits.
