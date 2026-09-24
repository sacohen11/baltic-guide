# API and A2A

Send the `A2A-Version: 1.0` header on A2A protocol requests; the SDK interprets a missing header as the older 0.3 protocol.

Interactive OpenAPI documentation: `/docs`. All `/api/*` and `/a2a/*` requests require `Authorization: Bearer <token>`. Health probes are public. CORS allows only configured origins, including the Pages origin when deployed.

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/api/overview` | Live ingestion, workers, queue and data counts |
| GET | `/api/agents` | Permanent hierarchy, memory, heartbeat and run state |
| GET | `/api/cases?before=<timestamp>&limit=50&search=...` | Recent case files |
| GET | `/api/cases/<case-id>` | Current evidence, case memory and revision history |
| GET | `/api/sources` | Per-topic message counts, errors and subscription health |
| GET | `/api/messages?before=<sequence>&limit=50` | Observer-specific briefing history |
| GET | `/api/stream?after=<sequence>` | Authenticated SSE with sequence cursor |
| PATCH | `/api/messages/<id>` | `{"read":true,"saved":true}` on an owned current message |
| GET / PUT | `/api/preferences` | Families, instruments, multi-messenger filter |
| POST | `/api/tasks` | Question for an agent; optional child verification |
| GET | `/api/tasks` | Own recent tasks |
| GET | `/api/tasks/<id>` | Own task state and structured answer |
| POST | `/api/tasks/<id>/cancel` | Cancel own pending work |
| POST | `/api/observers` | Admin-only creation; body `{"name":"Researcher"}`; returns token once |
| GET | `/api/deadletters` | Admin-only quarantine diagnostics |
| GET | `/api/raw/<id>` | Original GCN topic/partition/offset/payload |
| GET | `/healthz` | Process/database liveness |
| GET | `/readyz` | Requires a recent worker heartbeat and connected GCN ingestor |
| GET | `/metrics` | Authenticated Prometheus agent metrics |

Example task body:

```json
{"agent_id":"sky","message":"What changed in gravitational-wave evidence?","families":["gravity"],"verify":true,"request_id":"caller-generated-unique-id"}
```

Task creation is idempotent within observer + agent + request ID. Poll the returned task ID or consume briefing SSE. Without a model, the answer is structured source evidence and explicitly states that model analysis is unavailable.

Each permanent agent exposes `/a2a/<agent-id>/.well-known/agent-card.json`, JSON-RPC at `/a2a/<agent-id>/`, and REST routes provided by the pinned official A2A SDK. Protocol version is 1.0. It supports send, stream, task lookup/list/cancel and subscription. Push-notification configuration and task continuation are explicitly unsupported. Task reads/cancellation enforce ownership. Internal child messages retain their A2A envelope for audit.

User-facing content is rendered as text, never trusted HTML. GCN credentials, model credentials, and observer tokens are not returned by status APIs. Administrator-created observer credentials are returned only on creation, with only their hash stored in the database.
