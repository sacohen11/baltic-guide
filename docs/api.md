# API and A2A examples

Interactive OpenAPI: `http://localhost:8000/docs`. In production send `Authorization: Bearer <guide-token>` for guide operations and the admin token for ingestion/source control. The following examples use local demo authentication, so no token is needed. These payloads describe fictional examples.

## Ingest an observation

`POST /api/observations` accepts the Observation JSON Schema. Send external data here or to `baltic.ingest.raw.v1`; city topics carry internal canonical references, not raw publisher records.

```sh
curl http://localhost:8000/api/observations \
  -H 'Content-Type: application/json' \
  --data '{"source_id":"organizer-example","source_item_id":"market-2027","entity_id":"market-2027","country":"lv","city_ids":["lv:riga"],"kind":"event","title":"Illustrative winter market","summary":"An example for API testing.","source_url":"https://example.org/market","starts_at":"2027-12-01T10:00:00+02:00","ends_at":"2027-12-23T20:00:00+02:00","status":"scheduled","tags":["christmas"],"authoritative":true,"illustrative":true}'
```

To correct/cancel an event, resend the same source identity and entity ID, with a newer `updated_at` and changed status/fields. `observed_at` is fetch time, never the event's occurrence time. Only mark a source authoritative where the publisher has that role. Use `replay:true` for a backfill that should rebuild state without creating new alerts.

## Guide request

```sh
curl http://localhost:8000/api/query \
  -H 'Content-Type: application/json' \
  --data '{"message":"What changed for my tour?","city_ids":["ee:tallinn","lv:riga","lt:vilnius"],"verify":true,"request_id":"my-unique-request-1"}'
```

Returns HTTP 202 with `task_id`. Poll `GET /api/tasks/{task_id}` or listen to `GET /api/stream`. Reuse `request_id` to retry acceptance idempotently; use a new ID for a new question. `POST /api/tasks/{task_id}/cancel` cancels pending/running work and descendants. An in-flight model HTTP call may finish, but a canceled task cannot commit its result.

`GET /api/messages?after=0&limit=100` returns cards ordered by durable sequence. The SSE stream accepts `Last-Event-ID` or `?after=123`. It uses `event: message`, `id: <sequence>`, and JSON `data`. Use an authenticated fetch stream in production; native EventSource cannot attach a bearer header.

`POST /api/messages/{id}/actions` accepts `{"action":"save_to_trip"}` (toggle), `acknowledge`, `dismiss`, `read`, `mute_theme`, or `request_verification`. Verification checks stored evidence and does not contact an organizer. It returns a task ID.

`PUT /api/preferences` replaces preferences with `city_ids`, `countries`, `interests`, `muted_tags`, `starts_at`, `ends_at`, and `language`. Preferred language applies to model answers; deterministic fallback/source excerpts retain source text. Saving a card records it for the guide; it does not make a booking or compute a route.

## A2A 1.0

Agent Card: `/a2a/city:lv:riga/.well-known/agent-card.json`. The mounted SDK also advertises its supported REST interface. JSON-RPC example:

```sh
curl http://localhost:8000/a2a/city:lv:riga/ \
  -H 'Content-Type: application/json' -H 'A2A-Version: 1.0' \
  --data '{"jsonrpc":"2.0","id":"rpc-1","method":"SendMessage","params":{"message":{"messageId":"guide-msg-1","role":"ROLE_USER","parts":[{"text":"What is happening in Rīga?"}]},"configuration":{"returnImmediately":true}}}'
```

The official SDK defines the wire field names/enums; the repository tests round-trip these using its protobuf types. Use `GetTask` with `{"id":"TASK_ID"}` to retrieve a task. Streaming and subscription methods return SDK SSE events. New tasks are supported; continuation of an existing task ID is rejected explicitly. Push notification configuration is unsupported and not advertised. Supply the same bearer token rules as the guide API.

## Administration

- `POST /api/guides` with `{"name":"Tour guide"}` creates an owner and returns its bearer token once.
- `GET /api/sources` shows configured sources and observed health; `POST /api/sources/{id}/poll` polls one enabled source.
- `GET /api/agents`, `/api/agents/{id}`, `/api/overview` inspect current agents and operational state.
- `GET /api/facts` and `/api/evidence/{id}` inspect shared source knowledge; tasks/messages remain private per guide.
- `GET /api/admin/deadletters` shows failed work to an administrator.
- `GET /healthz` checks database connectivity; `/readyz` checks a recent worker heartbeat. These are not source-freshness or Kafka lag guarantees.
