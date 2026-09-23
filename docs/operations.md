# Operations

## Processes and configuration

- `baltic serve`: API, A2A, dashboard, optionally embedded runtime/connectors.
- `baltic worker`: warm Kafka consumer, outbox publisher, shared agent workers, maintenance scheduler.
- `baltic connectors`: independent source scheduler.
- `baltic init`: idempotent schema v1/agent/source initialization.
- `baltic demo`: queues and processes fictional observations using local transport only.
- `baltic schemas`: emits versioned API JSON Schemas.

Production uses `DEMO_MODE=false`, a random `ADMIN_TOKEN` of at least 24 characters, Postgres, and Kafka. Never expose demo authentication publicly. A guide has read access to shared tourism facts and only its own tasks/preferences/messages; source mutations require admin. Protect the deployment with HTTPS, request body/rate limits at the edge, and a deployment-specific identity policy. The built-in bearer tokens are appropriate for a controlled pilot; no OIDC/SSO service is included.

Set `PUBLIC_URL` to the external HTTPS origin before using remote A2A clients. Configure Kafka `KAFKA_SECURITY_PROTOCOL`, SASL credentials, and CA path where applicable. Broker topic-creation permissions are currently required at worker startup; topics can be precreated with the required names, but the startup client still lists them. Database credentials need schema creation/migration permissions for application and checkpoint tables.

`WORKERS` is concurrency per runtime process, not the number of agent identities. Start with 4; scale shared worker processes while watching inbox delay, DB load, Kafka lag, and model quota. Keep at least one worker warm. SQLite is for one local application instance; use Postgres before horizontal scaling.

## Kubernetes

`deploy/k8s/app.yaml` deploys API and worker replicas plus an API Service. It assumes externally provisioned Kafka and Postgres. Build/push the Docker image to your registry, replace `baltic-guide:local`, configure `PUBLIC_URL`, and create the `baltic-guide-secrets` Secret with:

- `DATABASE_URL` (SQLAlchemy `postgresql+psycopg://...`)
- `ADMIN_TOKEN`
- optionally `LLM_API_KEY`, `KAFKA_USERNAME`, `KAFKA_PASSWORD`

Set broker addresses and replication factor in the ConfigMap. For a three-broker cluster use replication factor 3 and configure topic/broker `min.insync.replicas=2`. The bootstrap code sets acks=all/idempotent producers but does not modify cluster durability policy. Add your HTTPS ingress, DNS, certificate, egress policy, and CA mounts. The connector Deployment in the manifest starts at zero replicas; enable approved sources and scale it to one when ready.

The manifest is a deployment template, not evidence that a cluster has been provisioned. It uses no in-cluster database/PVC because state lives in the external services. Schema setup is idempotent; Postgres serializes application table creation. Initial v1 is the only schema version; future changes need explicit migration code and rollout testing rather than relying on create_all to alter columns.

## Health and telemetry

- `/healthz`: API database connectivity.
- `/readyz`: recent worker presence (90-second window).
- `/api/agents`: queued/working/idle/offline logical state, last run, memory, error.
- `/api/sources`: last attempt/success, stale/error/disabled status, extraction counts.
- `/api/overview`: pending work and recent process health.
- `/metrics`: authenticated Prometheus process metrics, run duration, and queue latency. In a split deployment, the API endpoint contains API-process metrics; it does not aggregate Python counters from separate worker processes. Use database health/backlog and OTLP worker traces for shared visibility, or deploy a process metrics collection extension.
- `OTEL_EXPORTER_OTLP_ENDPOINT`: optional traces from runtime execution to your collector.

Alert on rising pending/dead work, absent worker heartbeat, source failure/staleness, and Kafka consumer lag using broker monitoring. The health endpoint alone cannot establish fresh source coverage. Inference reservations are visible in the `model_usage` table; actual provider billing must be monitored separately.

## Recovery and replay

A worker crash leaves its persisted inbox and expired lease available for a replacement. Restart the process; do not reset Kafka offsets to recover accepted work. The consumer commits only after persistence. If Kafka redelivers, IDs prevent duplicate effects. Checkpoints and factual state must be restored together with the application database.

Retry exhaustion sends failed jobs/outbox work to dead letters. Inspect `/api/admin/deadletters`, fix the underlying cause, then resubmit the relevant validated observation or create a new request ID for a failed task. Dead work is not silently retried forever, and no automated “replay all” endpoint is provided. Database repair of internal records should be deliberate and backed up first.

Use `replay:true` on backfill observations to update state without new user alerts. Source timestamps suppress older canonical overwrites. A newer authoritative correction can clear an unresolved conflict; immutable evidence remains retained. Parent themes recompute from current facts rather than treating previous summaries as new evidence.

## Retention and scale

Kafka's newly created topics default to 14 days retention; data recovery also requires Postgres backups. Enable continuous backups/PITR for managed Postgres and test restoring application plus LangGraph tables into an isolated environment. For local demo backup, stop the app and copy the complete `data/` directory including checkpoints and SQLite sidecars.

The initial application retains evidence, jobs, tasks, checkpoints, notifications, and runtime instance rows without an automatic purge. Define a retention policy and archive/delete completed records only after backup and legal/source retention review. Protect guide preferences/tokens and checkpoint content as application data. There is no automatic account erasure or administrative guide-token revocation UI yet; administrators can revoke an individual token by replacing its hash in the guide record. Rotating the admin token invalidates its prior hash on API startup.

Current scoped fact retrieval scans the canonical fact set and filters by city in Python. That is intentionally simple for a 30-city pilot but needs indexed city associations/date queries, pagination, and pruning for a large historical corpus. Theme matching uses tags and date overlap; embeddings/geospatial boundary services are not implemented. Do not infer production throughput or correctness metrics from the small test suite.

## Release verification

CI runs backend tests, UI tests/build, Docker image build, and a dedicated real Kafka/Postgres integration job. Before a live pilot, require the real integration job to pass, verify at least one source per intended city, exercise broker/database restart and restore, verify model behavior with the selected provider, and have a guide review multilingual outputs. The implementation environment verified local/background workflows and public RSS parsing; Docker/Kafka/Postgres integration and a paid model provider remained external gates.

Implementation test note: the workspace allowed HTTP requests through its proxy, so broadcaster feed fetch/parse checks succeeded. Its local DNS resolver was unavailable, so the connector's public-address validation correctly prevented a complete scheduled-fetch smoke test there. Production connector hosts need working DNS as well as outbound HTTPS.
