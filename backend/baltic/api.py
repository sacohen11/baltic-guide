import asyncio
import contextlib
import hmac
import json
import os
import secrets
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field
from sqlalchemy import select, update, func, text

from .a2a_gateway import mount_a2a
from .connectors import load_sources, ConnectorRunner
from .db import (
    Database,
    agents,
    inbox,
    facts,
    findings,
    guides,
    notifications,
    tasks,
    sources,
    outbox,
    evidence,
    runtime_health,
    deadletters,
)
from .demo import seed_demo
from .registry import AGENTS
from .runtime import Runtime
from .schemas import Observation, Query, GuidePreferences, digest
from .service import Service
from .settings import Settings


class GuideCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class CardAction(BaseModel):
    action: str
    tag: str | None = None


def make_app(settings=None):
    settings = settings or Settings()
    settings.validate()
    db = Database(settings.database_url)
    service = Service(db, settings)
    service.bootstrap(load_sources(settings.sources_file))
    if settings.admin_token:
        with db.tx() as c:
            db.insert_once(
                c,
                guides,
                {
                    "id": "operator",
                    "name": "Operator",
                    "token_hash": digest(settings.admin_token),
                    "preferences": GuidePreferences().model_dump(mode="json"),
                },
            )
            c.execute(
                update(guides)
                .where(guides.c.id == "operator")
                .values(token_hash=digest(settings.admin_token))
            )
    runtime = Runtime(service)
    connectors = ConnectorRunner(service)

    @asynccontextmanager
    async def lifespan(app):
        connector_task = None
        if settings.embedded_runtime:
            await runtime.start()
        if settings.embedded_runtime and os.getenv("ENABLE_CONNECTORS", "false").lower() == "true":
            connector_task = asyncio.create_task(connectors.run())
        yield
        if connector_task:
            connector_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await connector_task
        await runtime.stop()
        db.engine.dispose()

    app = FastAPI(title="Baltic Guide API", version="0.1.0", lifespan=lifespan)
    app.state.service, app.state.runtime, app.state.connectors = service, runtime, connectors
    rate = defaultdict(deque)

    @app.middleware("http")
    async def authentication(request: Request, call_next):
        path = request.url.path
        protected = path.startswith(("/api/", "/a2a/")) or path == "/metrics"
        if protected:
            auth = request.headers.get("Authorization", "")
            token = auth[7:] if auth.startswith("Bearer ") else ""
            principal = None
            if settings.admin_token and hmac.compare_digest(token, settings.admin_token):
                principal = {"id": "operator", "role": "admin"}
            elif token:
                found = db.rows(guides, guides.c.token_hash == digest(token))
                if found:
                    principal = {"id": found[0]["id"], "role": "guide"}
            elif settings.demo_mode:
                principal = {"id": "demo", "role": "admin"}
            if not principal:
                return JSONResponse(
                    {"detail": "A guide or administrator bearer token is required"}, status_code=401
                )
            request.state.principal = principal
            if request.method == "POST":
                key = principal["id"]
                queue = rate[key]
                now = time.monotonic()
                while queue and queue[0] < now - 60:
                    queue.popleft()
                if len(queue) >= 120:
                    return JSONResponse(
                        {"detail": "Request rate limit exceeded"},
                        status_code=429,
                        headers={"Retry-After": "60"},
                    )
                queue.append(now)
            if (
                request.headers.get("content-length", "0").isdigit()
                and int(request.headers.get("content-length", "0")) > 2_000_000
            ):
                return JSONResponse({"detail": "Request too large"}, status_code=413)
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "same-origin"
        response.headers["X-Frame-Options"] = "DENY"
        return response

    def admin(request: Request):
        if request.state.principal["role"] != "admin":
            raise HTTPException(403, "Administrator access required")

    def owner(request):
        return request.state.principal["id"]

    @app.get("/healthz")
    def health():
        with db.engine.connect() as c:
            c.execute(text("SELECT 1"))
        return {"status": "ok"}

    @app.get("/readyz")
    def ready():
        healthy = db.rows(runtime_health, runtime_health.c.updated_at > time.time() - 90)
        if not any(":worker:" in r["id"] for r in healthy):
            return JSONResponse({"status": "waiting_for_worker"}, status_code=503)
        return {"status": "ready"}

    @app.get("/metrics", dependencies=[Depends(admin)])
    def metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/api/overview")
    def overview(request: Request):
        def count(table, where=None):
            stmt = select(func.count()).select_from(table)
            if where is not None:
                stmt = stmt.where(where)
            with db.engine.connect() as c:
                return c.execute(stmt).scalar_one()

        health_rows = db.rows(runtime_health, runtime_health.c.updated_at > time.time() - 90)
        return {
            "agents": 34,
            "cities": 30,
            "countries": 3,
            "demo_mode": settings.demo_mode,
            "transport": settings.transport,
            "model": settings.model or "Evidence rules",
            "facts": count(facts),
            "findings": count(findings),
            "unread": count(
                notifications,
                (notifications.c.guide_id == owner(request)) & (notifications.c.status == "unread"),
            ),
            "pending": count(inbox, inbox.c.state.in_(["pending", "running"])),
            "dead_letters": count(deadletters),
            "outbox_pending": count(outbox, outbox.c.published_at.is_(None)),
            "workers_online": sum(":worker:" in r["id"] for r in health_rows),
            "connectors_enabled": os.getenv("ENABLE_CONNECTORS", "false").lower() == "true",
            "role": request.state.principal["role"],
            "guide_id": owner(request),
        }

    @app.get("/api/agents")
    def list_agents():
        jobs = db.rows(inbox, inbox.c.state.in_(["pending", "running"]))
        live_workers = db.rows(runtime_health, runtime_health.c.updated_at > time.time() - 90)
        return [
            {
                **AGENTS[r["id"]].public(),
                "memory": r["memory"],
                "last_run": r["last_run"],
                "last_heartbeat": r["last_heartbeat"],
                "runs": r["runs"],
                "last_error": r["last_error"],
                "pending": sum(j["agent_id"] == r["id"] for j in jobs),
                "state": "working"
                if r["lease_until"] > time.time()
                else "queued"
                if any(j["agent_id"] == r["id"] for j in jobs)
                else "idle"
                if live_workers
                else "offline",
            }
            for r in db.rows(agents)
        ]

    @app.get("/api/agents/{agent_id}")
    def agent_detail(agent_id: str):
        if agent_id not in AGENTS:
            raise HTTPException(404)
        row = db.one(agents, agent_id)
        return {
            **AGENTS[agent_id].public(),
            "memory": row["memory"],
            "facts": [r["payload"] for r in service.scoped_facts(agent_id)],
            "findings": [r["payload"] for r in db.rows(findings, findings.c.agent_id == agent_id)],
            "jobs": [
                {k: v for k, v in r.items() if k != "payload"}
                for r in db.rows(
                    inbox, inbox.c.agent_id == agent_id, limit=25, order=inbox.c.created_at.desc()
                )
            ],
        }

    @app.get("/api/sources")
    def list_sources():
        now = time.time()
        rows = []
        for r in db.rows(sources):
            src = r["definition"]
            state = (
                "disabled"
                if not src.get("enabled")
                else "error"
                if r["error"]
                else "never_fetched"
                if not r["last_success"]
                else (
                    "stale" if now - r["last_success"] > 2 * src.get("interval_seconds", 900) else "healthy"
                )
            )
            rows.append({**r, "state": state})
        return rows

    @app.post("/api/sources/{source_id}/poll", dependencies=[Depends(admin)])
    async def poll_source(source_id: str):
        try:
            return {"ingested": await connectors.poll(source_id, force=True)}
        except Exception as exc:
            raise HTTPException(400, str(exc)[:300]) from exc

    @app.post("/api/observations", dependencies=[Depends(admin)], status_code=202)
    def ingest(obs: Observation):
        return {"event_id": service.ingest(obs), "status": "accepted"}

    @app.get("/api/facts")
    def list_facts(city_id: str | None = None, country: str | None = None):
        rows = db.rows(facts, order=facts.c.updated_at.desc(), limit=500)
        return [
            {
                **r["payload"],
                "version": r["version"],
                "evidence_ids": r["evidence_ids"],
                "conflict": r["conflict"],
            }
            for r in rows
            if (not city_id or city_id in r["payload"]["city_ids"])
            and (not country or r["payload"]["country"] == country)
        ]

    @app.get("/api/evidence/{evidence_id}")
    def get_evidence(evidence_id: str):
        row = db.one(evidence, evidence_id)
        if not row:
            raise HTTPException(404)
        return row

    @app.get("/api/messages")
    def messages(request: Request, after: int = 0, limit: int = 100):
        limit = min(max(limit, 1), 500)
        return db.rows(
            notifications,
            (notifications.c.guide_id == owner(request)) & (notifications.c.seq > after),
            order=notifications.c.seq,
            limit=limit,
        )

    @app.get("/api/stream")
    async def stream(request: Request, after: int = 0):
        try:
            cursor = max(after, int(request.headers.get("last-event-id", "0")))
        except ValueError:
            raise HTTPException(400, "Invalid event cursor")
        uid = owner(request)

        async def events():
            nonlocal cursor
            last_ping = 0
            while not await request.is_disconnected():
                rows = db.rows(
                    notifications,
                    (notifications.c.guide_id == uid) & (notifications.c.seq > cursor),
                    order=notifications.c.seq,
                    limit=100,
                )
                for row in rows:
                    cursor = row["seq"]
                    yield f"id: {cursor}\nevent: message\ndata: {json.dumps(row)}\n\n"
                if time.time() - last_ping > 15:
                    yield ": heartbeat\n\n"
                    last_ping = time.time()
                await asyncio.sleep(0.5)

        return StreamingResponse(
            events(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.post("/api/messages/{message_id}/actions")
    async def action(message_id: str, body: CardAction, request: Request):
        with db.tx() as c:
            row = (
                c.execute(
                    select(notifications).where(
                        notifications.c.id == message_id, notifications.c.guide_id == owner(request)
                    )
                )
                .mappings()
                .first()
            )
            if not row:
                raise HTTPException(404)
            if body.action in {"acknowledge", "dismiss", "read"}:
                c.execute(
                    update(notifications)
                    .where(notifications.c.id == message_id)
                    .values(
                        status={"acknowledge": "acknowledged", "dismiss": "dismissed", "read": "read"}[
                            body.action
                        ]
                    )
                )
            elif body.action == "save_to_trip":
                c.execute(
                    update(notifications)
                    .where(notifications.c.id == message_id)
                    .values(saved=not row["saved"])
                )
            elif body.action == "mute_theme":
                guide = c.execute(select(guides).where(guides.c.id == owner(request))).mappings().one()
                prefs = dict(guide["preferences"])
                tags = row["payload"].get("tags", [])
                prefs["muted_tags"] = sorted(set(prefs.get("muted_tags", [])) | set(tags))
                c.execute(update(guides).where(guides.c.id == owner(request)).values(preferences=prefs))
            elif body.action == "request_verification":
                query = {
                    "message": "Verify: " + row["payload"]["title"],
                    "city_ids": row["payload"].get("city_ids", []),
                }
                task = service.create_task(
                    "baltic:coordinator", owner(request), query, str(secrets.token_hex(16)), c=c
                )
                handlers["baltic:coordinator"].delegate(c, task, Query(**query, verify=True))
                return {"status": "accepted", "task_id": task["id"]}
            else:
                raise HTTPException(400, "Unknown action")
        return {"status": "ok"}

    @app.get("/api/preferences")
    def get_preferences(request: Request):
        row = db.one(guides, owner(request))
        return row["preferences"]

    @app.put("/api/preferences")
    def set_preferences(body: GuidePreferences, request: Request):
        with db.tx() as c:
            c.execute(
                update(guides)
                .where(guides.c.id == owner(request))
                .values(preferences=body.model_dump(mode="json"))
            )
        return body

    @app.post("/api/guides", dependencies=[Depends(admin)])
    def create_guide(body: GuideCreate):
        token = secrets.token_urlsafe(32)
        gid = secrets.token_hex(12)
        with db.tx() as c:
            db.insert_once(
                c,
                guides,
                {
                    "id": gid,
                    "name": body.name,
                    "token_hash": digest(token),
                    "preferences": GuidePreferences().model_dump(mode="json"),
                },
            )
        return {"id": gid, "name": body.name, "token": token}

    @app.post("/api/query", status_code=202)
    async def query(body: Query, request: Request):
        if body.agent_id not in AGENTS:
            raise HTTPException(404, "Unknown agent")
        guide = db.one(guides, owner(request))
        prefs = guide["preferences"]
        data = body.model_dump(mode="json")
        data["language"] = prefs.get("language", "en")
        for field in ["city_ids", "starts_at", "ends_at"]:
            if not data.get(field) and prefs.get(field):
                data[field] = prefs[field]
        with db.tx() as c:
            task = service.create_task(body.agent_id, owner(request), data, body.request_id, c=c)
            if body.verify:
                handlers[body.agent_id].delegate(c, task, Query.model_validate(data))
        return {"task_id": task["id"], "state": task["state"]}

    @app.get("/api/tasks")
    def list_tasks(request: Request):
        return db.rows(tasks, tasks.c.owner == owner(request), order=tasks.c.created_at.desc(), limit=100)

    @app.get("/api/tasks/{task_id}")
    def get_task(task_id: str, request: Request):
        row = db.one(tasks, task_id)
        if not row or row["owner"] != owner(request):
            raise HTTPException(404)
        return row

    @app.post("/api/tasks/{task_id}/cancel")
    async def cancel(task_id: str, request: Request):
        from a2a.server.context import ServerCallContext
        from a2a.types import a2a_pb2 as pb

        row = db.one(tasks, task_id)
        if not row or row["owner"] != owner(request):
            raise HTTPException(404)
        try:
            await handlers[row["agent_id"]].on_cancel_task(
                pb.CancelTaskRequest(id=task_id), ServerCallContext(state={"owner": owner(request)})
            )
        except Exception as exc:
            raise HTTPException(409, str(exc)) from exc
        return {"state": "canceled"}

    @app.post("/api/demo/seed", dependencies=[Depends(admin)])
    def demo():
        if not settings.demo_mode:
            raise HTTPException(403, "Demo mode disabled")
        return {"ingested": seed_demo(service)}

    @app.post("/api/demo/cancel", dependencies=[Depends(admin)])
    def cancel_demo():
        if not settings.demo_mode:
            raise HTTPException(403)
        row = db.one(facts, "demo-market-riga")
        if not row:
            raise HTTPException(409, "Load the demo scenario first")
        from datetime import UTC, datetime

        obs = Observation.model_validate(
            {
                **row["payload"],
                "status": "cancelled",
                "summary": "Illustrative cancellation: this market is no longer taking place. Remove it from the demo itinerary.",
                "updated_at": datetime.now(UTC).isoformat(),
            }
        )
        return {"event_id": service.ingest(obs)}

    @app.get("/api/admin/deadletters", dependencies=[Depends(admin)])
    def list_deadletters():
        return db.rows(deadletters, order=deadletters.c.created_at.desc(), limit=100)

    handlers = mount_a2a(app, service)
    frontend = Path(__file__).parents[2] / "frontend" / "dist"
    # Source checkout and container both place the compiled UI at ./frontend/dist.
    frontend = Path(os.getenv("FRONTEND_DIR", "frontend/dist"))
    if frontend.exists():
        app.mount("/assets", StaticFiles(directory=frontend / "assets"), name="assets")

        @app.get("/", response_class=HTMLResponse)
        def index():
            return FileResponse(frontend / "index.html")
    else:

        @app.get("/", response_class=HTMLResponse)
        def index():
            return '<h1>Baltic Guide API</h1><p>Build the dashboard: cd frontend &amp;&amp; npm ci &amp;&amp; npm run build</p><a href="/docs">API docs</a>'

    return app
