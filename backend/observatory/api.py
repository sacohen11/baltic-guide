import asyncio
import hmac
import json
import secrets
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field
from sqlalchemy import func, select, text, update

from .a2a_gateway import mount_a2a
from .db import (
    Database,
    agents,
    cases,
    deadletters,
    evidence,
    facts,
    findings,
    inbox,
    notifications,
    observers,
    outbox,
    raw_records,
    runtime_health,
    sources,
    tasks,
)
from .graphs import briefing
from .registry import AGENTS
from .runtime import Runtime
from .schemas import ObserverPreferences, Query, digest
from .service import Service
from .settings import Settings


class ObserverCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class MessageAction(BaseModel):
    read: bool | None = None
    saved: bool | None = None


def make_app(settings=None):
    settings = settings or Settings()
    settings.validate()
    db = Database(settings.database_url)
    service = Service(db, settings)
    service.bootstrap()
    runtime = Runtime(service)

    @asynccontextmanager
    async def lifespan(app):
        if settings.embedded_runtime:
            await runtime.start()
        try:
            yield
        finally:
            await runtime.stop()
            db.engine.dispose()

    app = FastAPI(title="Space Observatory", version="1.0.0", lifespan=lifespan)
    app.state.service, app.state.runtime = service, runtime
    rate = defaultdict(deque)

    @app.middleware("http")
    async def authenticate(request: Request, call_next):
        path = request.url.path
        if path.startswith(("/api/", "/a2a/")) or path == "/metrics":
            authorization = request.headers.get("authorization", "")
            token = authorization[7:] if authorization.startswith("Bearer ") else ""
            principal = None
            if token and hmac.compare_digest(token, settings.admin_token):
                principal = {"id": "operator", "role": "admin"}
            elif token:
                rows = db.rows(observers, observers.c.token_hash == digest(token))
                if rows:
                    principal = {"id": rows[0]["id"], "role": "observer"}
            if not principal:
                return JSONResponse(
                    {"detail": "An observer or administrator token is required"}, status_code=401
                )
            request.state.principal = principal
            if request.method not in {"GET", "HEAD", "OPTIONS"}:
                q = rate[principal["id"]]
                now = time.monotonic()
                while q and q[0] < now - 60:
                    q.popleft()
                if len(q) >= 60:
                    return JSONResponse(
                        {"detail": "Too many requests"}, status_code=429, headers={"Retry-After": "60"}
                    )
                q.append(now)
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["X-Frame-Options"] = "DENY"
        if path.startswith(("/api/", "/a2a/")):
            response.headers["Cache-Control"] = "no-store"
        return response

    # CORS wraps authentication so browser preflights are handled without a bearer token.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[s.strip() for s in settings.cors_origins.split(",") if s.strip()],
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
        allow_headers=["Authorization", "Content-Type"],
    )

    def owner(request):
        return request.state.principal["id"]

    def admin(request):
        if request.state.principal["role"] != "admin":
            raise HTTPException(403, "Administrator access required")

    def count(table, condition=None):
        query = select(func.count()).select_from(table)
        if condition is not None:
            query = query.where(condition)
        with db.engine.connect() as c:
            return c.execute(query).scalar_one()

    def gcn_status():
        health = db.one(runtime_health, "gcn")
        if not health:
            return {"state": "not_running", "error": "GCN ingestor has not reported a heartbeat"}
        return {
            **health["payload"],
            "heartbeat": health["updated_at"],
            "state": health["payload"]["state"] if health["updated_at"] > time.time() - 90 else "offline",
        }

    @app.get("/healthz")
    def health():
        with db.engine.connect() as c:
            c.execute(text("SELECT 1"))
        return {"status": "ok"}

    @app.get("/readyz")
    def ready():
        workers = db.rows(runtime_health, runtime_health.c.updated_at > time.time() - 90)
        working = any(":worker:" in r["id"] for r in workers)
        connected = gcn_status()["state"] == "connected"
        return JSONResponse(
            {"ready": working and connected, "workers": working, "gcn": connected},
            status_code=200 if working and connected else 503,
        )

    @app.get("/metrics")
    def metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/api/overview")
    def overview(request: Request):
        worker_rows = db.rows(runtime_health, runtime_health.c.updated_at > time.time() - 90)
        return {
            "name": "Space Observatory",
            "agents": len(AGENTS),
            "case_agents": count(cases),
            "reports": count(facts, facts.c.test.is_(False)),
            "cases": count(cases, ~cases.c.id.startswith("test:")),
            "pending": count(inbox, inbox.c.state.in_(["pending", "running"])),
            "unread": count(
                notifications,
                (notifications.c.observer_id == owner(request)) & (notifications.c.status == "unread"),
            ),
            "outbox_pending": count(outbox, outbox.c.published_at.is_(None)),
            "quarantined": count(deadletters),
            "workers_online": sum(":worker:" in r["id"] for r in worker_rows),
            "gcn": gcn_status(),
            "transport": settings.transport,
            "model": settings.model or None,
            "role": request.state.principal["role"],
            "observer_id": owner(request),
        }

    @app.get("/api/agents")
    def list_agents():
        now = time.time()
        with db.engine.connect() as c:
            pending = dict(
                c.execute(
                    select(inbox.c.agent_id, func.count())
                    .where(inbox.c.state.in_(["pending", "running"]))
                    .group_by(inbox.c.agent_id)
                ).all()
            )
        rows = db.rows(agents)
        return [
            {
                **r["definition"],
                "memory": r["memory"],
                "runs": r["runs"],
                "last_run": r["last_run"],
                "last_heartbeat": r["last_heartbeat"],
                "last_error": r["last_error"],
                "pending": pending.get(r["id"], 0),
                "state": "working"
                if r["lease_until"] > now
                else "queued"
                if pending.get(r["id"])
                else "sleeping",
            }
            for r in rows
            if r["id"] in AGENTS
        ]

    @app.get("/api/cases")
    def list_cases(limit: int = 50, before: float | None = None, search: str = ""):
        condition = ~cases.c.id.startswith("test:")
        if before:
            condition &= cases.c.updated_at < before
        if search:
            condition &= cases.c.name.ilike("%" + search[:100] + "%")
        rows = db.rows(cases, condition, limit=min(max(limit, 1), 100), order=cases.c.updated_at.desc())
        result = []
        for row in rows:
            finding = db.one(findings, digest("briefing", row["agent_id"]))
            result.append({**row, "briefing": finding["payload"] if finding else None})
        return result

    @app.get("/api/cases/{case_id:path}")
    def case_detail(case_id: str):
        row = db.one(cases, case_id)
        if not row or case_id.startswith("test:"):
            raise HTTPException(404, "Case not found")
        current = service.scoped_facts(row["agent_id"])
        ids = [r["id"] for r in current]
        history = db.rows(
            evidence, evidence.c.entity_id.in_(ids), order=evidence.c.created_at.desc(), limit=200
        )
        return {
            **row,
            "agent": db.one(agents, row["agent_id"]),
            "briefing": briefing(current, row["name"], case_id),
            "reports": current,
            "history": history,
        }

    @app.get("/api/sources")
    def list_sources():
        status = gcn_status()
        subscribed = set(status.get("topics", []))
        return [
            {
                **r,
                "state": "not_subscribed"
                if r["id"] not in subscribed
                else "parser_error"
                if r["error"]
                else "listening"
                if status["state"] == "connected"
                else status["state"],
            }
            for r in db.rows(sources)
        ]

    @app.get("/api/messages")
    def messages(request: Request, before: int | None = None, limit: int = 50):
        condition = notifications.c.observer_id == owner(request)
        if before:
            condition &= notifications.c.seq < before
        return db.rows(
            notifications, condition, order=notifications.c.seq.desc(), limit=min(max(limit, 1), 100)
        )

    @app.patch("/api/messages/{message_id}")
    def message_action(message_id: str, body: MessageAction, request: Request):
        values = {}
        if body.read is not None:
            values["status"] = "read" if body.read else "unread"
        if body.saved is not None:
            values["saved"] = body.saved
        if not values:
            raise HTTPException(422, "No changes supplied")
        with db.tx() as c:
            changed = c.execute(
                update(notifications)
                .where(
                    notifications.c.id == message_id,
                    notifications.c.observer_id == owner(request),
                    notifications.c.status != "superseded",
                )
                .values(**values)
            ).rowcount
        if not changed:
            raise HTTPException(404, "Current message not found")
        return {"ok": True}

    @app.get("/api/stream")
    async def stream(request: Request, after: int = 0):
        oid = owner(request)

        async def events():
            cursor = max(0, after)
            while not await request.is_disconnected():
                rows = db.rows(
                    notifications,
                    (notifications.c.observer_id == oid) & (notifications.c.seq > cursor),
                    limit=100,
                    order=notifications.c.seq.asc(),
                )
                for row in rows:
                    cursor = row["seq"]
                    yield f"id: {cursor}\nevent: message\ndata: {json.dumps(row)}\n\n"
                if not rows:
                    yield ": heartbeat\n\n"
                    await asyncio.sleep(2)

        return StreamingResponse(
            events(), media_type="text/event-stream", headers={"X-Accel-Buffering": "no"}
        )

    @app.get("/api/preferences")
    def preferences(request: Request):
        return db.one(observers, owner(request))["preferences"]

    @app.put("/api/preferences")
    def save_preferences(body: ObserverPreferences, request: Request):
        if any(i not in AGENTS or AGENTS[i].level != "instrument" for i in body.instruments):
            raise HTTPException(422, "Unknown instrument")
        with db.tx() as c:
            c.execute(
                update(observers)
                .where(observers.c.id == owner(request))
                .values(preferences=body.model_dump())
            )
        return body

    @app.post("/api/tasks", status_code=202)
    def ask(body: Query, request: Request):
        try:
            service.agent(body.agent_id)
        except ValueError:
            raise HTTPException(404, "Agent not found")
        with db.tx() as c:
            task = service.create_task(
                body.agent_id, owner(request), body.model_dump(mode="json"), body.request_id, c=c
            )
            if body.verify and body.agent_id in handlers:
                handlers[body.agent_id].delegate(c, task, body)
        return task

    @app.get("/api/tasks")
    def list_tasks(request: Request):
        return db.rows(tasks, tasks.c.owner == owner(request), limit=50, order=tasks.c.created_at.desc())

    @app.get("/api/tasks/{task_id}")
    def get_task(task_id: str, request: Request):
        row = db.one(tasks, task_id)
        if not row or row["owner"] != owner(request):
            raise HTTPException(404, "Task not found")
        return row

    @app.post("/api/tasks/{task_id}/cancel")
    def cancel(task_id: str, request: Request):
        row = service.cancel_task(task_id, owner(request))
        if not row:
            raise HTTPException(404, "Task not found")
        return row

    @app.post("/api/observers", status_code=201)
    def create_observer(body: ObserverCreate, request: Request):
        admin(request)
        token = secrets.token_urlsafe(32)
        oid = secrets.token_hex(12)
        with db.tx() as c:
            db.insert_once(
                c,
                observers,
                {
                    "id": oid,
                    "name": body.name,
                    "token_hash": digest(token),
                    "preferences": ObserverPreferences().model_dump(),
                },
            )
        return {"id": oid, "name": body.name, "token": token}

    @app.get("/api/deadletters")
    def quarantine(request: Request):
        admin(request)
        return db.rows(deadletters, limit=100, order=deadletters.c.created_at.desc())

    @app.get("/api/raw/{raw_id}")
    def raw_record(raw_id: str):
        row = db.one(raw_records, raw_id)
        if not row:
            raise HTTPException(404, "Raw record not found")
        return row

    handlers = mount_a2a(app, service)
    dist = Path(__file__).resolve().parents[2] / "frontend" / "dist"
    if dist.exists():
        app.mount("/assets", StaticFiles(directory=dist / "assets"), name="assets")

        @app.get("/")
        def frontend():
            return FileResponse(dist / "index.html")

    return app
