"""A2A 1.0 HTTP bindings provided by the official a2a-sdk (pinned in pyproject)."""

import asyncio
import time
from uuid import uuid4

from a2a.server.request_handlers import RequestHandler
from a2a.server.routes import create_agent_card_routes, create_jsonrpc_routes, create_rest_routes
from a2a.server.routes.common import ServerCallContextBuilder, DefaultServerCallContextBuilder
from a2a.types import a2a_pb2 as pb
from a2a.utils.errors import (
    InvalidParamsError,
    TaskNotFoundError,
    TaskNotCancelableError,
    PushNotificationNotSupportedError,
    ExtendedAgentCardNotConfiguredError,
)
from fastapi import FastAPI
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.struct_pb2 import Value
from sqlalchemy import select, update

from .db import tasks
from .registry import AGENTS, child_agents
from .schemas import Query, digest

STATES = {
    s: getattr(pb, "TASK_STATE_" + s.upper())
    for s in ["submitted", "working", "completed", "failed", "canceled"]
}


def data_part(data):
    value = Value()
    ParseDict(data, value)
    return pb.Part(data=value, media_type="application/json")


def task_proto(row):
    status = pb.TaskStatus(state=STATES[row["state"]])
    status.timestamp.FromMilliseconds(int(row["updated_at"] * 1000))
    if row.get("error"):
        status.message.CopyFrom(
            pb.Message(message_id=str(uuid4()), role=pb.ROLE_AGENT, parts=[pb.Part(text=row["error"])])
        )
    result = pb.Task(id=row["id"], context_id=row["context_id"], status=status)
    if row.get("result"):
        result.artifacts.append(
            pb.Artifact(artifact_id=row["id"], name="Observer briefing", parts=[data_part(row["result"])])
        )
    return result


class ContextBuilder(ServerCallContextBuilder):
    def build(self, request):
        context = DefaultServerCallContextBuilder().build(request)
        context.state["owner"] = request.state.principal["id"]
        return context


class ObservatoryHandler(RequestHandler):
    def __init__(self, service, agent_id, handlers):
        self.service, self.agent_id, self.handlers = service, agent_id, handlers

    def owned(self, task_id, context):
        row = self.service.db.one(tasks, task_id)
        if not row or row["owner"] != context.state["owner"] or row["agent_id"] != self.agent_id:
            raise TaskNotFoundError()
        return row

    async def submit(self, params, context):
        if (
            not params.message.message_id
            or not params.message.parts
            or params.message.role not in {pb.ROLE_USER, pb.ROLE_AGENT}
        ):
            raise InvalidParamsError(message="messageId, role, and non-empty parts are required")
        text = " ".join(part.text for part in params.message.parts if part.WhichOneof("content") == "text")
        supplied = {}
        for part in params.message.parts:
            if part.WhichOneof("content") == "data":
                parsed = MessageToDict(part.data)
                if isinstance(parsed, dict):
                    supplied.update(parsed)
        if params.message.task_id:
            raise InvalidParamsError(
                message="Task continuation is not supported; send a new message and reference the previous task."
            )
        try:
            query = Query.model_validate(
                {
                    **{k: v for k, v in supplied.items() if k in Query.model_fields},
                    "message": text or supplied.get("message", ""),
                    "agent_id": self.agent_id,
                    "request_id": params.message.message_id or str(uuid4()),
                }
            )
        except ValueError as exc:
            raise InvalidParamsError(message=str(exc)) from exc
        request = query.model_dump(mode="json")
        # A2A requests are normalized at the gateway and durably accepted as one transaction.
        # Internal delegation uses the same validated request contract; HTTP is used for remote callers.
        with self.service.db.tx() as c:
            task = self.service.create_task(
                self.agent_id,
                context.state["owner"],
                request,
                query.request_id,
                params.message.context_id or None,
                c=c,
            )
            if query.verify and AGENTS[self.agent_id].level not in {"instrument", "circulars"}:
                self.delegate(c, task, query)
        if not params.configuration.return_immediately:
            for _ in range(300):
                current = self.service.db.one(tasks, task["id"])
                if current["state"] not in {"submitted", "working"}:
                    return task_proto(current)
                await asyncio.sleep(0.2)
        return task_proto(self.service.db.one(tasks, task["id"]))

    def delegate(self, c, parent, query):
        def create_children(parent_row, aid, depth):
            if depth >= 2:
                return
            for child in child_agents(aid):
                if query.families and child.family and child.family not in query.families:
                    continue
                request = {
                    **query.model_dump(mode="json"),
                    "agent_id": child.id,
                    "verify": False,
                    "message": "Verify against stored source evidence: " + query.message,
                }
                # Build and validate a typed A2A delegation message, retaining it for audit.
                envelope = pb.SendMessageRequest(
                    message=pb.Message(
                        message_id=digest(parent_row["id"], child.id),
                        role=pb.ROLE_AGENT,
                        parts=[data_part(request)],
                    ),
                    configuration=pb.SendMessageConfiguration(return_immediately=True),
                )
                request["a2a_delegation"] = MessageToDict(envelope)
                row = self.service.create_task(
                    child.id,
                    f"agent:{aid}",
                    request,
                    envelope.message.message_id,
                    parent_task_id=parent_row["id"],
                    c=c,
                )
                create_children(row, child.id, depth + 1)

        create_children(parent, self.agent_id, 0)

    async def on_message_send(self, params, context):
        return await self.submit(params, context)

    async def on_get_task(self, params, context):
        return task_proto(self.owned(params.id, context))

    async def on_cancel_task(self, params, context):
        row = self.owned(params.id, context)
        if row["state"] not in {"submitted", "working"}:
            raise TaskNotCancelableError()
        with self.service.db.tx() as c:
            # Cancel descendants as well; output commits always re-check task status.
            pending = [params.id]
            while pending:
                tid = pending.pop()
                pending += list(c.execute(select(tasks.c.id).where(tasks.c.parent_task_id == tid)).scalars())
                c.execute(
                    update(tasks)
                    .where(tasks.c.id == tid, tasks.c.state.in_(["submitted", "working"]))
                    .values(state="canceled", updated_at=time.time())
                )
        return task_proto(self.owned(params.id, context))

    async def on_list_tasks(self, params, context):
        rows = self.service.db.rows(
            tasks,
            (tasks.c.owner == context.state["owner"]) & (tasks.c.agent_id == self.agent_id),
            order=tasks.c.created_at.desc(),
        )
        if params.context_id:
            rows = [r for r in rows if r["context_id"] == params.context_id]
        if params.status:
            rows = [r for r in rows if STATES[r["state"]] == params.status]
        if params.HasField("status_timestamp_after"):
            rows = [
                r for r in rows if r["updated_at"] > params.status_timestamp_after.ToMilliseconds() / 1000
            ]
        try:
            offset = int(params.page_token or "0")
            if offset < 0:
                raise ValueError()
        except ValueError as exc:
            raise InvalidParamsError(message="Invalid page token") from exc
        size = min(max(params.page_size or 20, 1), 100)
        page = rows[offset : offset + size]
        return pb.ListTasksResponse(
            tasks=[task_proto(r) for r in page],
            page_size=size,
            total_size=len(rows),
            next_page_token=str(offset + size) if offset + size < len(rows) else "",
        )

    async def events(self, tid, context):
        last = None
        while True:
            row = self.owned(tid, context)
            fingerprint = (row["state"], row["updated_at"])
            if fingerprint != last:
                yield task_proto(row)
                last = fingerprint
            if row["state"] not in {"submitted", "working"}:
                return
            await asyncio.sleep(0.25)

    async def on_message_send_stream(self, params, context):
        params.configuration.return_immediately = True
        task = await self.submit(params, context)
        async for event in self.events(task.id, context):
            yield event

    async def on_subscribe_to_task(self, params, context):
        async for event in self.events(params.id, context):
            yield event

    async def on_create_task_push_notification_config(self, params, context):
        raise PushNotificationNotSupportedError()

    async def on_get_task_push_notification_config(self, params, context):
        raise PushNotificationNotSupportedError()

    async def on_list_task_push_notification_configs(self, params, context):
        raise PushNotificationNotSupportedError()

    async def on_delete_task_push_notification_config(self, params, context):
        raise PushNotificationNotSupportedError()

    async def on_get_extended_agent_card(self, params, context):
        raise ExtendedAgentCardNotConfiguredError()


def mount_a2a(app, service):
    handlers = {}
    for agent in AGENTS.values():
        handler = ObservatoryHandler(service, agent.id, handlers)
        handlers[agent.id] = handler
        base = f"{service.settings.public_url.rstrip('/')}/a2a/{agent.id}"
        card = pb.AgentCard(
            name=agent.name,
            description=f"{agent.level.title()} astronomy intelligence with durable event-driven execution.",
            version="1.0.0",
            supported_interfaces=[
                pb.AgentInterface(url=base + "/", protocol_binding="JSONRPC", protocol_version="1.0")
            ],
            capabilities=pb.AgentCapabilities(streaming=True, push_notifications=False),
            default_input_modes=["text/plain", "application/json"],
            default_output_modes=["application/json"],
            skills=[
                pb.AgentSkill(
                    id="briefing",
                    name="Evidence-backed briefing",
                    description="Query current events and changes with citations.",
                    tags=["astronomy", agent.level],
                ),
                pb.AgentSkill(
                    id="verify",
                    name="Verify stored evidence",
                    description="Delegate scoped checks to child agents; preserves uncertainty and source revisions.",
                    tags=["verification"],
                ),
            ],
        )
        card.security_schemes["bearer"].CopyFrom(
            pb.SecurityScheme(http_auth_security_scheme=pb.HTTPAuthSecurityScheme(scheme="Bearer"))
        )
        card.security_requirements.add().schemes["bearer"].SetInParent()
        sub = FastAPI()
        sub.router.routes += create_agent_card_routes(card)
        sub.router.routes += create_jsonrpc_routes(handler, rpc_url="/", context_builder=ContextBuilder())
        sub.router.routes += create_rest_routes(handler, context_builder=ContextBuilder())
        app.mount("/a2a/" + agent.id, sub)
    return handlers
