"""Transactional GCN normalization, durable cases, routing, and observer delivery."""

import time
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import select, update, text, delete

from .db import (
    agents,
    inbox,
    outbox,
    evidence,
    facts,
    findings,
    observers,
    notifications,
    tasks,
    sources,
    deadletters,
    cases,
)
from .db import case_links
from .registry import AGENTS, TOPIC_AGENT, GCN_TOPICS, Agent
from .schemas import Observation, ObserverPreferences, digest


def stamp(value):
    if not value:
        return 0
    return (
        datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        if isinstance(value, str)
        else value.timestamp()
    )


def match_preferences(payload, prefs):
    if prefs.get("families") and not set(payload.get("families", [])) & set(prefs["families"]):
        return False
    if prefs.get("instruments") and not set(payload.get("instruments", [])) & set(prefs["instruments"]):
        return False
    if prefs.get("multi_messenger_only") and len(payload.get("families", [])) < 2:
        return False
    return True


class Service:
    def __init__(self, db, settings):
        self.db, self.settings = db, settings

    def agent(self, aid):
        if aid in AGENTS:
            return AGENTS[aid]
        row = self.db.one(agents, aid)
        if not row:
            raise ValueError("Unknown agent")
        return Agent(**{k: v for k, v in row["definition"].items() if k != "topic"})

    def bootstrap(self):
        self.db.initialize()
        with self.db.tx() as c:
            for a in AGENTS.values():
                self.register_agent(c, a)
            values = {
                "id": "operator",
                "name": "Operator",
                "token_hash": digest(self.settings.admin_token),
                "preferences": ObserverPreferences().model_dump(),
            }
            self.db.insert_once(c, observers, values)
            c.execute(
                update(observers).where(observers.c.id == "operator").values(token_hash=values["token_hash"])
            )
            for topic, aid in GCN_TOPICS.items():
                self.db.insert_once(
                    c,
                    sources,
                    {
                        "id": topic,
                        "definition": {
                            "name": topic,
                            "agent_id": aid,
                            "kind": "kafka",
                            "url": "https://gcn.nasa.gov/docs/client",
                        },
                        "item_count": 0,
                        "last_success": 0,
                        "last_attempt": 0,
                    },
                )

    def register_agent(self, c, agent):
        self.db.insert_once(
            c,
            agents,
            {
                "id": agent.id,
                "definition": agent.public(),
                "memory": {},
                "lease_until": 0,
                "last_run": 0,
                "last_heartbeat": 0,
                "next_tick": time.time() + 60,
                "runs": 0,
            },
        )

    def emit(self, c, topic, key, payload, event_id=None):
        eid = event_id or digest(topic, key, payload)
        self.db.insert_once(
            c,
            outbox,
            {
                "id": eid,
                "topic": topic,
                "key": key,
                "payload": {**payload, "_event_id": eid},
                "created_at": time.time(),
                "lease_until": 0,
                "attempts": 0,
            },
        )
        return eid

    def enqueue(self, c, agent_id, kind, payload, key, priority=0):
        if not c.execute(select(agents.c.id).where(agents.c.id == agent_id)).first():
            raise ValueError("Unknown agent")
        jid = digest(agent_id, kind, key)
        self.db.insert_once(
            c,
            inbox,
            {
                "id": jid,
                "agent_id": agent_id,
                "kind": kind,
                "payload": payload,
                "priority": priority,
                "state": "pending",
                "created_at": time.time(),
                "available_at": time.time(),
                "attempts": 0,
            },
        )
        return jid

    def normalize(self, c, payload):
        obs = Observation.model_validate({k: v for k, v in payload.items() if not k.startswith("_")})
        if self.db.engine.dialect.name == "postgresql":
            c.execute(text("SELECT pg_advisory_xact_lock(81697102)"))
        entity_id = digest(obs.agent_id, obs.source_item_id, obs.test)
        body = obs.model_dump(mode="json")
        # Retain every source revision as evidence, including out-of-order ones.
        evid = digest(obs.raw_id)
        self.db.insert_once(
            c,
            evidence,
            {
                "id": evid,
                "entity_id": entity_id,
                "source_id": obs.source_id,
                "content_hash": digest(body),
                "payload": body,
            },
        )
        old = c.execute(select(facts).where(facts.c.id == entity_id).with_for_update()).mappings().first()
        source_time = stamp(obs.notice_time)
        if old:
            prior = old["payload"]
            if source_time < old["source_time"]:
                return
            if (
                obs.revision is not None
                and prior.get("revision") is not None
                and obs.revision < prior["revision"]
            ):
                return
            # Identical payload re-delivery at another offset does not create another finding.
            if digest({k: v for k, v in body.items() if k != "raw_id"}) == old["content_hash"]:
                return
        if obs.test:
            body["case_ids"] = ["test:" + cid for cid in body["case_ids"]]
        body["entity_id"] = entity_id
        version = old["version"] + 1 if old else 1
        values = {
            "agent_id": obs.agent_id,
            "family": obs.family,
            "test": obs.test,
            "version": version,
            "payload": body,
            "content_hash": digest({k: v for k, v in obs.model_dump(mode="json").items() if k != "raw_id"}),
            "evidence_ids": [evid],
            "updated_at": time.time(),
            "source_time": source_time,
            "conflict": False,
        }
        if old:
            c.execute(update(facts).where(facts.c.id == entity_id).values(**values))
        else:
            self.db.insert_once(c, facts, {"id": entity_id, **values})
        affected = set(body["case_ids"]) | set(old["payload"]["case_ids"] if old else [])
        c.execute(delete(case_links).where(case_links.c.entity_id == entity_id))
        for cid in body["case_ids"]:
            self.db.insert_once(
                c, case_links, {"id": digest(cid, entity_id), "case_id": cid, "entity_id": entity_id}
            )
        for cid in sorted(affected):
            aid = "case:" + digest(cid)[:32]
            self.register_agent(c, Agent(aid, cid, "case", parent="sky", case_id=cid))
            self.db.insert_once(
                c, cases, {"id": cid, "name": cid, "agent_id": aid, "updated_at": time.time()}
            )
            c.execute(update(cases).where(cases.c.id == cid).values(updated_at=time.time()))
            self.emit(
                c,
                "observatory.cases.v1",
                cid,
                {"agent_id": aid, "entity_id": entity_id, "version": version},
                digest("case", cid, entity_id, version),
            )
        self.emit(
            c,
            AGENTS[obs.agent_id].topic,
            entity_id,
            {"entity_id": entity_id, "version": version},
            digest("instrument", entity_id, version),
        )

    def route(self, c, topic, payload):
        eid = payload.get("_event_id") or digest(topic, payload)
        if topic == "observatory.ingest.raw.v1":
            self.normalize(c, payload)
        elif topic in TOPIC_AGENT:
            self.enqueue(c, TOPIC_AGENT[topic], "observation", payload, eid, 1)
        elif topic == "observatory.cases.v1":
            self.enqueue(c, payload["agent_id"], "observation", payload, eid, 2)
        elif topic == "observatory.findings.v1":
            row = c.execute(select(agents).where(agents.c.id == payload["agent_id"])).mappings().one()
            parent = row["definition"].get("parent")
            if parent:
                self.enqueue(c, parent, "finding", payload, eid)
        elif topic == "observatory.observer.messages.v1":
            self.deliver(c, payload)
        elif topic == "observatory.deadletter.v1":
            self.db.insert_once(
                c,
                deadletters,
                {"id": eid, "payload": payload, "error": payload.get("error", ""), "created_at": time.time()},
            )

    def scoped_facts(self, agent_id, c=None, since=None, families=None, case_id=None, limit=500):
        agent = self.agent(agent_id)
        condition = facts.c.test.is_(False)
        if agent.level in {"instrument", "circulars"}:
            condition &= facts.c.agent_id == agent.id
        elif agent.level == "family":
            condition &= facts.c.family == agent.family
        elif agent.level == "case":
            condition &= facts.c.id.in_(
                select(case_links.c.entity_id).where(case_links.c.case_id == agent.case_id)
            )
        if since:
            condition &= facts.c.updated_at >= since
        if families:
            condition &= facts.c.family.in_(families)
        if case_id:
            condition &= facts.c.id.in_(select(case_links.c.entity_id).where(case_links.c.case_id == case_id))
        query = select(facts).where(condition).order_by(facts.c.updated_at.desc()).limit(limit)
        if c is not None:
            return [dict(r) for r in c.execute(query).mappings()]
        with self.db.engine.connect() as connection:
            return [dict(r) for r in connection.execute(query).mappings()]

    def publish_finding(self, c, agent_id, finding_id, payload, replay=False):
        fingerprint = digest({k: v for k, v in payload.items() if k not in {"as_of"}})
        old = c.execute(select(findings).where(findings.c.id == finding_id)).mappings().first()
        if old and old["fingerprint"] == fingerprint:
            return False
        version = old["version"] + 1 if old else 1
        payload = {
            **payload,
            "id": finding_id,
            "version": version,
            "agent_id": agent_id,
            "as_of": datetime.now(UTC).isoformat(),
        }
        values = {
            "agent_id": agent_id,
            "version": version,
            "fingerprint": fingerprint,
            "payload": payload,
            "updated_at": time.time(),
        }
        if old:
            c.execute(update(findings).where(findings.c.id == finding_id).values(**values))
        else:
            self.db.insert_once(c, findings, {"id": finding_id, **values})
        self.emit(c, "observatory.findings.v1", finding_id, payload)
        definition = c.execute(select(agents.c.definition).where(agents.c.id == agent_id)).scalar_one()
        if definition["level"] in {"case", "coordinator"} and not payload.get("test"):
            self.emit(c, "observatory.observer.messages.v1", finding_id, payload)
        return True

    def deliver(self, c, payload):
        if self.db.engine.dialect.name == "postgresql":
            # Serialize sequence allocation through commit so SSE cursors cannot skip a late commit.
            c.execute(text("SELECT pg_advisory_xact_lock(81697003)"))
        if payload.get("observer_id"):
            recipients = c.execute(
                select(observers).where(observers.c.id == payload["observer_id"])
            ).mappings()
        else:
            recipients = c.execute(select(observers)).mappings()
        for observer in recipients:
            previous = c.execute(
                select(notifications.c.id).where(
                    notifications.c.observer_id == observer["id"], notifications.c.finding_id == payload["id"]
                )
            ).first()
            # Corrections must reach observers who saw an earlier version, even after a date/location changes.
            if (
                not previous
                and not payload.get("observer_id")
                and not match_preferences(payload, observer["preferences"])
            ):
                continue
            nid = digest(observer["id"], payload["id"], payload["version"])
            self.db.insert_once(
                c,
                notifications,
                {
                    "id": nid,
                    "observer_id": observer["id"],
                    "finding_id": payload["id"],
                    "version": payload["version"],
                    "payload": payload,
                    "status": "unread",
                    "saved": False,
                    "created_at": time.time(),
                },
            )
            c.execute(
                update(notifications)
                .where(
                    notifications.c.observer_id == observer["id"],
                    notifications.c.finding_id == payload["id"],
                    notifications.c.version < payload["version"],
                )
                .values(status="superseded")
            )

    def create_task(self, agent_id, owner, request, request_id, context_id=None, parent_task_id=None, c=None):
        self.agent(agent_id)
        if c is None:
            with self.db.tx() as conn:
                return self.create_task(
                    agent_id, owner, request, request_id, context_id, parent_task_id, conn
                )
        key = digest(agent_id, owner, request_id)
        tid = str(uuid4())
        existing = c.execute(select(tasks).where(tasks.c.request_key == key)).mappings().first()
        if existing:
            return dict(existing)
        row = {
            "id": tid,
            "request_key": key,
            "agent_id": agent_id,
            "owner": owner,
            "context_id": context_id or str(uuid4()),
            "parent_task_id": parent_task_id,
            "state": "submitted",
            "request": request,
            "created_at": time.time(),
            "updated_at": time.time(),
        }
        # Serialize task creation by its unique request key; conflicting requests use the winner.
        if self.db.engine.dialect.name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert
        else:
            from sqlalchemy.dialects.sqlite import insert
        inserted = c.execute(
            insert(tasks).values(**row).on_conflict_do_nothing(index_elements=["request_key"])
        )
        if not inserted.rowcount:
            return dict(c.execute(select(tasks).where(tasks.c.request_key == key)).mappings().one())
        self.enqueue(c, agent_id, "task", {"task_id": tid}, tid, 5)
        return row

    def cancel_task(self, task_id, owner):
        with self.db.tx() as c:
            task = (
                c.execute(
                    select(tasks).where(tasks.c.id == task_id, tasks.c.owner == owner).with_for_update()
                )
                .mappings()
                .first()
            )
            if not task:
                return None
            if task["state"] in {"submitted", "working"}:
                pending = [task_id]
                while pending:
                    tid = pending.pop()
                    pending += list(
                        c.execute(select(tasks.c.id).where(tasks.c.parent_task_id == tid)).scalars()
                    )
                    c.execute(
                        update(tasks)
                        .where(tasks.c.id == tid, tasks.c.state.in_(["submitted", "working"]))
                        .values(state="canceled", updated_at=time.time())
                    )
            return dict(c.execute(select(tasks).where(tasks.c.id == task_id)).mappings().one())
