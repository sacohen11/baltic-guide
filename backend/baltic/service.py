"""Transactional domain operations. All published events pass through the outbox."""

import time
import unicodedata
from datetime import UTC, datetime
from uuid import uuid4
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

from sqlalchemy import select, update, text

from .db import (
    agents,
    inbox,
    outbox,
    evidence,
    facts,
    findings,
    guides,
    notifications,
    tasks,
    sources,
    source_items,
    deadletters,
)
from .registry import AGENTS, TOPIC_AGENT
from .schemas import Observation, GuidePreferences, digest


def plain(text):
    return "".join(x for x in unicodedata.normalize("NFKD", text.lower()) if not unicodedata.combining(x))


def canonical_url(url):
    u = urlsplit(url)
    query = urlencode(sorted((k, v) for k, v in parse_qsl(u.query) if not k.startswith("utm_")))
    return urlunsplit((u.scheme, u.netloc.lower(), u.path.rstrip("/"), query, ""))


def stamp(value):
    if not value:
        return 0
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value.timestamp()


def relevant_cities(payload):
    """News mentions indicate relevance, never a confirmed event venue."""
    confirmed = set(payload.get("city_ids", []))
    mentioned = set(payload.get("candidate_city_ids", [])) if payload.get("kind") != "event" else set()
    return sorted(confirmed | mentioned)


def match_preferences(payload, prefs):
    cities = set(payload.get("city_ids", []))
    countries = set(payload.get("countries", []))
    selected_cities, selected_countries = set(prefs.get("city_ids", [])), set(prefs.get("countries", []))
    if (selected_cities or selected_countries) and not (
        cities & selected_cities or countries & selected_countries
    ):
        return False
    tags = set(payload.get("tags", []))
    if tags & set(prefs.get("muted_tags", [])):
        return False
    if prefs.get("interests") and payload.get("type") not in {"disruption", "answer"}:
        if not tags.intersection(prefs["interests"]):
            return False
    start, end = stamp(payload.get("starts_at")), stamp(payload.get("ends_at"))
    if start and prefs.get("ends_at") and start > stamp(prefs["ends_at"]):
        return False
    if end and prefs.get("starts_at") and end < stamp(prefs["starts_at"]):
        return False
    return True


class Service:
    def __init__(self, db, settings):
        self.db, self.settings = db, settings

    def bootstrap(self, source_defs=()):
        self.db.initialize()
        with self.db.tx() as c:
            for a in AGENTS.values():
                self.db.insert_once(
                    c,
                    agents,
                    {
                        "id": a.id,
                        "definition": a.public(),
                        "memory": {},
                        "lease_until": 0,
                        "last_run": 0,
                        "last_heartbeat": 0,
                        "next_tick": time.time() + 60,
                        "runs": 0,
                    },
                )
            if self.settings.demo_mode:
                self.db.insert_once(
                    c,
                    guides,
                    {
                        "id": "demo",
                        "name": "Demo guide",
                        "token_hash": digest("demo-guide"),
                        "preferences": GuidePreferences().model_dump(mode="json"),
                    },
                )
            for source in source_defs:
                self.db.insert_once(
                    c,
                    sources,
                    {
                        "id": source["id"],
                        "definition": source,
                        "next_poll": 0,
                        "last_success": 0,
                        "last_attempt": 0,
                        "item_count": 0,
                        "failures": 0,
                    },
                )
                c.execute(update(sources).where(sources.c.id == source["id"]).values(definition=source))

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
        if agent_id not in AGENTS:
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

    def ingest(self, obs: Observation):
        payload = obs.model_dump(mode="json")
        with self.db.tx() as c:
            return self.emit(
                c,
                "baltic.ingest.raw.v1",
                obs.source_id,
                payload,
                digest("raw", obs.source_id, obs.source_item_id, obs.content_hash(), obs.replay),
            )

    def normalize(self, c, payload):
        obs = Observation.model_validate({k: v for k, v in payload.items() if not k.startswith("_")})
        item_key = digest(obs.source_id, obs.source_item_id)
        if self.db.engine.dialect.name == "postgresql":
            # Serialize normalizers so related publisher assertions and source-item mappings remain atomic.
            c.execute(text("SELECT pg_advisory_xact_lock(81697002)"))
        item = c.execute(select(source_items).where(source_items.c.id == item_key)).mappings().first()
        source_time = stamp(obs.updated_at or obs.published_at or obs.observed_at)
        if item and (item["content_hash"] == obs.content_hash() or item["source_time"] > source_time):
            return
        if item:
            entity_id = item["entity_id"]
        elif obs.entity_id:
            entity_id = obs.entity_id
        elif obs.kind == "event" and obs.starts_at and obs.city_ids:
            entity_id = digest("event", plain(obs.title), obs.city_ids, obs.starts_at.date().isoformat())
        else:
            entity_id = digest("url", canonical_url(obs.source_url))
        evid = digest(item_key, obs.content_hash())
        body = obs.model_dump(mode="json")
        body["entity_id"] = entity_id
        self.db.insert_once(
            c,
            evidence,
            {
                "id": evid,
                "entity_id": entity_id,
                "source_id": obs.source_id,
                "content_hash": obs.content_hash(),
                "payload": body,
                "created_at": time.time(),
            },
        )
        item_values = {"entity_id": entity_id, "content_hash": obs.content_hash(), "source_time": source_time}
        self.db.insert_once(c, source_items, {"id": item_key, **item_values})
        c.execute(update(source_items).where(source_items.c.id == item_key).values(**item_values))
        old = c.execute(select(facts).where(facts.c.id == entity_id).with_for_update()).mappings().first()
        evids = sorted(set((old["evidence_ids"] if old else []) + [evid]))
        semantic_hash = digest(
            {
                k: body[k]
                for k in [
                    "title",
                    "summary",
                    "country",
                    "city_ids",
                    "kind",
                    "status",
                    "tags",
                    "starts_at",
                    "ends_at",
                    "location",
                    "verification",
                    "illustrative",
                ]
            }
        )
        conflict = False
        if old:
            previous = old["payload"]
            conflict = old["conflict"]
            different_source = previous["source_id"] != obs.source_id
            contradictory = any(previous.get(k) != body.get(k) for k in ["status", "starts_at", "ends_at"])
            if different_source and contradictory and not obs.authoritative:
                conflict = True
            if old["source_time"] > source_time or (different_source and not obs.authoritative):
                c.execute(
                    update(facts).where(facts.c.id == entity_id).values(evidence_ids=evids, conflict=conflict)
                )
                if conflict != old["conflict"]:
                    version = old["version"] + 1
                    c.execute(
                        update(facts)
                        .where(facts.c.id == entity_id)
                        .values(version=version, updated_at=time.time())
                    )
                    self.publish_fact(c, entity_id, version, previous, obs.replay)
                return
            if obs.authoritative:
                conflict = False
            if old["content_hash"] == semantic_hash and old["conflict"] == conflict:
                c.execute(update(facts).where(facts.c.id == entity_id).values(evidence_ids=evids))
                return
        version = old["version"] + 1 if old else 1
        values = {
            "version": version,
            "payload": body,
            "content_hash": semantic_hash,
            "evidence_ids": evids,
            "updated_at": time.time(),
            "source_time": source_time,
            "conflict": conflict,
        }
        if old:
            c.execute(update(facts).where(facts.c.id == entity_id).values(**values))
        else:
            self.db.insert_once(c, facts, {"id": entity_id, **values})
        self.publish_fact(c, entity_id, version, body, obs.replay)
        # Also revisit old cities when a venue moves between jurisdictions.
        for cid in set(relevant_cities(old["payload"]) if old else []) - set(relevant_cities(body)):
            self.emit(
                c,
                AGENTS[f"city:{cid}"].topic,
                entity_id,
                {"entity_id": entity_id, "version": version, "replay": obs.replay},
                digest("moved", entity_id, version, cid),
            )

    def publish_fact(self, c, entity_id, version, body, replay=False):
        targets = [AGENTS[f"city:{cid}"].topic for cid in relevant_cities(body)]
        # National agents follow their own sources even if no city was resolved.
        if not targets:
            targets = [AGENTS[f"country:{body['country']}"].topic]
        for topic in targets:
            self.emit(
                c,
                topic,
                entity_id,
                {"entity_id": entity_id, "version": version, "replay": replay},
                digest(topic, entity_id, version),
            )

    def route(self, c, topic, payload):
        eid = payload.get("_event_id") or digest(topic, payload)
        if topic == "baltic.ingest.raw.v1":
            self.normalize(c, payload)
        elif topic in TOPIC_AGENT:
            entity = c.execute(select(facts).where(facts.c.id == payload["entity_id"])).mappings().first()
            priority = 10 if entity and entity["payload"]["status"] == "cancelled" else 1
            self.enqueue(c, TOPIC_AGENT[topic], "observation", payload, eid, priority)
        elif topic in {"baltic.city.findings.v1", "baltic.country.findings.v1"}:
            sender = AGENTS[payload["agent_id"]]
            if sender.parent:
                self.enqueue(c, sender.parent, "finding", payload, eid)
            if sender.level == "city":
                self.enqueue(c, "baltic:coordinator", "finding", payload, eid)
        elif topic == "baltic.guide.messages.v1":
            self.deliver(c, payload)
        elif topic == "baltic.deadletter.v1":
            self.db.insert_once(
                c,
                deadletters,
                {"id": eid, "payload": payload, "error": payload.get("error", ""), "created_at": time.time()},
            )

    def scoped_facts(self, agent_id, c=None):
        agent = AGENTS[agent_id]
        rows = [dict(r) for r in c.execute(select(facts)).mappings()] if c else self.db.rows(facts)
        if agent.level == "city":
            rows = [r for r in rows if agent.city_id in relevant_cities(r["payload"])]
        elif agent.level == "country":
            rows = [
                r
                for r in rows
                if r["payload"]["country"] == agent.country
                or any(x.startswith(agent.country + ":") for x in r["payload"]["city_ids"])
            ]
        return rows

    def publish_finding(self, c, agent_id, finding_id, payload, replay=False):
        fingerprint = digest({k: v for k, v in payload.items() if k not in {"evidence_ids", "as_of"}})
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
        if AGENTS[agent_id].level != "baltic":
            topic = f"baltic.{AGENTS[agent_id].level}.findings.v1"
            self.emit(
                c,
                topic,
                finding_id,
                {"finding_id": finding_id, "version": version, "agent_id": agent_id, "replay": replay},
            )
        if not replay:
            self.emit(c, "baltic.guide.messages.v1", finding_id, payload)
        return True

    def deliver(self, c, payload):
        if self.db.engine.dialect.name == "postgresql":
            # Serialize sequence allocation through commit so SSE cursors cannot skip a late commit.
            c.execute(text("SELECT pg_advisory_xact_lock(81697003)"))
        if payload.get("guide_id"):
            recipients = c.execute(select(guides).where(guides.c.id == payload["guide_id"])).mappings()
        else:
            recipients = c.execute(select(guides)).mappings()
        for guide in recipients:
            previous = c.execute(
                select(notifications.c.id).where(
                    notifications.c.guide_id == guide["id"], notifications.c.finding_id == payload["id"]
                )
            ).first()
            # Corrections must reach guides who saw an earlier version, even after a date/location changes.
            if (
                not previous
                and not payload.get("guide_id")
                and not match_preferences(payload, guide["preferences"])
            ):
                continue
            nid = digest(guide["id"], payload["id"], payload["version"])
            self.db.insert_once(
                c,
                notifications,
                {
                    "id": nid,
                    "guide_id": guide["id"],
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
                    notifications.c.guide_id == guide["id"],
                    notifications.c.finding_id == payload["id"],
                    notifications.c.version < payload["version"],
                )
                .values(status="superseded")
            )

    def create_task(self, agent_id, owner, request, request_id, context_id=None, parent_task_id=None, c=None):
        if agent_id not in AGENTS:
            raise ValueError("Unknown agent")
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
                c.execute(
                    update(tasks)
                    .where(tasks.c.id == task_id)
                    .values(state="canceled", updated_at=time.time())
                )
            return dict(c.execute(select(tasks).where(tasks.c.id == task_id)).mappings().one())
