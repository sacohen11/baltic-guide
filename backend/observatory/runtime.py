import asyncio
import contextlib
import json
import logging
import time
from uuid import uuid4

from sqlalchemy import select, update, text

from .db import agents, inbox, outbox, tasks, facts, runtime_health, deadletters
from .graphs import Graphs
from .registry import TOPIC_AGENT, SYSTEM_TOPICS
from .schemas import digest
from .telemetry import RUNS, LATENCY, DURATION, setup

log = logging.getLogger(__name__)


class LeaseLost(Exception):
    pass


class Runtime:
    def __init__(self, service):
        self.service, self.db, self.settings = service, service.db, service.settings
        self.graphs = None
        self.running = False
        self.background = []
        self.stack = contextlib.ExitStack()
        self.producer = None
        self.consumer = None
        self.tracer = setup()
        self.instance_id = str(uuid4())
        self.health_times = {}

    def setup_graphs(self):
        if self.graphs:
            return
        if self.settings.database_url.startswith("postgresql"):
            from langgraph.checkpoint.postgres import PostgresSaver

            url = self.settings.database_url.replace("postgresql+psycopg://", "postgresql://")
            checkpointer = self.stack.enter_context(PostgresSaver.from_conn_string(url))
            with self.db.tx() as connection:
                connection.execute(text("SELECT pg_advisory_xact_lock(6248702)"))
                checkpointer.setup()
        else:
            from langgraph.checkpoint.sqlite import SqliteSaver
            from pathlib import Path

            path = Path(self.settings.checkpoint_dir)
            path.mkdir(parents=True, exist_ok=True)
            checkpointer = self.stack.enter_context(SqliteSaver.from_conn_string(str(path / "graph.db")))
        self.graphs = Graphs(self.service, checkpointer)

    async def start(self):
        self.setup_graphs()
        if self.settings.transport == "kafka":
            await self.connect_kafka()
        self.running = True
        self.background = [asyncio.create_task(self.worker_loop(i)) for i in range(self.settings.workers)]
        self.background += [asyncio.create_task(self.outbox_loop()), asyncio.create_task(self.tick_loop())]
        if self.consumer:
            self.background.append(asyncio.create_task(self.consume_loop()))

    async def stop(self):
        self.running = False
        for t in self.background:
            t.cancel()
        await asyncio.gather(*self.background, return_exceptions=True)
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()
        self.stack.close()

    async def connect_kafka(self):
        import os
        import ssl
        from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
        from aiokafka.admin import AIOKafkaAdminClient, NewTopic
        from aiokafka.errors import TopicAlreadyExistsError

        kwargs = {"bootstrap_servers": self.settings.kafka_bootstrap}
        protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
        kwargs["security_protocol"] = protocol
        if "SSL" in protocol:
            kwargs["ssl_context"] = ssl.create_default_context(cafile=os.getenv("KAFKA_CA_FILE"))
        if "SASL" in protocol:
            kwargs.update(
                sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
                sasl_plain_username=os.getenv("KAFKA_USERNAME"),
                sasl_plain_password=os.getenv("KAFKA_PASSWORD"),
            )
        admin = AIOKafkaAdminClient(**kwargs)
        await admin.start()
        try:
            existing = set(await admin.list_topics())
            new = [
                NewTopic(
                    t,
                    num_partitions=1,
                    replication_factor=self.settings.kafka_replication,
                    topic_configs={"retention.ms": str(14 * 86400 * 1000)},
                )
                for t in [*TOPIC_AGENT, *SYSTEM_TOPICS]
                if t not in existing
            ]
            if new:
                try:
                    await admin.create_topics(new)
                except TopicAlreadyExistsError:
                    pass
        finally:
            await admin.close()
        self.producer = AIOKafkaProducer(
            **kwargs, enable_idempotence=True, acks="all", value_serializer=lambda x: json.dumps(x).encode()
        )
        await self.producer.start()
        self.consumer = AIOKafkaConsumer(
            *TOPIC_AGENT,
            *SYSTEM_TOPICS,
            **kwargs,
            group_id=self.settings.kafka_group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await self.consumer.start()

    async def consume_loop(self):
        from aiokafka import TopicPartition

        while self.running:
            try:
                async for record in self.consumer:
                    tp = TopicPartition(record.topic, record.partition)
                    try:
                        body = json.loads(record.value)
                        # One transaction per record, in partition order. Offset follows durable insertion.
                        with self.db.tx() as c:
                            self.service.route(c, record.topic, body)
                    except (ValueError, KeyError, TypeError) as exc:
                        with self.db.tx() as c:
                            self.service.emit(
                                c,
                                "observatory.deadletter.v1",
                                str(record.offset),
                                {
                                    "topic": record.topic,
                                    "partition": record.partition,
                                    "offset": record.offset,
                                    "raw": record.value.decode(errors="replace")[:10000],
                                    "error": str(exc)[:500],
                                },
                                digest("kafka-invalid", record.topic, record.partition, record.offset),
                            )
                    await self.consumer.commit({tp: record.offset + 1})
                    self.health("kafka", {"connected": True})
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Kafka consumer failed; restarting from committed offsets")
                # Never continue beyond a record that was not durably handled.
                await self.consumer.stop()
                await asyncio.sleep(1)
                try:
                    await self.consumer.start()
                except Exception:
                    log.exception("Kafka reconnect failed")
                    await asyncio.sleep(3)

    def claim(self):
        now = time.time()
        with self.db.tx() as c:
            # Order agents by last run to prevent a noisy instrument monopolizing workers.
            available = select(inbox.c.agent_id).where(
                inbox.c.state.in_(["pending", "running"]), inbox.c.available_at <= now
            )
            candidates = (
                c.execute(
                    select(agents.c.id)
                    .where(agents.c.id.in_(available), agents.c.lease_until < now)
                    .order_by(agents.c.last_run)
                    .limit(100)
                )
                .scalars()
                .all()
            )
            for aid in candidates:
                token = str(uuid4())
                claimed = c.execute(
                    update(agents)
                    .where(agents.c.id == aid, agents.c.lease_until < now)
                    .values(lease_token=token, lease_until=now + self.settings.lease_seconds)
                ).rowcount
                if not claimed:
                    continue
                row = (
                    c.execute(
                        select(inbox)
                        .where(
                            inbox.c.agent_id == aid,
                            inbox.c.state.in_(["pending", "running"]),
                            inbox.c.available_at <= now,
                        )
                        .order_by(inbox.c.priority.desc(), inbox.c.created_at)
                        .limit(1)
                    )
                    .mappings()
                    .first()
                )
                if not row:
                    c.execute(
                        update(agents).where(agents.c.id == aid).values(lease_until=0, lease_token=None)
                    )
                    continue
                job = dict(row)
                if job["kind"] == "task":
                    task_state = c.execute(
                        select(tasks.c.state).where(tasks.c.id == job["payload"]["task_id"])
                    ).scalar_one()
                    if task_state == "canceled":
                        c.execute(update(inbox).where(inbox.c.id == job["id"]).values(state="done"))
                        c.execute(
                            update(agents).where(agents.c.id == aid).values(lease_until=0, lease_token=None)
                        )
                        continue
                    waiting = c.execute(
                        select(tasks.c.id)
                        .where(
                            tasks.c.parent_task_id == job["payload"]["task_id"],
                            tasks.c.state.in_(["submitted", "working"]),
                        )
                        .limit(1)
                    ).first()
                    if waiting:
                        c.execute(update(inbox).where(inbox.c.id == job["id"]).values(available_at=now + 0.5))
                        c.execute(
                            update(agents).where(agents.c.id == aid).values(lease_until=0, lease_token=None)
                        )
                        continue
                c.execute(
                    update(inbox)
                    .where(inbox.c.id == job["id"])
                    .values(state="running", attempts=job["attempts"] + 1)
                )
                if job["kind"] == "task":
                    c.execute(
                        update(tasks)
                        .where(tasks.c.id == job["payload"]["task_id"], tasks.c.state == "submitted")
                        .values(state="working", updated_at=now)
                    )
                return job, token
        return None

    async def renew(self, aid, token):
        while True:
            await asyncio.sleep(max(0.1, self.settings.lease_seconds / 3))
            with self.db.tx() as c:
                c.execute(
                    update(agents)
                    .where(
                        agents.c.id == aid, agents.c.lease_token == token, agents.c.lease_until > time.time()
                    )
                    .values(lease_until=time.time() + self.settings.lease_seconds, last_heartbeat=time.time())
                )

    def finish(self, job, token, result):
        now = time.time()
        with self.db.tx() as c:
            claimed = c.execute(
                update(agents)
                .where(
                    agents.c.id == job["agent_id"], agents.c.lease_token == token, agents.c.lease_until > now
                )
                .values(
                    memory=result["memory"],
                    last_run=now,
                    last_heartbeat=now,
                    last_error=None,
                    runs=agents.c.runs + 1,
                    lease_token=None,
                    lease_until=0,
                )
            ).rowcount
            if not claimed:
                raise LeaseLost()
            replay = job["payload"].get("replay", False)
            # A cancellation/update arriving during inference invalidates the run snapshot.
            for row in result["facts"]:
                current = c.execute(
                    select(facts.c.version).where(facts.c.id == row["id"]).with_for_update()
                ).scalar_one()
                if current != row["version"]:
                    raise ValueError("Evidence changed during the run; retrying with fresh facts")
            if job["kind"] == "task":
                tid = job["payload"]["task_id"]
                task = c.execute(select(tasks).where(tasks.c.id == tid).with_for_update()).mappings().one()
                if task["state"] != "canceled":
                    children = c.execute(select(tasks).where(tasks.c.parent_task_id == tid)).mappings().all()
                    if any(t["state"] in {"submitted", "working"} for t in children):
                        c.execute(
                            update(inbox)
                            .where(inbox.c.id == job["id"])
                            .values(state="pending", available_at=now + 0.5, attempts=0)
                        )
                        return
                    answer = {
                        **result["result"],
                        "id": tid,
                        "version": 1,
                        "agent_id": job["agent_id"],
                        "as_of": datetime_iso(now),
                        "delegated_tasks": [t["id"] for t in children],
                    }
                    c.execute(
                        update(tasks)
                        .where(tasks.c.id == tid)
                        .values(state="completed", result=answer, updated_at=now)
                    )
                    if not task["owner"].startswith("agent:"):
                        self.service.emit(
                            c,
                            "observatory.observer.messages.v1",
                            tid,
                            {**answer, "observer_id": task["owner"]},
                        )
            else:
                for proposal in result["proposals"]:
                    self.service.publish_finding(
                        c, job["agent_id"], proposal["id"], proposal["payload"], replay
                    )
            c.execute(
                update(inbox)
                .where(inbox.c.id == job["id"])
                .values(state="done", error=None, checkpoint={"finished_at": now, "memory": result["memory"]})
            )

    def fail(self, job, token, exc):
        with self.db.tx() as c:
            owned = c.execute(
                update(agents)
                .where(agents.c.id == job["agent_id"], agents.c.lease_token == token)
                .values(lease_token=None, lease_until=0, last_error=str(exc)[:500], last_run=time.time())
            ).rowcount
            if not owned:
                return
            tries = c.execute(select(inbox.c.attempts).where(inbox.c.id == job["id"])).scalar_one()
            dead = tries >= self.settings.max_attempts
            c.execute(
                update(inbox)
                .where(inbox.c.id == job["id"])
                .values(
                    state="dead" if dead else "pending",
                    available_at=time.time() + min(60, 2**tries),
                    error=str(exc)[:1000],
                )
            )
            if dead:
                self.service.emit(
                    c, "observatory.deadletter.v1", job["id"], {"job": job, "error": str(exc)[:1000]}
                )
                if job["kind"] == "task":
                    c.execute(
                        update(tasks)
                        .where(tasks.c.id == job["payload"]["task_id"], tasks.c.state != "canceled")
                        .values(state="failed", error=str(exc)[:500], updated_at=time.time())
                    )

    async def run_one(self):
        claim = self.claim()
        if not claim:
            return False
        job, token = claim
        level = self.service.agent(job["agent_id"]).level
        LATENCY.observe(max(0, time.time() - job["created_at"]))
        renewing = asyncio.create_task(self.renew(job["agent_id"], token))
        start = time.time()
        try:
            with self.tracer.start_as_current_span("agent.run") as span:
                span.set_attribute("agent.id", job["agent_id"])
                span.set_attribute("job.id", job["id"])
                result = await asyncio.to_thread(self.graphs.run, job)
                self.finish(job, token, result)
            RUNS.labels(level, "success").inc()
        except LeaseLost:
            RUNS.labels(level, "lease_lost").inc()
        except asyncio.CancelledError:
            # The in-flight thread may finish, but cannot publish without finish(). Lease expires on recovery.
            raise
        except Exception as exc:
            log.exception("Agent run failed: %s", job["agent_id"])
            self.fail(job, token, exc)
            RUNS.labels(level, "failed").inc()
        finally:
            renewing.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await renewing
            DURATION.labels(level).observe(time.time() - start)
        return True

    async def worker_loop(self, index):
        while self.running:
            try:
                self.health(f"worker:{index}", {"healthy": True})
                if not await self.run_one():
                    await asyncio.sleep(self.settings.poll_seconds)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Worker loop error")
                await asyncio.sleep(1)

    def claim_outbox(self):
        now = time.time()
        with self.db.tx() as c:
            candidates = (
                c.execute(
                    select(outbox.c.id)
                    .where(outbox.c.published_at.is_(None), outbox.c.lease_until < now)
                    .order_by(outbox.c.created_at)
                    .limit(20)
                )
                .scalars()
                .all()
            )
            for eid in candidates:
                token = str(uuid4())
                if c.execute(
                    update(outbox)
                    .where(outbox.c.id == eid, outbox.c.published_at.is_(None), outbox.c.lease_until < now)
                    .values(lease_token=token, lease_until=now + 60, attempts=outbox.c.attempts + 1)
                ).rowcount:
                    return dict(c.execute(select(outbox).where(outbox.c.id == eid)).mappings().one())
        return None

    async def pump_one(self):
        row = self.claim_outbox()
        if not row:
            return False
        try:
            if self.settings.transport == "kafka":
                await self.producer.send_and_wait(row["topic"], row["payload"], key=row["key"].encode())
            with self.db.tx() as c:
                if self.settings.transport == "local":
                    self.service.route(c, row["topic"], row["payload"])
                c.execute(
                    update(outbox)
                    .where(outbox.c.id == row["id"], outbox.c.lease_token == row["lease_token"])
                    .values(published_at=time.time(), lease_token=None, lease_until=0, error=None)
                )
        except Exception as exc:
            with self.db.tx() as c:
                if (
                    isinstance(exc, (ValueError, KeyError, TypeError))
                    and row["attempts"] >= self.settings.max_attempts
                ):
                    self.db.insert_once(
                        c,
                        deadletters,
                        {
                            "id": row["id"],
                            "payload": row["payload"],
                            "error": str(exc)[:1000],
                            "created_at": time.time(),
                        },
                    )
                    c.execute(
                        update(outbox)
                        .where(outbox.c.id == row["id"], outbox.c.lease_token == row["lease_token"])
                        .values(published_at=time.time(), error=str(exc)[:1000])
                    )
                else:
                    c.execute(
                        update(outbox)
                        .where(outbox.c.id == row["id"], outbox.c.lease_token == row["lease_token"])
                        .values(
                            lease_until=time.time() + min(60, 2 ** min(6, row["attempts"])),
                            error=str(exc)[:1000],
                        )
                    )
            log.warning("Outbox delivery failed: %s", type(exc).__name__)
        return True

    async def outbox_loop(self):
        while self.running:
            try:
                if not await self.pump_one():
                    await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Outbox loop error")
                await asyncio.sleep(1)

    def health(self, role, payload):
        if time.time() - self.health_times.get(role, 0) < 15:
            return
        self.health_times[role] = time.time()
        hid = f"{self.instance_id}:{role}"
        with self.db.tx() as c:
            self.db.insert_once(c, runtime_health, {"id": hid, "updated_at": time.time(), "payload": payload})
            c.execute(
                update(runtime_health)
                .where(runtime_health.c.id == hid)
                .values(updated_at=time.time(), payload=payload)
            )

    def tick(self):
        now = time.time()
        with self.db.tx() as c:
            due = c.execute(select(agents).where(agents.c.next_tick <= now)).mappings().all()
            for a in due:
                level = self.service.agent(a["id"]).level
                interval = {
                    "instrument": 300,
                    "family": 600,
                    "coordinator": 900,
                    "circulars": 300,
                    "case": 1800,
                }[level]
                if c.execute(
                    update(agents)
                    .where(agents.c.id == a["id"], agents.c.next_tick <= now)
                    .values(next_tick=now + interval, last_heartbeat=now)
                ).rowcount:
                    self.service.enqueue(c, a["id"], "heartbeat", {}, digest(a["id"], int(now // interval)))

    async def tick_loop(self):
        while self.running:
            try:
                self.tick()
                self.health("scheduler", {"healthy": True})
            except Exception:
                log.exception("Scheduler error")
            await asyncio.sleep(30)

    async def drain(self, limit=1000):
        """Durable local transport pump for acceptance tests."""
        self.setup_graphs()
        if self.settings.transport != "local":
            raise ValueError("drain is for local transport only")
        for _ in range(limit):
            progressed = await self.pump_one()
            progressed = await self.run_one() or progressed
            if not progressed:
                return
        raise RuntimeError("Drain limit reached; possible routing loop")


def datetime_iso(timestamp):
    from datetime import UTC, datetime

    return datetime.fromtimestamp(timestamp, UTC).isoformat()
