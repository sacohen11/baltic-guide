import time
from contextlib import contextmanager

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    Float,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    event,
    select,
)
from sqlalchemy.pool import StaticPool

metadata = MetaData()
agents = Table(
    "agents",
    metadata,
    Column("id", String(100), primary_key=True),
    Column("definition", JSON, nullable=False),
    Column("memory", JSON, nullable=False, default=dict),
    Column("lease_token", String(80)),
    Column("lease_until", Float, nullable=False, default=0),
    Column("last_run", Float, default=0),
    Column("last_heartbeat", Float, default=0),
    Column("next_tick", Float, default=0),
    Column("last_error", Text),
    Column("runs", Integer, default=0),
)
inbox = Table(
    "inbox",
    metadata,
    Column("id", String(80), primary_key=True),
    Column("agent_id", String(100), nullable=False, index=True),
    Column("kind", String(40), nullable=False),
    Column("payload", JSON, nullable=False),
    Column("state", String(20), default="pending", nullable=False),
    Column("priority", Integer, default=0),
    Column("created_at", Float, default=time.time),
    Column("available_at", Float, default=time.time),
    Column("attempts", Integer, default=0),
    Column("error", Text),
    Column("checkpoint", JSON),
)
Index("inbox_runnable", inbox.c.state, inbox.c.available_at, inbox.c.agent_id)
outbox = Table(
    "outbox",
    metadata,
    Column("id", String(80), primary_key=True),
    Column("topic", String(200), nullable=False),
    Column("key", String(200), nullable=False),
    Column("payload", JSON, nullable=False),
    Column("created_at", Float, default=time.time),
    Column("published_at", Float),
    Column("lease_until", Float, default=0),
    Column("lease_token", String(80)),
    Column("attempts", Integer, default=0),
    Column("error", Text),
)
evidence = Table(
    "evidence",
    metadata,
    Column("id", String(80), primary_key=True),
    Column("entity_id", String(80), index=True),
    Column("source_id", String(120), index=True),
    Column("content_hash", String(80)),
    Column("payload", JSON, nullable=False),
    Column("created_at", Float, default=time.time),
)
facts = Table(
    "facts",
    metadata,
    Column("id", String(128), primary_key=True),
    Column("version", Integer, nullable=False),
    Column("content_hash", String(80)),
    Column("payload", JSON, nullable=False),
    Column("evidence_ids", JSON, nullable=False),
    Column("updated_at", Float, default=time.time),
    Column("source_time", Float, default=0),
    Column("conflict", Boolean, default=False),
)
findings = Table(
    "findings",
    metadata,
    Column("id", String(128), primary_key=True),
    Column("agent_id", String(100), index=True),
    Column("version", Integer, nullable=False),
    Column("fingerprint", String(80)),
    Column("payload", JSON, nullable=False),
    Column("updated_at", Float, default=time.time),
)
guides = Table(
    "guides",
    metadata,
    Column("id", String(100), primary_key=True),
    Column("name", String(200)),
    Column("token_hash", String(80), unique=True),
    Column("preferences", JSON, default=dict),
)
notifications = Table(
    "notifications",
    metadata,
    Column("seq", Integer, primary_key=True, autoincrement=True),
    Column("id", String(80), unique=True, nullable=False),
    Column("guide_id", String(100), index=True),
    Column("finding_id", String(128)),
    Column("version", Integer),
    Column("payload", JSON, nullable=False),
    Column("status", String(30), default="unread"),
    Column("saved", Boolean, default=False),
    Column("created_at", Float, default=time.time),
)
tasks = Table(
    "tasks",
    metadata,
    Column("id", String(80), primary_key=True),
    Column("request_key", String(80), unique=True),
    Column("agent_id", String(100), index=True),
    Column("owner", String(100), index=True),
    Column("parent_task_id", String(80), index=True),
    Column("context_id", String(100)),
    Column("state", String(30), default="submitted"),
    Column("request", JSON, nullable=False),
    Column("result", JSON),
    Column("created_at", Float, default=time.time),
    Column("updated_at", Float, default=time.time),
    Column("error", Text),
)
sources = Table(
    "sources",
    metadata,
    Column("id", String(120), primary_key=True),
    Column("definition", JSON),
    Column("last_success", Float, default=0),
    Column("last_attempt", Float, default=0),
    Column("next_poll", Float, default=0),
    Column("error", Text),
    Column("item_count", Integer, default=0),
    Column("etag", Text),
    Column("modified", Text),
    Column("failures", Integer, default=0),
)
deadletters = Table(
    "deadletters",
    metadata,
    Column("id", String(80), primary_key=True),
    Column("payload", JSON),
    Column("error", Text),
    Column("created_at", Float, default=time.time),
)
runtime_health = Table(
    "runtime_health",
    metadata,
    Column("id", String(100), primary_key=True),
    Column("updated_at", Float),
    Column("payload", JSON),
)
model_usage = Table(
    "model_usage", metadata, Column("day", String(20), primary_key=True), Column("tokens", Integer, default=0)
)
schema_versions = Table("schema_versions", metadata, Column("version", Integer, primary_key=True))


class Database:
    def __init__(self, url):
        opts = {"pool_pre_ping": True}
        if url.startswith("sqlite"):
            opts["connect_args"] = {"check_same_thread": False, "timeout": 30}
            if ":memory:" in url:
                opts["poolclass"] = StaticPool
        self.engine = create_engine(url, **opts)
        if url.startswith("sqlite"):

            @event.listens_for(self.engine, "connect")
            def configure(conn, _):
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA busy_timeout=30000")

    def initialize(self):
        with self.tx() as c:
            if self.engine.dialect.name == "postgresql":
                from sqlalchemy import text

                c.execute(text("SELECT pg_advisory_xact_lock(81697001)"))
            metadata.create_all(c)
            self.insert_once(c, schema_versions, {"version": 1}, "version")

    @contextmanager
    def tx(self):
        with self.engine.begin() as conn:
            yield conn

    def insert_once(self, conn, table, values, key="id"):
        if self.engine.dialect.name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert
        else:
            from sqlalchemy.dialects.sqlite import insert
        result = conn.execute(insert(table).values(**values).on_conflict_do_nothing(index_elements=[key]))
        return result.rowcount > 0

    def rows(self, table, where=None, limit=None, order=None):
        stmt = select(table)
        if where is not None:
            stmt = stmt.where(where)
        if order is not None:
            stmt = stmt.order_by(order)
        if limit:
            stmt = stmt.limit(limit)
        with self.engine.connect() as c:
            return [dict(r) for r in c.execute(stmt).mappings()]

    def one(self, table, id):
        rows = self.rows(table, table.c.id == id)
        return rows[0] if rows else None


source_items = Table(
    "source_items",
    metadata,
    Column("id", String(80), primary_key=True),
    Column("entity_id", String(128)),
    Column("content_hash", String(80)),
    Column("source_time", Float, default=0),
)
