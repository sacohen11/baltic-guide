"""Real Kafka + PostgreSQL acceptance test. Enabled by TEST_DATABASE_URL and TEST_KAFKA_BOOTSTRAP."""

import asyncio
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from baltic.db import Database, facts, notifications, metadata
from baltic.runtime import Runtime
from baltic.schemas import Observation
from baltic.service import Service
from baltic.settings import Settings


@pytest.mark.asyncio
async def test_real_kafka_postgres_delivery_restart_and_cancellation(tmp_path):
    url = os.getenv("TEST_DATABASE_URL")
    broker = os.getenv("TEST_KAFKA_BOOTSTRAP")
    if not url or not broker:
        pytest.skip("Set TEST_DATABASE_URL and TEST_KAFKA_BOOTSTRAP for integration test")
    from sqlalchemy.engine import make_url

    if "test" not in (make_url(url).database or ""):
        pytest.fail("Integration tests require a disposable database with test in its name")
    cfg = Settings(
        database_url=url,
        transport="kafka",
        kafka_bootstrap=broker,
        embedded_runtime=False,
        sources_file="missing.yaml",
        demo_mode=True,
        workers=3,
    )
    cfg.kafka_group = "baltic-test-" + uuid4().hex
    db = Database(url)
    metadata.drop_all(db.engine)
    service = Service(db, cfg)
    service.bootstrap()
    runtime = Runtime(service)
    uid = uuid4().hex
    start = datetime.now(UTC) + timedelta(days=2)
    observation = Observation(
        source_id="integration",
        source_item_id=uid,
        entity_id=uid,
        country="lv",
        city_ids=["lv:riga"],
        kind="event",
        title="Integration event",
        source_url="https://example.org/" + uid,
        starts_at=start,
        ends_at=start + timedelta(days=1),
        status="scheduled",
        authoritative=True,
    )

    async def until(predicate, seconds=45):
        deadline = asyncio.get_running_loop().time() + seconds
        while asyncio.get_running_loop().time() < deadline:
            if predicate():
                return
            await asyncio.sleep(0.2)
        pytest.fail("Timed out waiting for durable delivery")

    await runtime.start()
    try:
        service.ingest(observation)
        await until(lambda: any(n["payload"].get("entity_ids") == [uid] for n in db.rows(notifications)))
        first = [n for n in db.rows(notifications) if n["payload"].get("entity_ids") == [uid]]
        assert len(first) == 1
    finally:
        await runtime.stop()
    replacement = Runtime(service)
    await replacement.start()
    try:
        service.ingest(observation)
        service.ingest(
            observation.model_copy(update={"status": "cancelled", "updated_at": datetime.now(UTC)})
        )
        await until(
            lambda: any(
                n["payload"].get("entity_ids") == [uid] and n["payload"]["status"] == "retracted"
                for n in db.rows(notifications)
            )
        )
        assert db.one(facts, uid)["version"] == 2
        assert len([n for n in db.rows(notifications) if n["payload"].get("entity_ids") == [uid]]) == 2
    finally:
        await replacement.stop()
        db.engine.dispose()
