"""Disposable, real Kafka + PostgreSQL test used by CI, never against a production database."""

import asyncio
import os
import time
from uuid import uuid4

import pytest
from sqlalchemy.engine import make_url

from observatory.db import Database, agents, deadletters, facts, inbox, metadata, notifications, outbox
from observatory.runtime import Runtime
from observatory.service import Service
from observatory.settings import Settings
from conftest import accept, lvk


@pytest.mark.asyncio
async def test_real_kafka_postgres_delivery_restart_retraction():
    url, broker = os.getenv("TEST_DATABASE_URL"), os.getenv("TEST_KAFKA_BOOTSTRAP")
    if not url or not broker:
        pytest.skip("Requires disposable PostgreSQL and Kafka; runs in CI")
    assert "test" in (make_url(url).database or ""), "Refusing a non-test database"
    settings = Settings(
        database_url=url,
        kafka_bootstrap=broker,
        transport="kafka",
        workers=3,
        admin_token="test-only-integration-token-24-characters",
        model="",
        model_api_key="",
        kafka_group="observatory-test-" + uuid4().hex,
    )
    db = Database(url)
    metadata.drop_all(db.engine)
    service = Service(db, settings)
    service.bootstrap()

    async def until(predicate):
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            if predicate():
                return
            await asyncio.sleep(0.1)
        pytest.fail(
            "Timed out waiting for durable Kafka delivery; "
            f"inbox={[(r['agent_id'], r['state'], r['error']) for r in db.rows(inbox)]}; "
            f"outbox={[(r['topic'], r['published_at'], r['error']) for r in db.rows(outbox)]}; "
            f"agents={[(r['id'], r['runs'], r['last_error']) for r in db.rows(agents)]}; "
            f"deadletters={db.rows(deadletters)}"
        )

    runtime = Runtime(service)
    await runtime.start()
    try:
        accept(service, lvk())
        await until(
            lambda: any(n["payload"].get("case_id") == "lvk:S260923abc" for n in db.rows(notifications))
        )
    finally:
        await runtime.stop()
    replacement = Runtime(service)
    await replacement.start()
    try:
        accept(service, lvk(), 1)
        accept(service, lvk(kind="RETRACTION", time="2026-09-23T12:01:00Z"), 2)
        await until(
            lambda: any(
                n["payload"].get("case_id") == "lvk:S260923abc" and n["payload"]["status"] == "retracted"
                for n in db.rows(notifications)
            )
        )
        assert db.rows(facts)[0]["version"] == 2
    finally:
        await replacement.stop()
        db.engine.dispose()
