from datetime import UTC, datetime, timedelta

from sqlalchemy import update

from baltic.db import agents, inbox, outbox, facts, findings, notifications, tasks
from baltic.runtime import Runtime, LeaseLost
from baltic.registry import AGENTS


async def test_all_agents_and_duplicate_delivery(service, runtime, observation):
    assert len(AGENTS) == 34
    for country in ["ee", "lv", "lt"]:
        assert len([a for a in AGENTS.values() if a.country == country and a.level == "city"]) == 10
    service.ingest(observation)
    service.ingest(observation.model_copy(update={"message_id": "different-id"}))
    await runtime.drain()
    assert len(service.db.rows(facts)) == 1
    cards = service.db.rows(notifications)
    assert len(cards) == 1
    assert cards[0]["payload"]["title"] == observation.title
    assert service.db.one(agents, "city:lv:riga")["runs"] == 1
    # Raw replay is safe across restarts.
    with service.db.tx() as c:
        c.execute(update(outbox).values(published_at=None, lease_until=0))
    await runtime.drain()
    assert len(service.db.rows(notifications)) == 1


async def test_cancel_retracts_city_and_country_theme(service, runtime, observation):
    cities = ["lv:riga", "lv:liepaja", "lv:jelgava"]
    for i, city in enumerate(cities):
        service.ingest(
            observation.model_copy(
                update={
                    "city_ids": [city],
                    "source_item_id": str(i),
                    "entity_id": f"event-{i}",
                    "source_url": f"https://example.org/{i}",
                }
            )
        )
    await runtime.drain()
    theme = [r for r in service.db.rows(findings) if r["payload"]["type"] == "theme"]
    assert len(theme) == 1 and theme[0]["payload"]["coverage"]["cities"] == 3
    canceled = observation.model_copy(
        update={
            "source_item_id": "0",
            "entity_id": "event-0",
            "source_url": "https://example.org/0",
            "status": "cancelled",
            "updated_at": datetime.now(UTC),
        }
    )
    service.ingest(canceled)
    await runtime.drain()
    theme = service.db.one(findings, theme[0]["id"])
    assert theme["payload"]["status"] == "retracted"
    assert theme["version"] == 2
    assert any(
        n["payload"]["type"] == "disruption" and n["payload"]["status"] == "retracted"
        for n in service.db.rows(notifications)
    )


async def test_cross_country_overlap_and_distinct_events(service, runtime, observation):
    for i, city in enumerate(["lv:riga", "ee:tallinn", "lt:vilnius"]):
        service.ingest(
            observation.model_copy(
                update={
                    "country": city[:2],
                    "city_ids": [city],
                    "source_item_id": str(i),
                    "entity_id": str(i),
                    "source_url": f"https://example.org/{i}",
                }
            )
        )
    await runtime.drain()
    themes = [r["payload"] for r in service.db.rows(findings) if r["payload"]["type"] == "theme"]
    assert len(themes) == 1
    assert themes[0]["coverage"] == {"cities": 3, "countries": 3, "events": 3}
    assert themes[0]["agent_id"] == "baltic:coordinator"


async def test_disjoint_dates_do_not_make_theme(service, runtime, observation):
    service.ingest(observation)
    later = observation.starts_at + timedelta(days=40)
    service.ingest(
        observation.model_copy(
            update={
                "country": "ee",
                "city_ids": ["ee:tallinn"],
                "source_item_id": "b",
                "entity_id": "b",
                "source_url": "https://example.org/b",
                "starts_at": later,
                "ends_at": later + timedelta(days=2),
            }
        )
    )
    await runtime.drain()
    assert not any(r["payload"]["type"] == "theme" for r in service.db.rows(findings))


async def test_single_active_run_and_expired_lease_fencing(service, runtime, observation):
    service.ingest(observation)
    while await runtime.pump_one():
        pass
    job, token = runtime.claim()
    assert job["agent_id"] == "city:lv:riga"
    assert runtime.claim() is None
    result = runtime.graphs.run(job)
    with service.db.tx() as c:
        c.execute(update(agents).where(agents.c.id == job["agent_id"]).values(lease_until=0))
    recovered, new_token = runtime.claim()
    assert recovered["id"] == job["id"] and new_token != token
    import pytest

    with pytest.raises(LeaseLost):
        runtime.finish(job, token, result)
    runtime.finish(recovered, new_token, result)
    await runtime.drain()
    assert len(service.db.rows(notifications)) == 1


async def test_restart_after_inbox_persisted(service, runtime, observation):
    service.ingest(observation)
    while await runtime.pump_one():
        pass
    assert service.db.rows(inbox)[0]["state"] == "pending"
    replacement = Runtime(service)
    try:
        await replacement.drain()
        assert len(service.db.rows(notifications)) == 1
    finally:
        replacement.stack.close()


async def test_out_of_order_updates_are_ignored(service, runtime, observation):
    now = datetime.now(UTC)
    service.ingest(observation.model_copy(update={"status": "cancelled", "updated_at": now}))
    await runtime.drain()
    service.ingest(observation.model_copy(update={"updated_at": now - timedelta(days=1)}))
    await runtime.drain()
    assert service.db.one(facts, observation.entity_id)["payload"]["status"] == "cancelled"


async def test_multicity_routes_all_agents(service, runtime, observation):
    service.ingest(observation.model_copy(update={"city_ids": ["lv:riga", "ee:tallinn"]}))
    await runtime.drain()
    assert service.db.one(agents, "city:lv:riga")["memory"]["fact_count"] == 1
    assert service.db.one(agents, "city:ee:tallinn")["memory"]["fact_count"] == 1


async def test_national_event_wakes_country(service, runtime, observation):
    service.ingest(observation.model_copy(update={"city_ids": [], "kind": "news"}))
    await runtime.drain()
    assert service.db.one(agents, "country:lv")["runs"] >= 1
    assert service.db.one(agents, "city:lv:riga")["runs"] == 0


async def test_task_cancellation_suppresses_answer(service, runtime, observation):
    task = service.create_task("baltic:coordinator", "demo", {"message": "What changed?"}, "request-1")
    service.cancel_task(task["id"], "demo")
    await runtime.drain()
    assert service.db.one(tasks, task["id"])["state"] == "canceled"
    assert not service.db.rows(notifications)


async def test_subscription_and_correction_even_after_trip_window_changes(service, runtime, observation):
    from baltic.db import guides

    with service.db.tx() as c:
        c.execute(
            update(guides)
            .where(guides.c.id == "demo")
            .values(preferences={"city_ids": ["lv:riga"], "ends_at": observation.ends_at.isoformat()})
        )
    service.ingest(observation)
    await runtime.drain()
    assert len(service.db.rows(notifications)) == 1
    shifted = observation.model_copy(
        update={
            "starts_at": observation.starts_at + timedelta(days=100),
            "ends_at": observation.ends_at + timedelta(days=100),
            "updated_at": datetime.now(UTC),
        }
    )
    service.ingest(shifted)
    await runtime.drain()
    cards = service.db.rows(notifications)
    assert len(cards) == 2
    assert cards[0]["status"] == "superseded"


async def test_syndication_no_duplicate_alert(service, runtime, observation):
    service.ingest(observation)
    await runtime.drain()
    for i in range(10):
        service.ingest(
            observation.model_copy(
                update={
                    "source_id": f"other-{i}",
                    "authoritative": False,
                    "source_item_id": f"other-{i}",
                    "source_url": f"https://example.org/syndicated/{i}",
                }
            )
        )
    await runtime.drain()
    assert len(service.db.rows(notifications)) == 1
    assert len(service.db.one(facts, observation.entity_id)["evidence_ids"]) == 11


async def test_expiration_retracts_recommendation(service, runtime, observation):
    obs = observation.model_copy(
        update={
            "starts_at": datetime.now(UTC) - timedelta(days=2),
            "ends_at": datetime.now(UTC) - timedelta(days=1),
        }
    )
    service.ingest(obs)
    await runtime.drain()
    runtime.tick()
    await runtime.drain()
    assert service.db.one(facts, obs.entity_id)["payload"]["status"] == "expired"
