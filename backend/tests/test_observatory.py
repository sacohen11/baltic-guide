import asyncio
import json
import time

import pytest
from sqlalchemy import update

from observatory.db import (
    agents,
    cases,
    deadletters,
    evidence,
    facts,
    inbox,
    notifications,
    observers,
    raw_records,
    tasks,
)
from observatory.gcn import GCNIngestor, parse_record
from observatory.graphs import briefing
from observatory.registry import AGENTS, GCN_TOPICS
from observatory.runtime import LeaseLost, Runtime
from observatory.schemas import digest

from conftest import accept, lvk


def test_registry_is_real_hierarchy():
    assert len(AGENTS) == 12
    assert len([a for a in AGENTS.values() if a.level == "instrument"]) == 7
    assert all(a in AGENTS for a in GCN_TOPICS.values())


@pytest.mark.asyncio
async def test_end_to_end_dedup_revision_retraction_and_restart(service, runtime):
    payload = lvk()
    accept(service, payload)
    await runtime.drain()
    case = service.db.one(cases, "lvk:S260923abc")
    assert case
    assert all(
        service.db.one(agents, a)["runs"] > 0 for a in ["lvk", "family:gravity", "sky", case["agent_id"]]
    )
    first_count = len(service.db.rows(notifications))
    accept(service, payload)  # Same Kafka offset
    accept(service, payload, 1)  # Same source payload, different offset
    await runtime.drain()
    assert len(service.db.rows(notifications)) == first_count
    assert service.db.rows(facts)[0]["version"] == 1
    assert len(service.db.rows(raw_records)) == 2
    # Recover the durable outbox in a fresh runtime.
    accept(service, lvk(kind="RETRACTION", time="2026-09-23T12:01:00Z"), 2)
    replacement = Runtime(service)
    try:
        await replacement.drain()
    finally:
        replacement.stack.close()
    assert service.db.rows(facts)[0]["version"] == 2
    assert any(
        n["payload"]["status"] == "retracted" and n["payload"].get("case_id") == case["id"]
        for n in service.db.rows(notifications)
    )
    accept(service, payload, 3)  # Late old revision cannot resurrect the event.
    await runtime.drain()
    assert service.db.rows(facts)[0]["payload"]["status"] == "retracted"
    assert len(service.db.rows(evidence)) == 4


@pytest.mark.asyncio
async def test_test_events_do_not_reach_live_observers(service, runtime):
    accept(service, lvk(name="MS260923abc"))
    await runtime.drain()
    assert service.db.rows(raw_records)
    assert service.db.rows(cases)[0]["id"].startswith("test:")
    assert not service.db.rows(notifications)


@pytest.mark.asyncio
async def test_quarantine_commits_raw_and_error_without_fabricating_fact(service, runtime):
    GCNIngestor(service).accept("igwn.gwalert", 0, 10, b'{"bad":true}')
    await runtime.drain()
    assert service.db.rows(raw_records)[0]["state"] == "quarantined"
    assert service.db.rows(deadletters)
    assert not service.db.rows(facts)


@pytest.mark.asyncio
async def test_explicit_lvk_followup_is_linked_but_not_claimed_as_detection(service, runtime):
    accept(service, lvk())
    # Shape follows the published NASA example, which has no alert_tense or event_name.
    followup = {
        "type": "IceCube LVK Alert Nu Track Search",
        "reference": {"gcn.notices.LVK.alert": "S260923abc-2-Initial"},
        "ref_ID": "S260923abc",
        "alert_datetime": "2026-09-23T12:05:00Z",
        "trigger_time": "2026-09-23T11:59:00Z",
        "n_events_coincident": 0,
        "pval_generic": None,
        "pval_bayesian": 1.0,
    }
    accept(service, followup, 0, "gcn.notices.icecube.lvk_nu_track_search")
    await runtime.drain()
    case = service.db.one(cases, "lvk:S260923abc")
    rows = service.scoped_facts(case["agent_id"])
    card = briefing(rows, "case", case["id"])
    assert card["families"] == ["gravity", "neutrino"]
    assert any(r["kind"] == "followup" for r in card["reports"])
    assert card["association"] == "explicit_references_only"
    accept(service, lvk(kind="RETRACTION", time="2026-09-23T12:10:00Z"), 1)
    await runtime.drain()
    assert briefing(service.scoped_facts(case["agent_id"]), "case", case["id"])["status"] == "retracted"


def test_icecube_array_identifiers_and_test_tense():
    body = {
        "event_name": ["IceCube-230416A"],
        "id": ["137840_57034692"],
        "record_number": 1,
        "alert_datetime": "2023-04-16T05:42:00Z",
        "alert_type": "initial",
        "alert_tense": "current",
    }
    row = parse_record("gcn.notices.icecube.gold_bronze_track_alerts", json.dumps(body), "raw")
    assert row.case_ids == ["icecube:icecube-230416a"]
    body["alert_tense"] = "injections"
    assert parse_record("gcn.notices.icecube.gold_bronze_track_alerts", json.dumps(body), "raw").test
    del body["alert_tense"]
    with pytest.raises(ValueError):
        parse_record("gcn.notices.icecube.gold_bronze_track_alerts", json.dumps(body), "raw")


def test_swift_guano_retraction_array_identifier():
    body = {
        "id": ["694215995"],
        "record_number": 4,
        "alert_datetime": "2023-01-01T03:24:36Z",
        "alert_tense": "current",
        "alert_type": "retraction",
        "trigger_time": "2022-12-31T21:46:05.13Z",
        "follow_up_event": "Fermi 694215970",
    }
    row = parse_record("gcn.notices.swift.bat.guano", json.dumps(body), "raw")
    assert row.status == "retracted"
    assert row.source_item_id == "search:694215995"


def test_classic_notice_preserves_measurements_and_unifies_swift_trigger():
    raw = "NOTICE_DATE: Wed 23 Sep 26 12:00:00 UT\nNOTICE_TYPE: Swift-XRT Position\nTRIGGER_NUM: 100004, Seg_Num: 0\nGRB_DATE: 12345 TJD; 266 DOY; 26/09/23\nGRB_TIME: 43000.5 SOD\nGRB_RA: 20.0d (J2000)\nCOMMENTS: Source text\nCOMMENTS: More text"
    row = parse_record("gcn.classic.text.SWIFT_XRT_POSITION", raw, "raw")
    assert row.case_ids == ["swift:100004"]
    assert row.metadata["classic_fields"]["GRB_RA"] == "20.0d (J2000)"
    assert row.summary == "Source text\nMore text"
    assert row.event_time.year == 2026


@pytest.mark.asyncio
async def test_circular_revision_keeps_history(service, runtime):
    body = {
        "circularId": 123,
        "subject": "S260923abc follow-up",
        "body": "No counterpart identified.",
        "createdOn": 1780000000000,
    }
    accept(service, body, 0, "gcn.circulars")
    await runtime.drain()
    body.update(body="Corrected upper limit reported.", updatedOn=1780000001000)
    accept(service, body, 1, "gcn.circulars")
    await runtime.drain()
    assert service.db.rows(facts)[0]["version"] == 2
    assert len(service.db.rows(evidence)) == 2


@pytest.mark.asyncio
async def test_retraction_reaches_prior_subscriber_after_preferences_change(service, runtime):
    accept(service, lvk())
    await runtime.drain()
    with service.db.tx() as c:
        c.execute(update(observers).values(preferences={"families": ["light"], "multi_messenger_only": True}))
    accept(service, lvk(kind="RETRACTION", time="2026-09-23T12:01:00Z"), 1)
    await runtime.drain()
    assert any(r["payload"]["status"] == "retracted" for r in service.db.rows(notifications))


@pytest.mark.asyncio
async def test_lease_fencing_recovery_and_task_idempotency(service, runtime):
    task = service.create_task("sky", "operator", {"message": "What changed?"}, "same-request")
    assert (
        service.create_task("sky", "operator", {"message": "What changed?"}, "same-request")["id"]
        == task["id"]
    )
    runtime.setup_graphs()
    job, token = runtime.claim()
    with service.db.tx() as c:
        c.execute(update(agents).where(agents.c.id == "sky").values(lease_until=time.time() - 1))
    with pytest.raises(LeaseLost):
        runtime.finish(job, token, runtime.graphs.run(job))
    await runtime.drain()
    assert service.db.one(tasks, task["id"])["state"] == "completed"


@pytest.mark.asyncio
async def test_background_wakes_sleeping_agent(service):
    runtime = Runtime(service)
    await runtime.start()
    try:
        accept(service, lvk())
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and not service.db.rows(notifications):
            await asyncio.sleep(0.05)
        assert service.db.rows(notifications)
        assert service.db.one(agents, "lvk")["runs"] > 0
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_shutdown_waits_for_inflight_graph_before_closing_checkpoint(service):
    import threading

    runtime = Runtime(service)
    runtime.setup_graphs()
    with service.db.tx() as c:
        service.enqueue(c, "lvk", "heartbeat", {}, "shutdown-test")
    started = threading.Event()
    release = threading.Event()
    original_run = runtime.graphs.run

    def slow_graph(job):
        started.set()
        assert release.wait(5)
        return original_run(job)

    runtime.graphs.run = slow_graph
    worker = asyncio.create_task(runtime.run_one())
    runtime.background = [worker]
    try:
        assert await asyncio.to_thread(started.wait, 2)
        stopping = asyncio.create_task(runtime.stop())
        await asyncio.sleep(0.05)
        assert not stopping.done()
    finally:
        release.set()
    await asyncio.wait_for(stopping, 5)
    assert worker.done()


@pytest.mark.asyncio
async def test_missing_gcn_credentials_is_explicit(service):
    service.settings.gcn_client_id = service.settings.gcn_client_secret = ""
    with pytest.raises(RuntimeError, match="credentials"):
        await GCNIngestor(service).run()


def test_no_duplicate_agent_jobs(service):
    with service.db.tx() as c:
        service.enqueue(c, "lvk", "heartbeat", {}, "same")
        service.enqueue(c, "lvk", "heartbeat", {}, "same")
    assert len(service.db.rows(inbox)) == 1
    assert service.db.rows(inbox)[0]["id"] == digest("lvk", "heartbeat", "same")


@pytest.mark.asyncio
async def test_question_cannot_swallow_pending_automatic_case_finding(service, runtime):
    accept(service, lvk())
    while await runtime.pump_one():
        pass
    case = service.db.one(cases, "lvk:S260923abc")
    service.create_task(case["agent_id"], "operator", {"message": "Check this event"}, "early-question")
    await runtime.drain()
    assert any(
        n["payload"].get("case_id") == case["id"] and n["payload"]["type"] == "case"
        for n in service.db.rows(notifications)
    )
