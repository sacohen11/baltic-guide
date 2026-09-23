import asyncio
import time

import httpx
import pytest
from fastapi.testclient import TestClient

from baltic.api import make_app
from baltic.db import findings, model_usage
from baltic.graphs import Reasoner
from baltic.registry import AGENTS
from baltic.settings import Settings


def test_running_workers_wake_deliver_and_delegate(tmp_path):
    app = make_app(
        Settings(
            database_url=f"sqlite:///{tmp_path}/live.db",
            checkpoint_dir=str(tmp_path / "checkpoints"),
            sources_file="missing.yaml",
            workers=3,
        )
    )
    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200
        assert client.post("/api/demo/seed").status_code == 200
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            cards = client.get("/api/messages?limit=500").json()
            if any(c["payload"]["type"] == "theme" for c in cards):
                break
            time.sleep(0.1)
        else:
            pytest.fail("Warm background workers did not deliver a theme")
        query = client.post(
            "/api/query",
            json={"message": "What changed?", "verify": True, "city_ids": ["lv:riga", "ee:tallinn"]},
        )
        assert query.status_code == 202
        tid = query.json()["task_id"]
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            task = client.get("/api/tasks/" + tid).json()
            if task["state"] == "completed":
                break
            time.sleep(0.1)
        else:
            pytest.fail("Delegated background task did not finish")
        assert len(task["result"]["verification_checks"]) == 2
        assert client.get("/readyz").status_code == 200


def test_news_mentions_reach_city_without_creating_event_theme(service, runtime, observation):
    news = observation.model_copy(
        update={"kind": "news", "city_ids": [], "candidate_city_ids": ["lv:riga", "lv:liepaja", "lv:jelgava"]}
    )
    service.ingest(news)
    asyncio.run(runtime.drain())
    cards = service.db.rows(findings)
    city_cards = [c for c in cards if c["agent_id"].startswith("city:")]
    assert len(city_cards) == 3
    assert all(c["payload"]["routing_basis"] == "source_mention" for c in city_cards)
    assert not any(c["payload"]["type"] == "theme" for c in cards)


def test_model_budget_and_failure_fallback(service, monkeypatch):
    service.settings.model = "configured-model"
    calls = []

    class Client:
        def __init__(self, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def post(self, url, **kw):
            calls.append(kw["json"])
            return httpx.Response(
                200,
                json={"choices": [{"message": {"content": "No evidence is available."}}]},
                request=httpx.Request("POST", url),
            )

    monkeypatch.setattr("baltic.graphs.httpx.Client", Client)
    reasoner = Reasoner(service)
    answer, error = reasoner.summarize(AGENTS["city:lv:riga"], "What changed?", [])
    assert answer == "No evidence is available." and error is None
    assert "untrusted data" in calls[0]["messages"][0]["content"]
    assert service.db.rows(model_usage)[0]["tokens"] > 0
    service.settings.model_daily_tokens = 1
    assert "budget" in reasoner.summarize(AGENTS["city:lv:riga"], "Again", [])[1]
    assert len(calls) == 1
    service.settings.model_daily_tokens = 200000

    def fail(*args, **kw):
        raise httpx.ConnectError("unavailable")

    monkeypatch.setattr(Client, "post", fail)
    assert "unavailable" in reasoner.summarize(AGENTS["city:lv:riga"], "Again", [])[1]
