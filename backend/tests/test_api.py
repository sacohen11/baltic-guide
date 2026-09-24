import asyncio
import time

import pytest
from fastapi.testclient import TestClient

from observatory.api import make_app
from observatory.db import tasks


@pytest.fixture
def app(service):
    return make_app(service.settings)


def headers(app):
    return {"Authorization": "Bearer " + app.state.service.settings.admin_token}


def test_authentication_cors_empty_state_and_no_demo(app):
    client = TestClient(app)
    assert client.get("/api/overview").status_code == 401
    response = client.get("/api/overview", headers=headers(app))
    assert response.status_code == 200
    assert response.json()["reports"] == 0
    assert response.json()["agents"] == 12
    assert client.get("/readyz").status_code == 503
    assert client.post("/api/demo/seed", headers=headers(app)).status_code == 404
    response = client.options(
        "/api/overview",
        headers={
            "Origin": "https://sacohen11.github.io",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "authorization",
        },
    )
    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "https://sacohen11.github.io"
    response = client.options(
        "/api/overview",
        headers={"Origin": "https://untrusted.invalid", "Access-Control-Request-Method": "GET"},
    )
    assert "access-control-allow-origin" not in response.headers


def test_observer_tokens_are_hashed_and_task_ownership_enforced(app):
    client = TestClient(app)
    created = client.post("/api/observers", headers=headers(app), json={"name": "Researcher"})
    assert created.status_code == 201
    observer = {"Authorization": "Bearer " + created.json()["token"]}
    assert client.get("/api/overview", headers=observer).json()["role"] == "observer"
    assert client.post("/api/observers", headers=observer, json={"name": "Other"}).status_code == 403
    assert client.get("/api/deadletters", headers=observer).status_code == 403
    task = client.post("/api/tasks", headers=headers(app), json={"message": "Review the sky"}).json()
    assert client.get("/api/tasks/" + task["id"], headers=observer).status_code == 404
    assert client.post("/api/tasks/" + task["id"] + "/cancel", headers=observer).status_code == 404


def test_a2a_card_declares_protocol_and_security(app):
    client = TestClient(app)
    result = client.get("/a2a/sky/.well-known/agent-card.json", headers=headers(app))
    assert result.status_code == 200
    assert result.json()["supportedInterfaces"][0]["protocolVersion"] == "1.0"
    assert "bearer" in result.json()["securitySchemes"]


@pytest.mark.asyncio
async def test_query_delegation_finishes_durably(app):
    client = TestClient(app)
    response = client.post(
        "/api/tasks",
        headers=headers(app),
        json={
            "message": "Review gravity evidence",
            "verify": True,
            "families": ["gravity"],
            "request_id": "verify-once",
        },
    )
    assert response.status_code == 202
    tid = response.json()["id"]
    runtime = app.state.runtime
    try:
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            await runtime.drain()
            if app.state.service.db.one(tasks, tid)["state"] == "completed":
                break
            await asyncio.sleep(0.1)
        result = client.get("/api/tasks/" + tid, headers=headers(app)).json()
        assert result["state"] == "completed"
        assert result["result"]["delegated_tasks"]
        assert result["result"]["child_results"]
        assert not app.state.service.db.rows(tasks, tasks.c.state.in_(["working", "submitted"]))
    finally:
        runtime.stack.close()


def test_preferences_validation(app):
    client = TestClient(app)
    assert (
        client.put("/api/preferences", headers=headers(app), json={"families": ["not-real"]}).status_code
        == 422
    )
    assert (
        client.put("/api/preferences", headers=headers(app), json={"instruments": ["not-real"]}).status_code
        == 422
    )
    assert (
        client.put(
            "/api/preferences",
            headers=headers(app),
            json={"families": ["gravity"], "multi_messenger_only": True},
        ).status_code
        == 200
    )


def test_a2a_send_message_and_cancel_descendants(app):
    client = TestClient(app)
    response = client.post(
        "/a2a/sky/",
        headers={**headers(app), "A2A-Version": "1.0"},
        json={
            "jsonrpc": "2.0",
            "id": "rpc-1",
            "method": "SendMessage",
            "params": {
                "message": {
                    "messageId": "a2a-request-1",
                    "role": "ROLE_USER",
                    "parts": [{"text": "Review current evidence"}],
                },
                "configuration": {"returnImmediately": True},
            },
        },
    )
    assert response.status_code == 200
    assert "error" not in response.json(), response.json()
    created = client.post(
        "/api/tasks", headers=headers(app), json={"message": "Delegate this review", "verify": True}
    ).json()
    assert (
        client.post("/api/tasks/" + created["id"] + "/cancel", headers=headers(app)).json()["state"]
        == "canceled"
    )
    children = app.state.service.db.rows(tasks, tasks.c.parent_task_id == created["id"])
    assert children and all(t["state"] == "canceled" for t in children)
