import asyncio

from fastapi.testclient import TestClient
from a2a.types import a2a_pb2 as pb
from google.protobuf.json_format import MessageToDict, ParseDict

from baltic.api import make_app
from baltic.settings import Settings
from baltic.db import tasks


def app_for(tmp_path, **kwargs):
    return make_app(
        Settings(
            database_url=f"sqlite:///{tmp_path}/api.db",
            embedded_runtime=False,
            checkpoint_dir=str(tmp_path / "checkpoints"),
            sources_file="missing.yaml",
            **kwargs,
        )
    )


def test_api_demo_query_and_actions(tmp_path):
    app = app_for(tmp_path)
    with TestClient(app) as c:
        assert len(c.get("/api/agents").json()) == 34
        assert c.post("/api/demo/seed").status_code == 200
        asyncio.run(app.state.runtime.drain())
        messages = c.get("/api/messages?limit=500").json()
        assert len(messages) >= 10
        assert any(m["payload"]["type"] == "theme" for m in messages)
        mid = messages[0]["id"]
        assert c.post(f"/api/messages/{mid}/actions", json={"action": "save_to_trip"}).status_code == 200
        assert c.get("/api/messages").json()[0]["saved"]
        query = c.post("/api/query", json={"message": "What changed?", "request_id": "unique"})
        assert query.status_code == 202
        task_id = query.json()["task_id"]
        assert (
            c.post("/api/query", json={"message": "What changed?", "request_id": "unique"}).json()["task_id"]
            == task_id
        )
        asyncio.run(app.state.runtime.drain())
        assert c.get("/api/tasks/" + task_id).json()["state"] == "completed"


def test_auth_and_tenant_isolation(tmp_path):
    token = "test-admin-token-long-enough-for-validation"
    app = app_for(tmp_path, demo_mode=False, admin_token=token)
    with TestClient(app) as c:
        assert c.get("/api/messages").status_code == 401
        h = {"Authorization": "Bearer " + token}
        g1 = c.post("/api/guides", headers=h, json={"name": "One"}).json()
        g2 = c.post("/api/guides", headers=h, json={"name": "Two"}).json()
        h1 = {"Authorization": "Bearer " + g1["token"]}
        h2 = {"Authorization": "Bearer " + g2["token"]}
        tid = c.post("/api/query", headers=h1, json={"message": "Hello"}).json()["task_id"]
        assert c.get("/api/tasks/" + tid, headers=h2).status_code == 404
        assert c.post("/api/demo/seed", headers=h1).status_code == 403
        assert c.post("/api/demo/seed", headers=h).status_code == 403
        assert c.get("/api/admin/deadletters", headers=h1).status_code == 403
        assert c.get("/metrics", headers=h1).status_code == 403


def test_a2a_sdk_jsonrpc_roundtrip_and_card(tmp_path):
    app = app_for(tmp_path)
    with TestClient(app) as c:
        base = "/a2a/city:lv:riga"
        card = c.get(base + "/.well-known/agent-card.json")
        assert card.status_code == 200
        parsed_card = ParseDict(card.json(), pb.AgentCard())
        assert parsed_card.name == "Rīga" and parsed_card.capabilities.streaming
        req = pb.SendMessageRequest(
            message=pb.Message(
                message_id="sdk-msg-1", role=pb.ROLE_USER, parts=[pb.Part(text="What is happening?")]
            ),
            configuration=pb.SendMessageConfiguration(return_immediately=True),
        )
        headers = {"A2A-Version": "1.0"}
        response = c.post(
            base + "/",
            headers=headers,
            json={"jsonrpc": "2.0", "id": "1", "method": "SendMessage", "params": MessageToDict(req)},
        )
        assert response.status_code == 200, response.text
        data = response.json()
        assert "error" not in data, data
        # SDK's 1.0 SendMessage returns a response envelope containing a task.
        task = data["result"].get("task", data["result"])
        task_id = task["id"]
        asyncio.run(app.state.runtime.drain())
        response = c.post(
            base + "/",
            headers=headers,
            json={"jsonrpc": "2.0", "id": "2", "method": "GetTask", "params": {"id": task_id}},
        )
        assert "error" not in response.json(), response.text
        task = ParseDict(response.json()["result"], pb.Task())
        assert task.status.state == pb.TASK_STATE_COMPLETED
        assert len(task.artifacts) == 1


def test_delegated_verification_finishes(tmp_path):
    app = app_for(tmp_path)
    with TestClient(app) as c:
        result = c.post(
            "/api/query",
            json={
                "message": "Check the stored evidence",
                "verify": True,
                "city_ids": ["lv:riga", "ee:tallinn"],
            },
        )
        assert result.status_code == 202, result.text
        tid = result.json()["task_id"]
        # Parent may yield until child tasks finish; advance availability in this deterministic test.
        from sqlalchemy import update
        from baltic.db import inbox

        for _ in range(15):
            asyncio.run(app.state.runtime.drain())
            with app.state.service.db.tx() as conn:
                conn.execute(update(inbox).values(available_at=0))
            if c.get("/api/tasks/" + tid).json()["state"] == "completed":
                break
        assert c.get("/api/tasks/" + tid).json()["state"] == "completed"
        rows = app.state.service.db.rows(tasks)
        assert len(rows) == 5
        assert all(t["state"] == "completed" for t in rows)
