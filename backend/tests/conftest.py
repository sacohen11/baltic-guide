import json

import pytest

from observatory.db import Database
from observatory.gcn import GCNIngestor
from observatory.runtime import Runtime
from observatory.service import Service
from observatory.settings import Settings


@pytest.fixture
def service(tmp_path):
    settings = Settings(
        database_url=f"sqlite:///{tmp_path}/test.db",
        transport="local",
        admin_token="test-only-token-at-least-24-characters",
        model="",
        model_api_key="",
        checkpoint_dir=str(tmp_path / "checkpoints"),
    )
    db = Database(settings.database_url)
    result = Service(db, settings)
    result.bootstrap()
    yield result
    db.engine.dispose()


@pytest.fixture
def runtime(service):
    result = Runtime(service)
    yield result
    result.stack.close()


def lvk(name="S260923abc", kind="INITIAL", time="2026-09-23T12:00:00Z"):
    return {
        "superevent_id": name,
        "alert_type": kind,
        "time_created": time,
        "event": None
        if kind == "RETRACTION"
        else {
            "time": "2026-09-23T11:59:00Z",
            "far": 1e-9,
            "classification": {"BNS": 0.8, "BBH": 0.1, "Noise": 0.1},
            "skymap": "cmF3LW1hcA==",
        },
    }


def accept(service, body, offset=0, topic="igwn.gwalert"):
    return GCNIngestor(service).accept(topic, 0, offset, json.dumps(body).encode())
