from datetime import UTC, datetime, timedelta

import pytest

from baltic.db import Database
from baltic.runtime import Runtime
from baltic.schemas import Observation
from baltic.service import Service
from baltic.settings import Settings


@pytest.fixture
def service(tmp_path):
    settings = Settings(
        database_url=f"sqlite:///{tmp_path}/test.db",
        embedded_runtime=False,
        sources_file="missing.yaml",
        checkpoint_dir=str(tmp_path / "checkpoints"),
    )
    service = Service(Database(settings.database_url), settings)
    service.bootstrap()
    yield service
    service.db.engine.dispose()


@pytest.fixture
def runtime(service):
    rt = Runtime(service)
    rt.setup_graphs()
    yield rt
    rt.stack.close()


@pytest.fixture
def observation():
    start = datetime.now(UTC) + timedelta(days=3)
    return Observation(
        source_id="test",
        source_item_id="a",
        entity_id="event-a",
        country="lv",
        city_ids=["lv:riga"],
        source_url="https://example.org/event-a",
        title="Winter market",
        summary="An organizer-reported market.",
        kind="event",
        status="scheduled",
        tags=["christmas"],
        starts_at=start,
        ends_at=start + timedelta(days=10),
        authoritative=True,
    )
