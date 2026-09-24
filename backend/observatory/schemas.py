import hashlib
import json
from typing import Literal
from uuid import uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


def digest(*values):
    return hashlib.sha256(
        json.dumps(values, sort_keys=True, default=str, ensure_ascii=False).encode()
    ).hexdigest()


class Observation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_id: str
    source_item_id: str
    agent_id: str
    family: str | None
    case_ids: list[str]
    title: str = Field(min_length=1, max_length=500)
    summary: str = Field(max_length=20000)
    source_url: str
    notice_time: AwareDatetime
    event_time: AwareDatetime | None = None
    revision: int | None = None
    status: Literal["active", "retracted"] = "active"
    kind: Literal["detection", "followup", "circular"] = "detection"
    test: bool = False
    metadata: dict = Field(default_factory=dict)
    references: list[dict] = Field(default_factory=list)
    raw_id: str


class ObserverPreferences(BaseModel):
    families: list[Literal["light", "gravity", "neutrino"]] = Field(default_factory=list)
    instruments: list[str] = Field(default_factory=list, max_length=7)
    multi_messenger_only: bool = False


class Query(BaseModel):
    message: str = Field(min_length=1, max_length=3000)
    agent_id: str = "sky"
    families: list[Literal["light", "gravity", "neutrino"]] = Field(default_factory=list)
    case_id: str | None = Field(default=None, max_length=128)
    request_id: str = Field(default_factory=lambda: str(uuid4()), max_length=128)
    verify: bool = False
