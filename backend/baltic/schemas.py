import hashlib
import json
from datetime import UTC, datetime
from typing import Literal
from uuid import uuid4
from urllib.parse import urlparse

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from .registry import CITY_IDS


def digest(*values):
    return hashlib.sha256(
        json.dumps(values, sort_keys=True, default=str, ensure_ascii=False).encode()
    ).hexdigest()


class Observation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal["1.0"] = "1.0"
    message_id: str = Field(default_factory=lambda: str(uuid4()), max_length=128)
    source_id: str = Field(min_length=1, max_length=120)
    source_item_id: str = Field(min_length=1, max_length=1000)
    entity_id: str | None = Field(default=None, max_length=128)
    title: str = Field(min_length=1, max_length=500)
    summary: str = Field(default="", max_length=20000)
    source_url: str
    country: Literal["lv", "ee", "lt"]
    city_ids: list[str] = Field(default_factory=list, max_length=30)
    candidate_city_ids: list[str] = Field(default_factory=list, max_length=30)
    language: str = Field(default="en", max_length=12)
    kind: Literal["event", "news", "disruption", "weather", "closure"] = "news"
    status: Literal["scheduled", "active", "cancelled", "postponed", "expired"] = "active"
    tags: list[str] = Field(default_factory=list, max_length=20)
    starts_at: AwareDatetime | None = None
    ends_at: AwareDatetime | None = None
    published_at: AwareDatetime | None = None
    updated_at: AwareDatetime | None = None
    observed_at: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))
    timezone: str = "Europe/Riga"
    location: str | None = Field(default=None, max_length=500)
    verification: Literal["source_reported", "organizer_confirmed", "needs_verification"] = "source_reported"
    authoritative: bool = False
    illustrative: bool = False
    trace_id: str = Field(default_factory=lambda: str(uuid4()))
    replay: bool = False

    @field_validator("source_url")
    @classmethod
    def valid_url(cls, v):
        u = urlparse(v)
        if u.scheme not in {"https", "http"} or not u.hostname or u.username or u.password:
            raise ValueError("source_url must be an HTTP(S) URL without credentials")
        return v

    @field_validator("city_ids", "candidate_city_ids")
    @classmethod
    def cities(cls, v):
        if set(v) - CITY_IDS:
            raise ValueError("Unknown city ID")
        return sorted(set(v))

    @field_validator("tags")
    @classmethod
    def clean_tags(cls, v):
        return sorted({x.lower().strip()[:80] for x in v if x.strip()})

    @model_validator(mode="after")
    def dates(self):
        if self.starts_at and self.ends_at and self.ends_at < self.starts_at:
            raise ValueError("ends_at must follow starts_at")
        return self

    def content_hash(self):
        return digest(
            self.model_dump(mode="json", exclude={"message_id", "observed_at", "trace_id", "replay"})
        )


class GuidePreferences(BaseModel):
    city_ids: list[str] = Field(default_factory=list)
    countries: list[Literal["lv", "ee", "lt"]] = Field(default_factory=list)
    interests: list[str] = Field(default_factory=list)
    muted_tags: list[str] = Field(default_factory=list)
    starts_at: AwareDatetime | None = None
    ends_at: AwareDatetime | None = None
    language: str = "en"

    @field_validator("city_ids")
    @classmethod
    def cities(cls, v):
        return Observation.cities(v)

    @model_validator(mode="after")
    def dates(self):
        if self.starts_at and self.ends_at and self.ends_at < self.starts_at:
            raise ValueError("Invalid trip date range")
        return self


class Query(BaseModel):
    message: str = Field(min_length=1, max_length=3000)
    agent_id: str = "baltic:coordinator"
    city_ids: list[str] = Field(default_factory=list)
    starts_at: AwareDatetime | None = None
    ends_at: AwareDatetime | None = None
    request_id: str = Field(default_factory=lambda: str(uuid4()), max_length=128)
    verify: bool = False

    @field_validator("city_ids")
    @classmethod
    def cities(cls, v):
        return Observation.cities(v)
