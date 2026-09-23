"""Bounded RSS/Atom, ICS, JSON and JSON-LD connectors with conditional requests."""

import asyncio
import ipaddress
import json
import logging
import os
import re
import socket
import time
from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse
from zoneinfo import ZoneInfo

import feedparser
import httpx
import yaml
from bs4 import BeautifulSoup
from icalendar import Calendar
from sqlalchemy import select, update

from .db import sources
from .registry import AGENTS, COUNTRIES
from .schemas import Observation
from .service import plain
from .telemetry import INGEST

log = logging.getLogger(__name__)
TAG_WORDS = {
    "christmas": ["christmas", "ziemassvet", "joul", "kaled", "рождеств"],
    "music": ["concert", "music", "koncert", "muusik", "muzik"],
    "festival": ["festival", "festivals"],
    "food": ["food", "restaurant", "gastronom", "edien", "toit", "maist"],
    "art": ["exhibition", "museum", "izstad", "naitus", "parod"],
    "outdoors": ["hiking", "trail", "park", "outdoor"],
    "transport": ["transport", "rail", "flight", "bus", "tram", "road closure"],
    "sport": ["marathon", "running", "sport", "tennis"],
}


def load_sources(path):
    if not Path(path).exists():
        return []
    data = yaml.safe_load(Path(path).read_text()) or {}
    return data.get("sources", [])


def tags_for(text):
    txt = plain(text)
    return [tag for tag, words in TAG_WORDS.items() if any(plain(w) in txt for w in words)]


def candidate_cities(text, country):
    text = plain(text)
    return [
        a.city_id
        for a in AGENTS.values()
        if a.country == country
        and a.city_id
        and any(re.search(r"(?<!\w)" + re.escape(plain(alias)) + r"(?!\w)", text) for alias in a.aliases)
    ]


def parse_date(value, timezone):
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime.combine(value, datetime.min.time())
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            try:
                dt = parsedate_to_datetime(str(value))
            except (ValueError, TypeError):
                return None
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=ZoneInfo(timezone))
    return dt.astimezone(UTC)


def clean(text):
    return BeautifulSoup(str(text or ""), "html.parser").get_text(" ", strip=True)


def base(source, item_id, title, summary, url):
    country = source["country"]
    return dict(
        source_id=source["id"],
        source_item_id=str(item_id),
        title=clean(title)[:500],
        summary=clean(summary)[:20000],
        source_url=url,
        country=country,
        city_ids=source.get("city_ids", []),
        candidate_city_ids=candidate_cities(title + " " + summary, country),
        language=source.get("language", "en"),
        timezone=COUNTRIES[country][1],
        tags=tags_for(title + " " + summary),
        authoritative=source.get("authoritative", False),
    )


def parse_rss(source, body):
    feed = feedparser.parse(body)
    if feed.bozo and not feed.entries:
        raise ValueError("Invalid RSS/Atom document")
    if not feed.entries and not feed.get("version"):
        raise ValueError("Response is not RSS/Atom")
    result = []
    for entry in feed.entries[:300]:
        url = urljoin(source["url"], entry.get("link", ""))
        if not entry.get("title") or not entry.get("link"):
            continue
        item = base(source, entry.get("id", url), entry.title, entry.get("summary", ""), url)
        item.update(
            kind=source.get("event_kind", "news"),
            published_at=parse_date(entry.get("published"), item["timezone"]),
            updated_at=parse_date(entry.get("updated"), item["timezone"]),
        )
        # City mentions remain candidates unless a city-specific feed establishes scope.
        result.append(Observation(**item))
    return result


def jsonld_events(value):
    if isinstance(value, list):
        for item in value:
            yield from jsonld_events(item)
    elif isinstance(value, dict):
        types = value.get("@type", [])
        if isinstance(types, str):
            types = [types]
        if any(t.endswith("Event") for t in types):
            yield value
        for key in ["@graph", "itemListElement", "item"]:
            if key in value:
                yield from jsonld_events(value[key])


def parse_event(source, data):
    location = data.get("location", {})
    if isinstance(location, list):
        location = location[0] if location else {}
    address = location.get("address", {}) if isinstance(location, dict) else {}
    locality = address.get("addressLocality", "") if isinstance(address, dict) else ""
    place = location.get("name", "") if isinstance(location, dict) else str(location)
    url = urljoin(source["url"], str(data.get("url") or data.get("@id") or source["url"]))
    item = base(
        source,
        data.get("identifier") or data.get("@id") or url,
        str(data.get("name", "")),
        str(data.get("description", "")),
        url,
    )
    if not source.get("city_ids") and locality:
        item["city_ids"] = candidate_cities(locality, source["country"])
    status = str(data.get("eventStatus", ""))
    item.update(
        kind="event",
        location=place or locality or None,
        starts_at=parse_date(data.get("startDate"), item["timezone"]),
        ends_at=parse_date(data.get("endDate"), item["timezone"]),
        updated_at=parse_date(data.get("dateModified"), item["timezone"]),
        status="cancelled"
        if "Cancelled" in status
        else "postponed"
        if "Postponed" in status
        else "scheduled",
    )
    if not item["title"]:
        return None
    return Observation(**item)


def parse_html(source, body):
    soup = BeautifulSoup(body, "html.parser")
    result = []
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            for event in jsonld_events(json.loads(script.string or script.get_text())):
                obs = parse_event(source, event)
                if obs:
                    result.append(obs)
        except (ValueError, TypeError):
            continue
    if not result:
        raise ValueError("No parseable JSON-LD Events found; connector needs a publisher-specific adapter")
    return result


def get_path(value, path):
    for part in str(path).split("."):
        value = value.get(part) if isinstance(value, dict) else None
    return value


def parse_json(source, body):
    data = json.loads(body)
    items = get_path(data, source["items_path"]) if source.get("items_path") else data
    if not isinstance(items, list):
        raise ValueError("Configured JSON items_path does not select an array")
    result = []
    for row in items[:500]:
        if source.get("field_map"):
            row = {target: get_path(row, path) for target, path in source["field_map"].items()}
        obs = parse_event(source, row)
        if obs:
            result.append(obs)
    return result


def parse_ics(source, body):
    result = []
    for event in Calendar.from_ical(body).walk("VEVENT"):
        if event.get("RRULE"):
            # Never silently advertise an unexpanded recurring event as complete coverage.
            raise ValueError("Recurring ICS requires expansion; use an expanded feed")
        uid = str(event.get("UID", ""))
        title = str(event.get("SUMMARY", ""))
        if not uid or not title:
            continue
        url = str(event.get("URL") or source["url"])
        item = base(source, uid, title, str(event.get("DESCRIPTION", "")), url)
        start, end = event.get("DTSTART"), event.get("DTEND")
        item.update(
            kind="event",
            starts_at=parse_date(start.dt if start else None, item["timezone"]),
            ends_at=parse_date(end.dt if end else None, item["timezone"]),
            location=str(event.get("LOCATION", "")) or None,
            updated_at=parse_date(
                event.decoded("LAST-MODIFIED") if event.get("LAST-MODIFIED") else None, item["timezone"]
            ),
            status="cancelled" if str(event.get("STATUS", "")) == "CANCELLED" else "scheduled",
        )
        result.append(Observation(**item))
    return result


PARSERS = {"rss": parse_rss, "html_jsonld": parse_html, "json": parse_json, "ics": parse_ics}


def public_url(url):
    parsed = urlparse(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("Connectors require public HTTPS URLs without credentials")
    for _, _, _, _, sockaddr in socket.getaddrinfo(
        parsed.hostname, parsed.port or 443, type=socket.SOCK_STREAM
    ):
        if not ipaddress.ip_address(sockaddr[0]).is_global:
            raise ValueError("Connector host resolves to a non-public address")


class ConnectorRunner:
    def __init__(self, service):
        self.service = service

    async def poll(self, source_id, force=False):
        db = self.service.db
        now = time.time()
        with db.tx() as c:
            row = c.execute(select(sources).where(sources.c.id == source_id)).mappings().first()
            if not row:
                raise ValueError("Unknown source")
            src = dict(row["definition"])
            if not src.get("enabled", False):
                raise ValueError("Source disabled: configure access and validate parsing first")
            if not force and row["next_poll"] > now:
                return 0
            if not c.execute(
                update(sources)
                .where(sources.c.id == source_id, sources.c.next_poll == row["next_poll"])
                .values(next_poll=now + 120, last_attempt=now)
            ).rowcount:
                return 0
        try:
            if src.get("url_env"):
                src["url"] = os.environ.get(src["url_env"], "")
            await asyncio.to_thread(public_url, src["url"])
            headers = {"User-Agent": "BalticGuide/0.1 (+source-monitoring)", "Accept": "*/*"}
            if row["etag"]:
                headers["If-None-Match"] = row["etag"]
            if row["modified"]:
                headers["If-Modified-Since"] = row["modified"]
            if src.get("token_env"):
                token = os.environ.get(src["token_env"])
                if not token:
                    raise ValueError("Source API credential is not configured")
                headers["Authorization"] = f"Bearer {token}"
            async with httpx.AsyncClient(timeout=30, follow_redirects=False) as client:
                async with client.stream("GET", src["url"], headers=headers) as response:
                    if response.status_code == 304:
                        body = None
                    else:
                        response.raise_for_status()
                        if 300 <= response.status_code < 400:
                            raise ValueError("Redirect requires reviewing the configured source URL")
                        chunks, size = [], 0
                        async for chunk in response.aiter_bytes():
                            size += len(chunk)
                            if size > 5_000_000:
                                raise ValueError("Source response exceeds 5 MB")
                            chunks.append(chunk)
                        body = b"".join(chunks)
                parsed = await asyncio.to_thread(PARSERS[src["kind"]], src, body) if body is not None else []
                if body is not None and not parsed and not src.get("allow_empty", False):
                    raise ValueError("Empty extraction; not treating this as a healthy source")
            for obs in parsed:
                self.service.ingest(obs)
                INGEST.labels(src["id"]).inc()
            with db.tx() as c:
                c.execute(
                    update(sources)
                    .where(sources.c.id == source_id)
                    .values(
                        last_success=now,
                        next_poll=now + max(60, src.get("interval_seconds", 900)),
                        error=None,
                        failures=0,
                        item_count=len(parsed) if body is not None else row["item_count"],
                        etag=response.headers.get("etag", row["etag"]),
                        modified=response.headers.get("last-modified", row["modified"]),
                    )
                )
            return len(parsed)
        except Exception as exc:
            with db.tx() as c:
                failures = row["failures"] + 1
                c.execute(
                    update(sources)
                    .where(sources.c.id == source_id)
                    .values(
                        failures=failures,
                        error=f"{type(exc).__name__}: {str(exc)[:250]}",
                        next_poll=now + min(3600, 60 * 2 ** min(6, failures)),
                    )
                )
            raise

    async def run(self):
        while True:
            for source in self.service.db.rows(sources):
                if source["definition"].get("enabled") and source["next_poll"] <= time.time():
                    try:
                        await self.poll(source["id"])
                    except Exception:
                        log.warning("Source poll failed: %s", source["id"])
            await asyncio.sleep(5)
