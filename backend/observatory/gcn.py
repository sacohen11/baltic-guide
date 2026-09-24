"""Native external GCN Kafka ingestion; no generated events or RSS substitution."""

import asyncio
import base64
import json
import logging
import re
import ssl
import time
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime

import httpx
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.abc import AbstractTokenProvider
from sqlalchemy import update

from .db import raw_records, runtime_health, sources
from .registry import AGENTS, GCN_TOPICS
from .schemas import Observation, digest

log = logging.getLogger(__name__)


def timestamp(value):
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000 if value > 1e11 else value, UTC)
    if not value:
        raise ValueError("Source notice timestamp missing")
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        result = parsedate_to_datetime(str(value))
    if result.tzinfo is None:
        raise ValueError("Source timestamp must specify a timezone")
    return result


def event_references(text):
    """Extract exact publisher identifiers, never spatial/temporal associations."""
    refs = []
    for event in sorted(set(re.findall(r"\b(?:MS|TS|S)\d{6}[a-z]+\b", text))):
        refs.append({"case_id": "lvk:" + event, "basis": "explicit_event_reference", "event": event})
    for event in sorted(set(re.findall(r"\bIceCube-\d{6}[A-Z]\b", text, re.I))):
        refs.append(
            {"case_id": "icecube:" + event.lower(), "basis": "explicit_event_reference", "event": event}
        )
    for event in sorted(set(re.findall(r"\bGRB\s*(\d{6}[A-Z])\b", text, re.I))):
        refs.append(
            {
                "case_id": "grb:" + event.upper(),
                "basis": "explicit_event_reference",
                "event": "GRB " + event.upper(),
            }
        )
    for mission, pattern in [
        ("swift", r"\bSwift(?:[- /](?:BAT|XRT|UVOT))?\s+(?:trigger\s*[#:=]?\s*)?(\d{5,})\b"),
        ("fermi-gbm", r"\bFermi[- /]GBM\s+(?:trigger\s*[#:=]?\s*)?(\d{5,})\b"),
    ]:
        for event in sorted(set(re.findall(pattern, text, re.I))):
            refs.append(
                {"case_id": mission + ":" + event, "basis": "explicit_trigger_reference", "event": event}
            )
    return refs


def identifier(value):
    if isinstance(value, list):
        value = value[0] if value else ""
    if not isinstance(value, (str, int)):
        return ""
    return str(value)


def parse_record(topic, raw, raw_id):
    if topic not in GCN_TOPICS:
        raise ValueError("Unsupported GCN topic")
    aid = GCN_TOPICS[topic]
    common = dict(source_id=topic, agent_id=aid, family=AGENTS[aid].family, raw_id=raw_id)
    if topic.startswith("gcn.classic.text."):
        fields = {}
        for line in raw.splitlines():
            match = re.match(r"^([A-Z_]+):\s*(.*)", line)
            if match:
                k, v = match.groups()
                fields[k] = fields.get(k, "") + ("\n" if k in fields else "") + v
        match = re.match(r"\d+", fields.get("TRIGGER_NUM", ""))
        if not match:
            raise ValueError("Classic notice has no trigger number")
        trigger = match.group()
        name = fields.get("NOTICE_TYPE", topic.rsplit(".", 1)[-1])
        notice = timestamp(fields.get("NOTICE_DATE"))
        event_time = None
        day = re.search(r"(\d{2}/\d{2}/\d{2})", fields.get("GRB_DATE", ""))
        seconds = re.match(r"[\d.]+", fields.get("GRB_TIME", ""))
        if day and seconds:
            event_time = datetime.strptime(day.group(), "%y/%m/%d").replace(tzinfo=UTC) + timedelta(
                seconds=float(seconds.group())
            )
        # Swift instruments share a mission trigger. Do not merge unrelated mission IDs.
        case_id = ("swift" if aid.startswith("swift") else aid) + ":" + trigger
        refs = event_references(fields.get("GRB_NAME", ""))
        return Observation(
            **common,
            source_item_id=trigger,
            case_ids=[case_id, *[r["case_id"] for r in refs]],
            title=f"{name} · trigger {trigger}",
            summary=fields.get("COMMENTS", name)[:20000],
            source_url="https://gcn.nasa.gov/missions/" + ("swift" if aid.startswith("swift") else "fermi"),
            notice_time=notice,
            event_time=event_time,
            test=bool(re.search(r"\btest\b", name, re.I)),
            metadata={"classic_fields": fields},
            references=refs,
        )
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("Expected a JSON object")
    if topic == "igwn.gwalert":
        name = str(data["superevent_id"])
        if not re.fullmatch(r"(?:MS|TS|S)\d{6}[a-z]+", name):
            raise ValueError("Invalid LVK superevent identifier")
        event = data.get("event") or {}
        kind = str(data["alert_type"]).upper()
        if kind not in {"EARLYWARNING", "PRELIMINARY", "INITIAL", "UPDATE", "RETRACTION"}:
            raise ValueError("Unknown LVK alert type")
        details = {k: v for k, v in event.items() if k != "skymap"}
        details["skymap_in_raw"] = bool(event.get("skymap"))
        details["external_coinc"] = {
            k: v for k, v in (data.get("external_coinc") or {}).items() if k != "combined_skymap"
        }
        details["alert_type"] = kind
        return Observation(
            **common,
            source_item_id=name,
            case_ids=["lvk:" + name],
            title=f"LVK {name} · {kind.lower()}",
            summary=f"LVK published a {kind.lower()} notice for {name}. Source classifications and false-alarm estimates are preserved in the evidence.",
            source_url=f"https://gracedb.ligo.org/superevents/{name}/view/",
            notice_time=timestamp(data["time_created"]),
            event_time=timestamp(event["time"]) if event.get("time") else None,
            status="retracted" if kind == "RETRACTION" else "active",
            test=name.startswith(("MS", "TS")) or event.get("search") == "MDC",
            metadata=details,
        )
    if topic == "gcn.circulars":
        cid = str(data.get("circularId", data.get("circularID", "")))
        if not cid.isdigit():
            raise ValueError("Circular ID missing")
        subject, body = str(data["subject"]), str(data["body"])
        refs = event_references(subject + "\n" + body)
        return Observation(
            **common,
            source_item_id=cid,
            case_ids=[r["case_id"] for r in refs] or ["circular:" + cid],
            title=subject[:500],
            summary=body[:20000],
            source_url="https://gcn.nasa.gov/circulars/" + cid,
            notice_time=timestamp(data.get("updatedOn") or data.get("createdOn")),
            kind="circular",
            test=bool(re.search(r"\b(?:test|exercise)\b", subject, re.I))
            or any(r["event"].startswith(("MS", "TS")) for r in refs),
            metadata={"submitter": data.get("submitter"), "format": data.get("format", "text/plain")},
            references=refs,
        )
    is_lvk_search = topic.endswith("lvk_nu_track_search")
    name = (
        identifier(data.get("ref_ID") if is_lvk_search else data.get("event_name"))
        or identifier(data.get("id"))
        or str(data.get("trigger_time") or "")
    )
    if not name:
        raise ValueError("Notice has no stable event identifier")
    alert_type = str(data.get("alert_type", "initial")).lower()
    if alert_type not in {"initial", "update", "retraction"}:
        raise ValueError("Unknown notice alert type")
    # The LVK-search schema predates alert_tense; its explicit LVK identifier distinguishes mock events.
    fallback_tense = (
        ("test" if name.startswith(("MS", "TS")) else "current")
        if is_lvk_search and re.fullmatch(r"(?:MS|TS|S)\d{6}[a-z]+", name)
        else "unknown"
    )
    tense = str(data.get("alert_tense", fallback_tense)).lower()
    if tense not in {"current", "test", "injections"}:
        raise ValueError("Missing or unsupported alert_tense; refusing to classify as live")
    refs = event_references(str(data.get("ref_ID", "")) + " " + str(data.get("follow_up_event", "")))
    followup = topic.endswith("lvk_nu_track_search") or bool(data.get("follow_up_event"))
    cid = ("icecube:" + name.lower()) if aid == "icecube" else "swift-guano:" + name
    # A search refers to an event but is not necessarily a detection of it.
    case_ids = [r["case_id"] for r in refs] if followup and refs else [cid, *[r["case_id"] for r in refs]]
    for ref in refs:
        ref["source_reference"] = data.get("reference")
    return Observation(
        **common,
        source_item_id=("search:" if followup else "track:") + name,
        case_ids=case_ids,
        title=f"{AGENTS[aid].name} · {name} · {alert_type}",
        summary=(
            f"Follow-up search referencing {', '.join(r['event'] for r in refs) or name}. "
            "A follow-up report does not establish a counterpart detection."
            if followup
            else f"Source-reported {data.get('pipeline', 'transient')} alert. See the original measurements and uncertainty."
        ),
        source_url="https://gcn.nasa.gov/missions/" + ("icecube" if aid == "icecube" else "swift"),
        notice_time=timestamp(data.get("alert_datetime")),
        event_time=timestamp(data["trigger_time"]) if data.get("trigger_time") else None,
        revision=data.get("record_number"),
        status="retracted" if alert_type == "retraction" else "active",
        test=tense != "current",
        kind="followup" if followup else "detection",
        references=refs,
        metadata={k: v for k, v in data.items() if k not in {"skymap", "combined_skymap"}},
    )


class GCNTokenProvider(AbstractTokenProvider):
    def __init__(self, settings):
        self.settings, self.access_token, self.expires = settings, "", 0
        self.lock = asyncio.Lock()

    async def token(self):
        async with self.lock:
            if time.time() >= self.expires:
                async with httpx.AsyncClient(timeout=20) as client:
                    response = await client.post(
                        "https://auth.gcn.nasa.gov/oauth2/token",
                        data={"grant_type": "client_credentials"},
                        auth=(self.settings.gcn_client_id, self.settings.gcn_client_secret),
                    )
                    if response.status_code != 200:
                        raise RuntimeError(f"GCN authentication returned HTTP {response.status_code}")
                    result = response.json()
                self.access_token = result["access_token"]
                self.expires = time.time() + max(1, int(result.get("expires_in", 3600)) - 60)
            return self.access_token


class GCNIngestor:
    def __init__(self, service):
        self.service, self.settings, self.db = service, service.settings, service.db
        requested = [t.strip() for t in self.settings.gcn_topics.split(",") if t.strip()]
        self.topics = requested or list(GCN_TOPICS)
        if set(self.topics) - GCN_TOPICS.keys():
            raise ValueError("GCN_TOPICS contains unsupported topics")

    def health(self, state, error=None, **extra):
        with self.db.tx() as c:
            values = {
                "updated_at": time.time(),
                "payload": {"state": state, "error": error, "topics": self.topics, **extra},
            }
            self.db.insert_once(c, runtime_health, {"id": "gcn", **values})
            c.execute(update(runtime_health).where(runtime_health.c.id == "gcn").values(**values))

    def accept(self, topic, partition, offset, value):
        rid = digest("gcn", topic, partition, offset)
        raw = (value or b"").decode("utf-8", errors="replace")
        try:
            if value is None:
                raise ValueError("Kafka tombstone has no notice payload")
            observation = parse_record(topic, value.decode("utf-8"), rid)
            error = None
        except (ValueError, KeyError, TypeError, OverflowError) as exc:
            observation, error = None, f"{type(exc).__name__}: {str(exc)[:250]}"
        with self.db.tx() as c:
            if not self.db.insert_once(
                c,
                raw_records,
                {
                    "id": rid,
                    "topic": topic,
                    "partition": partition,
                    "offset": offset,
                    "payload": raw,
                    "original_base64": base64.b64encode(value or b"").decode(),
                    "state": "quarantined" if error else "accepted",
                    "error": error,
                },
            ):
                return rid
            c.execute(
                update(sources)
                .where(sources.c.id == topic)
                .values(
                    last_attempt=time.time(),
                    last_success=time.time() if not error else sources.c.last_success,
                    error=error,
                    item_count=sources.c.item_count + 1,
                )
            )
            if error:
                self.service.emit(
                    c,
                    "observatory.deadletter.v1",
                    rid,
                    {"raw_id": rid, "error": error},
                    digest("quarantine", rid),
                )
            else:
                self.service.emit(
                    c,
                    "observatory.ingest.raw.v1",
                    observation.agent_id,
                    observation.model_dump(mode="json"),
                    rid,
                )
        return rid

    async def run(self):
        if not self.settings.gcn_client_id or not self.settings.gcn_client_secret:
            self.health("unconfigured", "Set GCN_CLIENT_ID and GCN_CLIENT_SECRET on the backend")
            raise RuntimeError("GCN credentials are required; no synthetic fallback is available")
        retry = 1
        while True:
            consumer = AIOKafkaConsumer(
                bootstrap_servers=self.settings.gcn_bootstrap,
                group_id=self.settings.gcn_group,
                security_protocol="SASL_SSL",
                ssl_context=ssl.create_default_context(),
                sasl_mechanism="OAUTHBEARER",
                sasl_oauth_token_provider=GCNTokenProvider(self.settings),
                enable_auto_commit=False,
                auto_offset_reset=self.settings.gcn_offset_reset,
                max_partition_fetch_bytes=20_000_000,
                fetch_max_bytes=40_000_000,
                max_poll_interval_ms=300000,
            )
            try:
                self.health("connecting")
                await consumer.start()
                available = await consumer.topics()
                missing = sorted(set(self.topics) - available)
                # Fail loudly instead of silently claiming full instrument coverage.
                if missing:
                    raise RuntimeError("GCN topics unavailable: " + ", ".join(missing))
                consumer.subscribe(self.topics)
                retry = 1
                while True:
                    batches = await consumer.getmany(timeout_ms=1000, max_records=25)
                    for tp, records in batches.items():
                        for record in records:
                            await asyncio.to_thread(
                                self.accept, record.topic, record.partition, record.offset, record.value
                            )
                            # Only advance after raw retention + outbox transaction succeeds.
                            await consumer.commit({TopicPartition(tp.topic, tp.partition): record.offset + 1})
                    assignment = consumer.assignment()
                    lag = {}
                    for tp in assignment:
                        high = consumer.highwater(tp)
                        pos = await consumer.position(tp)
                        if high is not None:
                            lag[f"{tp.topic}:{tp.partition}"] = max(0, high - pos)
                    self.health("connected" if assignment else "assigning", lag=lag)
            except asyncio.CancelledError:
                self.health("stopped")
                raise
            except Exception as exc:
                # Do not expose authentication responses or credentials in logs/UI.
                error = str(exc) if isinstance(exc, RuntimeError) else type(exc).__name__
                self.health("error", error[:1000])
                log.warning("GCN ingestion interrupted: %s", type(exc).__name__)
            finally:
                await consumer.stop()
            await asyncio.sleep(retry)
            retry = min(60, retry * 2)
