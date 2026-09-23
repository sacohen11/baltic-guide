import json

import pytest
from baltic.connectors import parse_rss, parse_html, parse_ics, parse_json, public_url
from baltic.schemas import Observation

SRC = {"id": "test", "country": "lv", "url": "https://example.org/feed", "kind": "rss"}


def test_rss_mentions_are_candidates_not_event_locations():
    data = b'<rss version="2.0"><channel><title>News</title><item><guid>1</guid><title>Riga and Liepaja music</title><link>https://example.org/1</link><description>Concert news</description></item></channel></rss>'
    obs = parse_rss(SRC, data)[0]
    assert obs.city_ids == []
    assert set(obs.candidate_city_ids) == {"lv:riga", "lv:liepaja"}
    assert "music" in obs.tags


def test_jsonld_structured_locality_and_cancellation():
    item = {
        "@type": "Event",
        "name": "Music fair",
        "url": "https://example.org/a",
        "startDate": "2030-12-01T10:00:00+02:00",
        "endDate": "2030-12-01T18:00:00+02:00",
        "location": {"name": "Town square", "address": {"addressLocality": "Rīga"}},
        "eventStatus": "https://schema.org/EventCancelled",
    }
    obs = parse_html(SRC, ('<script type="application/ld+json">' + json.dumps(item) + "</script>").encode())[
        0
    ]
    assert obs.city_ids == ["lv:riga"] and obs.status == "cancelled"
    assert obs.starts_at.hour == 8


def test_invalid_html_not_marked_healthy():
    with pytest.raises(ValueError, match="No parseable"):
        parse_html(SRC, b"<html>Nothing here</html>")


def test_ics_timezone_and_cancel():
    raw = b"BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:abc\r\nSUMMARY:City walk\r\nDTSTART;TZID=Europe/Riga:20300510T100000\r\nDTEND;TZID=Europe/Riga:20300510T120000\r\nSTATUS:CANCELLED\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
    obs = parse_ics({**SRC, "city_ids": ["lv:riga"]}, raw)[0]
    assert obs.starts_at.hour == 7 and obs.status == "cancelled"


def test_json_mapping():
    src = {**SRC, "items_path": "items", "field_map": {"name": "title", "url": "link", "startDate": "when"}}
    obs = parse_json(
        src,
        json.dumps(
            {"items": [{"title": "Event", "link": "https://example.org/a", "when": "2030-12-01"}]}
        ).encode(),
    )[0]
    assert obs.title == "Event" and obs.starts_at.tzinfo


def test_private_source_rejected():
    with pytest.raises(ValueError):
        public_url("https://127.0.0.1/secrets")
    with pytest.raises(ValueError):
        public_url("http://example.org/feed")


def test_schema_rejects_naive_dates_unknown_cities_and_bad_urls():
    data = dict(
        source_id="test", source_item_id="1", title="Event", source_url="https://example.org/a", country="lv"
    )
    with pytest.raises(ValueError):
        Observation(**data, starts_at="2030-12-01T10:00:00")
    with pytest.raises(ValueError):
        Observation(**data, city_ids=["lv:unknown"])
    with pytest.raises(ValueError):
        Observation(**{**data, "source_url": "javascript:alert(1)"})
