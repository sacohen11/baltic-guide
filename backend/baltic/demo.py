from datetime import UTC, datetime, timedelta

from .schemas import Observation


def seed_demo(service):
    if not service.settings.demo_mode:
        raise ValueError("Demo data is disabled outside demo mode")
    now = datetime.now(UTC)
    start = (now + timedelta(days=7)).replace(hour=10, minute=0, second=0, microsecond=0)
    groups = {
        "lv": [("riga", "Rīga"), ("liepaja", "Liepāja"), ("jelgava", "Jelgava")],
        "ee": [("tallinn", "Tallinn"), ("tartu", "Tartu"), ("parnu", "Pärnu")],
        "lt": [("vilnius", "Vilnius"), ("kaunas", "Kaunas"), ("klaipeda", "Klaipėda")],
    }
    count = 0
    for country, cities in groups.items():
        for i, (slug, name) in enumerate(cities):
            service.ingest(
                Observation(
                    source_id="demo-" + country,
                    source_item_id="market-" + slug,
                    entity_id="demo-market-" + slug,
                    title=f"{name} winter market",
                    summary=f"Illustrative scenario: a seasonal market in {name}, with local crafts, music, and food. These are simulated dates, not a real event listing.",
                    source_url=f"https://example.org/demo/{slug}-market",
                    country=country,
                    city_ids=[f"{country}:{slug}"],
                    kind="event",
                    status="scheduled",
                    tags=["christmas", "food"],
                    starts_at=start + timedelta(hours=i),
                    ends_at=start + timedelta(days=20),
                    illustrative=True,
                    authoritative=True,
                    verification="needs_verification",
                )
            )
            count += 1
    service.ingest(
        Observation(
            source_id="demo-lv",
            source_item_id="tram-works",
            entity_id="demo-tram-riga",
            title="Rīga old-town tram diversion",
            summary="Illustrative service notice: allow an extra 15 minutes on the old-town transfer.",
            source_url="https://example.org/demo/riga-tram",
            country="lv",
            city_ids=["lv:riga"],
            kind="disruption",
            starts_at=start,
            ends_at=start + timedelta(days=2),
            tags=["transport"],
            illustrative=True,
        )
    )
    return count + 1
