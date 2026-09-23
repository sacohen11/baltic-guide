from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class Agent:
    id: str
    name: str
    level: str
    country: str | None
    city_id: str | None
    parent: str | None
    timezone: str
    aliases: tuple[str, ...] = ()
    lat: float | None = None
    lon: float | None = None

    @property
    def topic(self):
        if self.level == "city":
            return f"baltic.city.{self.country}.{self.city_id.split(':')[1]}.events.v1"
        if self.level == "country":
            return f"baltic.country.{self.country}.events.v1"
        return "baltic.country.findings.v1"

    def public(self):
        return {**asdict(self), "topic": self.topic}


CITIES = {
    "lv": [
        ("riga", "Rīga", 56.95, 24.11, "Riga"),
        ("daugavpils", "Daugavpils", 55.87, 26.52, ""),
        ("liepaja", "Liepāja", 56.51, 21.01, "Liepaja"),
        ("jelgava", "Jelgava", 56.65, 23.72, ""),
        ("jurmala", "Jūrmala", 56.97, 23.78, "Jurmala"),
        ("ventspils", "Ventspils", 57.39, 21.56, ""),
        ("rezekne", "Rēzekne", 56.50, 27.33, "Rezekne"),
        ("valmiera", "Valmiera", 57.54, 25.43, ""),
        ("ogre", "Ogre", 56.82, 24.60, ""),
        ("jekabpils", "Jēkabpils", 56.50, 25.86, "Jekabpils"),
    ],
    "ee": [
        ("tallinn", "Tallinn", 59.44, 24.75, "Tallinnas|Tallinna"),
        ("tartu", "Tartu", 58.38, 26.72, "Tartus"),
        ("narva", "Narva", 59.38, 28.19, ""),
        ("parnu", "Pärnu", 58.38, 24.50, "Parnu"),
        ("kohtla-jarve", "Kohtla-Järve", 59.40, 27.27, "Kohtla-Jarve"),
        ("viljandi", "Viljandi", 58.36, 25.59, ""),
        ("maardu", "Maardu", 59.47, 24.98, ""),
        ("rakvere", "Rakvere", 59.35, 26.36, ""),
        ("kuressaare", "Kuressaare", 58.25, 22.49, ""),
        ("sillamae", "Sillamäe", 59.40, 27.76, "Sillamae"),
    ],
    "lt": [
        ("vilnius", "Vilnius", 54.69, 25.28, "Vilniuje"),
        ("kaunas", "Kaunas", 54.90, 23.90, "Kaune"),
        ("klaipeda", "Klaipėda", 55.70, 21.14, "Klaipeda|Klaipėdoje"),
        ("siauliai", "Šiauliai", 55.93, 23.32, "Siauliai"),
        ("panevezys", "Panevėžys", 55.73, 24.36, "Panevezys"),
        ("alytus", "Alytus", 54.40, 24.05, ""),
        ("marijampole", "Marijampolė", 54.56, 23.35, "Marijampole"),
        ("mazeikiai", "Mažeikiai", 56.31, 22.34, "Mazeikiai"),
        ("jonava", "Jonava", 55.07, 24.28, ""),
        ("utena", "Utena", 55.50, 25.60, ""),
    ],
}
COUNTRIES = {
    "lv": ("Latvia", "Europe/Riga"),
    "ee": ("Estonia", "Europe/Tallinn"),
    "lt": ("Lithuania", "Europe/Vilnius"),
}
AGENTS = {
    "baltic:coordinator": Agent(
        "baltic:coordinator", "Baltic coordinator", "baltic", None, None, None, "Europe/Riga"
    )
}
for country, (name, timezone) in COUNTRIES.items():
    aid = f"country:{country}"
    AGENTS[aid] = Agent(aid, name, "country", country, None, "baltic:coordinator", timezone)
    for slug, city, lat, lon, other in CITIES[country]:
        cid = f"{country}:{slug}"
        AGENTS[f"city:{cid}"] = Agent(
            f"city:{cid}",
            city,
            "city",
            country,
            cid,
            aid,
            timezone,
            tuple(dict.fromkeys([city, slug, *filter(None, other.split("|"))])),
            lat,
            lon,
        )
CITY_IDS = {a.city_id for a in AGENTS.values() if a.city_id}
TOPIC_AGENT = {a.topic: a.id for a in AGENTS.values() if a.level != "baltic"}
SYSTEM_TOPICS = [
    "baltic.ingest.raw.v1",
    "baltic.city.findings.v1",
    "baltic.country.findings.v1",
    "baltic.guide.messages.v1",
    "baltic.agent.health.v1",
    "baltic.deadletter.v1",
]


def child_agents(agent_id):
    return [a for a in AGENTS.values() if a.parent == agent_id]
