from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class Agent:
    id: str
    name: str
    level: str
    family: str | None = None
    parent: str | None = None
    case_id: str | None = None

    @property
    def topic(self):
        return (
            f"observatory.agent.{self.id.replace(':', '.')}.v1"
            if self.level != "case"
            else "observatory.cases.v1"
        )

    def public(self):
        return {**asdict(self), "topic": self.topic}


AGENTS = {"sky": Agent("sky", "Whole-sky coordinator", "coordinator")}
for family, name in [("light", "Light"), ("gravity", "Gravitational waves"), ("neutrino", "Neutrinos")]:
    aid = f"family:{family}"
    AGENTS[aid] = Agent(aid, name, "family", family, "sky")
for aid, name, family in [
    ("fermi-gbm", "Fermi GBM", "light"),
    ("fermi-lat", "Fermi LAT", "light"),
    ("swift-bat", "Swift BAT", "light"),
    ("swift-xrt", "Swift XRT", "light"),
    ("swift-uvot", "Swift UVOT", "light"),
    ("lvk", "LIGO / Virgo / KAGRA", "gravity"),
    ("icecube", "IceCube", "neutrino"),
]:
    AGENTS[aid] = Agent(aid, name, "instrument", family, f"family:{family}")
AGENTS["circulars"] = Agent("circulars", "GCN Circulars", "circulars", parent="sky")
TOPIC_AGENT = {a.topic: a.id for a in AGENTS.values()}
SYSTEM_TOPICS = [
    "observatory.ingest.raw.v1",
    "observatory.findings.v1",
    "observatory.cases.v1",
    "observatory.observer.messages.v1",
    "observatory.deadletter.v1",
]

# External, existing NASA GCN topics. These are never created on the external broker.
GCN_TOPICS = {
    "igwn.gwalert": "lvk",
    "gcn.notices.icecube.gold_bronze_track_alerts": "icecube",
    "gcn.notices.icecube.lvk_nu_track_search": "icecube",
    "gcn.notices.swift.bat.guano": "swift-bat",
    "gcn.circulars": "circulars",
}
for agent, types in {
    "fermi-gbm": [
        "FERMI_GBM_ALERT",
        "FERMI_GBM_FLT_POS",
        "FERMI_GBM_GND_POS",
        "FERMI_GBM_FIN_POS",
        "FERMI_GBM_SUBTHRESH",
    ],
    "fermi-lat": [
        "FERMI_LAT_POS_INI",
        "FERMI_LAT_POS_UPD",
        "FERMI_LAT_GND",
        "FERMI_LAT_OFFLINE",
        "FERMI_LAT_TRANS",
    ],
    "swift-bat": ["SWIFT_BAT_GRB_POS_ACK"],
    "swift-xrt": ["SWIFT_XRT_POSITION"],
    "swift-uvot": ["SWIFT_UVOT_POS"],
}.items():
    GCN_TOPICS.update({"gcn.classic.text." + t: agent for t in types})


def child_agents(agent_id):
    return [a for a in AGENTS.values() if a.parent == agent_id]
