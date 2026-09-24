# External GCN streams

This app consumes existing Kafka topics from `kafka.gcn.nasa.gov:9092` using TLS and OAuth bearer authentication. Create a GCN account/client at https://gcn.nasa.gov/quickstart. The internal broker and external broker are separate.

| External topic / suffix | Agent | Format |
|---|---|---|
| `igwn.gwalert` | LVK | JSON |
| `gcn.notices.icecube.gold_bronze_track_alerts` | IceCube | JSON |
| `gcn.notices.icecube.lvk_nu_track_search` | IceCube | JSON |
| `gcn.notices.swift.bat.guano` | Swift BAT | JSON |
| `gcn.circulars` | Circulars | JSON |
| `gcn.classic.text.FERMI_GBM_ALERT`, `FERMI_GBM_FLT_POS`, `FERMI_GBM_GND_POS`, `FERMI_GBM_FIN_POS`, `FERMI_GBM_SUBTHRESH` | Fermi GBM | Classic text |
| `gcn.classic.text.FERMI_LAT_POS_INI`, `FERMI_LAT_POS_UPD`, `FERMI_LAT_GND`, `FERMI_LAT_OFFLINE`, `FERMI_LAT_TRANS` | Fermi LAT | Classic text |
| `gcn.classic.text.SWIFT_BAT_GRB_POS_ACK` | Swift BAT | Classic text |
| `gcn.classic.text.SWIFT_XRT_POSITION` | Swift XRT | Classic text |
| `gcn.classic.text.SWIFT_UVOT_POS` | Swift UVOT | Classic text |

All classic suffixes in a row use the same `gcn.classic.text.` prefix. The registry contains the full strings. Availability is verified against broker metadata at startup; access was not assumed from documentation alone. Missing topics fail clearly and can be removed explicitly via `GCN_TOPICS`.

Original message bytes are retained as base64 alongside their UTF-8 text representation; invalid UTF-8 is quarantined. Classic measurements remain in `classic_fields` so units and original formatting are not silently altered. GUANO and IceCube string-array IDs are handled explicitly. The IceCube LVK-search legacy schema has no `alert_tense`; its valid LVK identifier distinguishes live (`S...`) and mock/test (`MS...`/`TS...`) references. Other JSON notice schemas require a known `alert_tense`.

Parser implementation references, checked September 23, 2026:

- https://gcn.nasa.gov/docs/client
- https://gcn.nasa.gov/missions/fermi
- https://gcn.nasa.gov/missions/swift
- https://gcn.nasa.gov/missions/icecube
- https://gcn.nasa.gov/missions/lvk
- https://emfollow.docs.ligo.org/userguide/content.html
- https://gcn.nasa.gov/docs/circulars/subscribing
- https://github.com/nasa-gcn/gcn-schema/blob/main/gcn/notices/icecube/lvk_nu_track_search.example.json
- https://gcn.nasa.gov/docs/schema/v7.1.0/gcn/notices/swift/bat/Guano.schema.json

Schema drift is quarantined instead of silently inventing missing timestamps/identifiers. This is operational parsing and domain validation, not full remote JSON-Schema validation. The consumer never fetches arbitrary schema or payload URLs during ingestion.
