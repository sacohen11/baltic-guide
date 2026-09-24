import sys
import time

from observatory.db import Database, runtime_health
from observatory.settings import Settings

db = Database(Settings().database_url)
rows = db.rows(runtime_health, runtime_health.c.updated_at > time.time() - 90)
if sys.argv[-1] == "gcn":
    ok = any(r["id"] == "gcn" and r["payload"]["state"] == "connected" for r in rows)
else:
    ok = any(":worker:" in r["id"] for r in rows)
raise SystemExit(0 if ok else 1)
