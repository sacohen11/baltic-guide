import time
from sqlalchemy import select
from baltic.db import Database, runtime_health
from baltic.settings import Settings

db = Database(Settings().database_url)
with db.engine.connect() as connection:
    records = connection.execute(
        select(runtime_health).where(runtime_health.c.updated_at > time.time() - 90)
    ).mappings()
    if not any(":worker:" in r["id"] for r in records):
        raise SystemExit(1)
