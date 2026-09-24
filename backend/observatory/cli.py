import argparse
import asyncio
import logging

from .db import Database
from .gcn import GCNIngestor
from .runtime import Runtime
from .service import Service
from .settings import Settings


async def background(mode):
    settings = Settings()
    settings.validate()
    db = Database(settings.database_url)
    service = Service(db, settings)
    service.bootstrap()
    if mode == "ingest":
        await GCNIngestor(service).run()
    else:
        runtime = Runtime(service)
        try:
            await runtime.start()
            await asyncio.Event().wait()
        finally:
            await runtime.stop()


def main():
    parser = argparse.ArgumentParser(description="Live GCN multi-agent observatory")
    parser.add_argument("command", choices=["serve", "worker", "ingest", "migrate"])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    if args.command == "serve":
        import uvicorn

        uvicorn.run("observatory.api:make_app", factory=True, host=args.host, port=args.port)
    elif args.command == "migrate":
        settings = Settings()
        settings.validate()
        Service(Database(settings.database_url), settings).bootstrap()
    else:
        try:
            asyncio.run(background(args.command))
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
