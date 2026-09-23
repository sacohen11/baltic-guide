import argparse
import asyncio
import json
import logging
import os

from .connectors import ConnectorRunner, load_sources
from .db import Database
from .runtime import Runtime
from .service import Service
from .settings import Settings


def main():
    parser = argparse.ArgumentParser(prog="baltic")
    parser.add_argument("command", choices=["serve", "worker", "connectors", "init", "demo", "schemas"])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    if args.command == "serve":
        import uvicorn

        uvicorn.run("baltic.api:make_app", factory=True, host=args.host, port=args.port)
        return
    settings = Settings()
    settings.validate()
    service = Service(Database(settings.database_url), settings)
    service.bootstrap(load_sources(settings.sources_file))
    if args.command == "init":
        print("Initialized 34 agents and source registry.")
    elif args.command == "schemas":
        from .schemas import Observation, GuidePreferences, Query

        print(
            json.dumps(
                {c.__name__: c.model_json_schema() for c in [Observation, GuidePreferences, Query]}, indent=2
            )
        )
    elif args.command == "demo":
        from .demo import seed_demo

        print(f"Queued {seed_demo(service)} illustrative observations")
        runtime = Runtime(service)
        asyncio.run(runtime.drain())
        runtime.stack.close()
    elif args.command == "connectors":
        asyncio.run(ConnectorRunner(service).run())
    elif args.command == "worker":

        async def run():
            runtime = Runtime(service)
            await runtime.start()
            try:
                await asyncio.Event().wait()
            finally:
                await runtime.stop()

        asyncio.run(run())


if __name__ == "__main__":
    main()
