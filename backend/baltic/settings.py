import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    database_url: str = field(default_factory=lambda: os.getenv("DATABASE_URL", "sqlite:///./data/baltic.db"))
    transport: str = field(default_factory=lambda: os.getenv("TRANSPORT", "local"))
    kafka_bootstrap: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    kafka_replication: int = field(default_factory=lambda: int(os.getenv("KAFKA_REPLICATION_FACTOR", "1")))
    kafka_group: str = "baltic-dispatch-v1"
    workers: int = field(default_factory=lambda: int(os.getenv("WORKERS", "4")))
    embedded_runtime: bool = field(
        default_factory=lambda: os.getenv("EMBEDDED_RUNTIME", "true").lower() == "true"
    )
    demo_mode: bool = field(default_factory=lambda: os.getenv("DEMO_MODE", "true").lower() == "true")
    admin_token: str = field(default_factory=lambda: os.getenv("ADMIN_TOKEN", ""))
    public_url: str = field(default_factory=lambda: os.getenv("PUBLIC_URL", "http://localhost:8000"))
    sources_file: str = field(default_factory=lambda: os.getenv("SOURCES_FILE", "config/sources.yaml"))
    model: str = field(default_factory=lambda: os.getenv("LLM_MODEL", ""))
    model_base_url: str = field(
        default_factory=lambda: os.getenv("LLM_BASE_URL", "https://api.openai.com/v1")
    )
    model_api_key: str = field(default_factory=lambda: os.getenv("LLM_API_KEY", ""))
    model_daily_tokens: int = field(
        default_factory=lambda: int(os.getenv("LLM_DAILY_TOKEN_BUDGET", "200000"))
    )
    model_max_tokens: int = 1200
    lease_seconds: float = 90
    max_attempts: int = 5
    country_theme_cities: int = 3
    baltic_theme_countries: int = 2
    poll_seconds: float = 0.2
    checkpoint_dir: str = "data/checkpoints"

    def validate(self):
        if self.transport not in {"local", "kafka"}:
            raise ValueError("TRANSPORT must be local or kafka")
        if not self.demo_mode and len(self.admin_token) < 24:
            raise ValueError("Set a strong ADMIN_TOKEN (at least 24 characters) outside demo mode")
        if self.database_url.startswith("sqlite"):
            Path("data").mkdir(exist_ok=True)
        if self.workers < 1:
            raise ValueError("WORKERS must be positive")
