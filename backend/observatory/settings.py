import os
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv()


def env(name, default=""):
    return field(default_factory=lambda: os.getenv(name, default))


@dataclass
class Settings:
    database_url: str = env("DATABASE_URL", "sqlite:///./data/observatory.db")
    transport: str = env("TRANSPORT", "kafka")
    kafka_bootstrap: str = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    kafka_group: str = env("KAFKA_GROUP", "observatory-dispatch-v1")
    kafka_replication: int = field(default_factory=lambda: int(os.getenv("KAFKA_REPLICATION_FACTOR", "1")))
    workers: int = field(default_factory=lambda: int(os.getenv("WORKERS", "4")))
    embedded_runtime: bool = field(default_factory=lambda: os.getenv("EMBEDDED_RUNTIME", "false") == "true")
    admin_token: str = env("ADMIN_TOKEN")
    public_url: str = env("PUBLIC_URL", "http://localhost:8000")
    cors_origins: str = env("CORS_ORIGINS", "http://localhost:5173,https://sacohen11.github.io")
    gcn_client_id: str = env("GCN_CLIENT_ID")
    gcn_client_secret: str = env("GCN_CLIENT_SECRET")
    gcn_bootstrap: str = env("GCN_BOOTSTRAP_SERVERS", "kafka.gcn.nasa.gov:9092")
    gcn_group: str = env("GCN_GROUP_ID", "space-observatory-v1")
    gcn_offset_reset: str = env("GCN_AUTO_OFFSET_RESET", "latest")
    gcn_topics: str = env("GCN_TOPICS")
    model: str = env("LLM_MODEL")
    model_base_url: str = env("LLM_BASE_URL", "https://api.openai.com/v1")
    model_api_key: str = env("LLM_API_KEY")
    model_daily_tokens: int = field(
        default_factory=lambda: int(os.getenv("LLM_DAILY_TOKEN_BUDGET", "200000"))
    )
    model_max_tokens: int = 1200
    lease_seconds: float = 90
    max_attempts: int = 5
    poll_seconds: float = 0.1
    checkpoint_dir: str = "data/observatory-checkpoints"

    def validate(self):
        if self.transport not in {"local", "kafka"}:
            raise ValueError("TRANSPORT must be local or kafka")
        if len(self.admin_token) < 24:
            raise ValueError("ADMIN_TOKEN must contain at least 24 characters")
        if self.gcn_offset_reset not in {"earliest", "latest"}:
            raise ValueError("GCN_AUTO_OFFSET_RESET must be earliest or latest")
        if self.workers < 1:
            raise ValueError("WORKERS must be positive")
        if bool(self.model) != bool(self.model_api_key):
            raise ValueError("Configure both LLM_MODEL and LLM_API_KEY, or leave both empty")
        if urlparse(self.model_base_url).scheme not in {"http", "https"}:
            raise ValueError("LLM_BASE_URL must be HTTP(S)")
        if self.database_url.startswith("sqlite"):
            Path("data").mkdir(exist_ok=True)
