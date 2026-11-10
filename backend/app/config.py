"""Backend settings, read from environment variables (see .env.example).

Every URL is configurable, so the frontend, backend and model server stay
independent services connected only through REST calls.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def _origins() -> list[str]:
    raw = _env("CORS_ORIGINS", "https://localhost:8443,http://localhost:8080")
    return [o.strip() for o in raw.split(",") if o.strip()]


@dataclass
class Settings:
    """Runtime configuration for the LogSentinel API."""

    model_url: str = field(
        default_factory=lambda: _env("MODEL_URL", "http://localhost:5001/invocations")
    )
    model_ping_url: str = field(
        default_factory=lambda: _env("MODEL_PING_URL", "http://localhost:5001/ping")
    )
    model_timeout_s: float = field(default_factory=lambda: float(_env("MODEL_TIMEOUT_S", "60")))
    mlflow_tracking_uri: str = field(
        default_factory=lambda: _env("MLFLOW_TRACKING_URI", "http://localhost:5000")
    )
    registered_model: str = field(default_factory=lambda: _env("REGISTERED_MODEL", "LogSentinel"))
    model_alias: str = field(default_factory=lambda: _env("MODEL_ALIAS", "champion"))
    experiment: str = field(default_factory=lambda: _env("MLFLOW_EXPERIMENT", "LogSentinel"))

    api_key: str = field(default_factory=lambda: _env("API_KEY", "change-me"))
    alert_webhook_token: str = field(
        default_factory=lambda: _env("ALERT_WEBHOOK_TOKEN", "change-me-too")
    )
    cors_origins: list[str] = field(default_factory=_origins)

    repo_dir: Path = field(default_factory=lambda: Path(_env("REPO_DIR", ".")))
    retrain_dir: Path = field(default_factory=lambda: Path(_env("RETRAIN_DIR", "retrain_requests")))
    feedback_file: Path = field(
        default_factory=lambda: Path(_env("FEEDBACK_FILE", "feedback/feedback.jsonl"))
    )

    max_lines: int = field(default_factory=lambda: int(_env("MAX_LINES", "50000")))
    max_upload_bytes: int = field(
        default_factory=lambda: int(_env("MAX_UPLOAD_BYTES", str(5 * 1024 * 1024)))
    )
    model_ping_interval_s: float = field(
        default_factory=lambda: float(_env("MODEL_PING_INTERVAL_S", "15"))
    )


settings = Settings()
