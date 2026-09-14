"""Typed configuration loaded from environment variables.

Never read os.environ directly elsewhere in the codebase. Everything flows
through Settings so that config errors surface at startup rather than halfway
through a DAG run.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings, populated from the environment or a .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="PIPELINE_",
        extra="ignore",
    )

    # --- environment -----------------------------------------------------
    env: str = Field(default="local", description="local | staging | prod")
    log_level: str = Field(default="INFO")
    log_json: bool = Field(default=False, description="Structured JSON logs; on in prod")

    # --- source ----------------------------------------------------------
    source_base_url: str = Field(default="https://example.com/api")
    source_api_key: SecretStr = Field(default=SecretStr(""))
    request_timeout_seconds: float = Field(default=30.0, gt=0)
    max_retries: int = Field(default=5, ge=0, le=10)

    # --- storage ---------------------------------------------------------
    data_dir: Path = Field(default=Path("data"))
    duckdb_path: Path = Field(default=Path("data/warehouse/warehouse.duckdb"))

    @property
    def landing_dir(self) -> Path:
        """Raw, immutable, as-received files. Never mutated after write."""
        return self.data_dir / "landing"

    def ensure_dirs(self) -> None:
        """Create local storage paths if they are missing."""
        self.landing_dir.mkdir(parents=True, exist_ok=True)
        self.duckdb_path.parent.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings singleton."""
    return Settings()
