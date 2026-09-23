"""
Application configuration, loaded entirely from environment variables.
Nothing here should ever contain a real credential -- see .env.example
for the placeholder values used in local development.
"""
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Postgres connection string, e.g.
    # postgresql+psycopg2://user:password@localhost:5432/dqp
    DATABASE_URL: str = "sqlite:///./dqp_dev.db"

    # Comma-separated list of allowed origins for CORS, e.g.
    # "http://localhost:5173,https://my-frontend.vercel.app"
    CORS_ORIGINS: str = "http://localhost:5173"

    # Maximum accepted upload size, in bytes. Default: 10 MB.
    MAX_UPLOAD_SIZE: int = 10 * 1024 * 1024

    # Directory used to write generated report CSVs before they're served.
    REPORTS_DIR: str = "./reports"

    APP_NAME: str = "Data Quality Monitoring Platform"
    LOG_LEVEL: str = "INFO"

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.CORS_ORIGINS.split(",") if o.strip()]


settings = Settings()
