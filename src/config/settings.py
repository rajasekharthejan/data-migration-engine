"""Application settings loaded from environment variables."""

from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings


class DB2Settings(BaseSettings):
    """DB2 mainframe connection settings."""

    db2_host: str = "localhost"
    db2_port: int = 50000
    db2_database: str = "POLDB"
    db2_username: str = "etl_reader"
    db2_password: str = "changeme"
    db2_schema: str = "INSURANCE"

    @property
    def connection_string(self) -> str:
        return (
            f"DATABASE={self.db2_database};"
            f"HOSTNAME={self.db2_host};"
            f"PORT={self.db2_port};"
            f"PROTOCOL=TCPIP;"
            f"UID={self.db2_username};"
            f"PWD={self.db2_password};"
        )


class AWSSettings(BaseSettings):
    """AWS service settings."""

    aws_region: str = "us-east-1"
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    s3_staging_bucket: str = "migration-staging"
    s3_archive_bucket: str = "migration-archive"
    glue_database: str = "migration_transforms"
    glue_iam_role: str = ""


class FASTSettings(BaseSettings):
    """FAST target system settings."""

    fast_api_base_url: str = "https://fast.internal/api/v2"
    fast_api_key: str = "changeme"
    fast_batch_size: int = 500
    fast_max_retries: int = 3
    fast_timeout_seconds: int = 60


class ChatbotSettings(BaseSettings):
    """Azure OpenAI chatbot settings."""

    azure_openai_endpoint: str = ""
    azure_openai_api_key: str = ""
    azure_openai_deployment: str = "gpt-4"
    azure_openai_api_version: str = "2024-02-01"


class Settings(BaseSettings):
    """Root application settings."""

    # Database (metadata store)
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/migration_engine"
    database_url_sync: str = "postgresql://postgres:postgres@localhost:5432/migration_engine"

    # Migration config
    migration_batch_size: int = 10_000
    migration_parallel_workers: int = 8
    migration_max_retries: int = 3
    reconciliation_tolerance: float = 0.001

    # Logging
    log_level: str = "INFO"
    log_format: str = "json"

    # Sub-settings
    db2: DB2Settings = DB2Settings()
    aws: AWSSettings = AWSSettings()
    fast: FASTSettings = FASTSettings()
    chatbot: ChatbotSettings = ChatbotSettings()

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache
def get_settings() -> Settings:
    """Return cached settings singleton."""
    return Settings()
