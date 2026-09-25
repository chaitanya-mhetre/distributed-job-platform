"""Settings, read from the environment with the RELAY_ prefix (see .env.example)."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RELAY_", env_file=".env", extra="ignore")

    redis_url: str = "redis://:relaydev@localhost:56384/0"
    database_url: str = "postgresql+asyncpg://relay:relay@localhost:55437/relay"

    # Redis key namespace. Tests use a random one so runs never see each other's data.
    namespace: str = "relay"

    api_key: str = ""
    max_payload_bytes: int = 64 * 1024
    log_level: str = "INFO"


def get_settings() -> Settings:
    return Settings()
