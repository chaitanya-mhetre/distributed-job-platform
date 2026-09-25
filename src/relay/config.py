"""Settings, read from the environment with the RELAY_ prefix (see .env.example)."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RELAY_", env_file=".env", extra="ignore")

    redis_url: str = "redis://:relaydev@localhost:56384/0"
    database_url: str = "postgresql+asyncpg://relay:relay@localhost:55437/relay"

    # Redis key namespace. Tests use a random one so runs never see each other's data.
    namespace: str = "relay"

    # Connection budget: every process opens at most db_pool_size + db_max_overflow Postgres
    # connections. Total = processes x that, and it must stay under Postgres max_connections
    # (100 by default). The first 8-worker load test hit "too many clients" with 20/process.
    db_pool_size: int = 8
    db_max_overflow: int = 0

    api_key: str = ""
    # Backpressure: reject new jobs (HTTP 503) once a queue holds this many entries. 0 = off.
    # Growing queues are the symptom; rejecting early protects Redis memory and gives the
    # producer a clear signal to slow down instead of silently piling up latency.
    max_queue_depth: int = 100_000
    max_payload_bytes: int = 64 * 1024
    log_level: str = "INFO"


def get_settings() -> Settings:
    return Settings()
