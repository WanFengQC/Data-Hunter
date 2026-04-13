from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Data Hunter API"
    app_env: str = "dev"
    api_prefix: str = "/api/v1"

    pg_host: str = "192.168.110.107"
    pg_user: str = "postgres"
    pg_pass: str = "123456"
    pg_port: int = 5432
    pg_db: str = "hunter"
    pg_schema: str = "public"
    pg_table: str = "seller_sprite_items"
    pg_pool_min_size: int = 1
    pg_pool_max_size: int = 12
    pg_pool_wait_timeout_seconds: float = 10.0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


settings = Settings()
