from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Data Hunter API"
    app_env: str = "dev"
    api_prefix: str = "/api/v1"

    mongo_uri: str = "mongodb://admin:123456@192.168.110.107:27017/?authSource=admin"
    mongo_db: str = "testdb"

    # PostgreSQL (for downstream processing/reporting modules)
    pg_host: str = "192.168.110.107"
    pg_user: str = "postgres"
    pg_pass: str = "123456"
    pg_port: int = 5432
    pg_db: str = "hunter"
    pg_schema: str = "public"
    pg_table: str = "seller_sprite_items"

    crawler_source_url: str = "https://jsonplaceholder.typicode.com/posts"
    crawler_timeout: int = 30
    crawler_batch_size: int = 100

    scheduler_enabled: bool = True
    scheduler_cron: str = "0 */2 * * *"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


settings = Settings()
