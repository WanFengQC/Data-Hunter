from fastapi import FastAPI

from app.api.v1.router import api_router
from app.core.config import settings
from app.services.postgres_table import close_pg_pool

app = FastAPI(title=settings.app_name)
app.include_router(api_router, prefix=settings.api_prefix)


@app.on_event("shutdown")
def shutdown_event() -> None:
    close_pg_pool()