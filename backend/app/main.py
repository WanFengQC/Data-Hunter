import sys
from contextlib import asynccontextmanager
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI

# Allow running this file directly: `python backend/app/main.py`.
if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.api.v1.router import api_router
from app.core.config import settings
from app.core.database import close_mongo_connection, connect_to_mongo
from app.core.indexes import ensure_indexes
from app.tasks.pipeline import run_crawl_ingest_pipeline

scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(_: FastAPI):
    await connect_to_mongo()
    await ensure_indexes()

    if settings.scheduler_enabled:
        minute, hour, _, _, _ = settings.scheduler_cron.split(" ")
        scheduler.add_job(
            run_crawl_ingest_pipeline,
            trigger="cron",
            minute=minute,
            hour=hour,
            id="crawl-ingest-job",
            replace_existing=True,
        )
        scheduler.start()

    yield

    if scheduler.running:
        scheduler.shutdown(wait=False)
    await close_mongo_connection()


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.include_router(api_router, prefix=settings.api_prefix)
