from fastapi import APIRouter

from app.api.v1 import crawl, data, health, postgres_data

api_router = APIRouter()
api_router.include_router(health.router)
api_router.include_router(crawl.router)
api_router.include_router(data.router)
api_router.include_router(postgres_data.router)
