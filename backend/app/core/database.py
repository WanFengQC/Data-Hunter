from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from app.core.config import settings


class MongoDB:
    client: Optional[AsyncIOMotorClient] = None
    db: Optional[AsyncIOMotorDatabase] = None


mongo = MongoDB()


async def connect_to_mongo() -> None:
    mongo.client = AsyncIOMotorClient(settings.mongo_uri)
    mongo.db = mongo.client[settings.mongo_db]


async def close_mongo_connection() -> None:
    if mongo.client:
        mongo.client.close()
