import asyncio

from app.core.database import close_mongo_connection, connect_to_mongo
from app.core.indexes import ensure_indexes
from app.tasks.pipeline import run_crawl_ingest_pipeline


async def main() -> None:
    await connect_to_mongo()
    await ensure_indexes()
    result = await run_crawl_ingest_pipeline()
    await close_mongo_connection()
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
