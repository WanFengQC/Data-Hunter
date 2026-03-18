from datetime import datetime, timezone

from app.core.database import mongo
from app.services.crawlers.example_site import JsonPlaceholderCrawler, to_raw_records
from app.services.processors.text_processor import normalize_record


async def run_crawl_ingest_pipeline() -> dict:
    crawler = JsonPlaceholderCrawler()
    raw_items = await crawler.fetch()
    raw_records = to_raw_records(raw_items)

    if not raw_records:
        return {"raw_inserted": 0, "processed_upserted": 0}

    raw_collection = mongo.db["raw_records"]
    processed_collection = mongo.db["processed_records"]

    now = datetime.now(timezone.utc)
    raw_inserted = 0
    processed_upserted = 0

    for record in raw_records:
        record["fetched_at"] = now
        await raw_collection.update_one(
            {"source": record["source"], "external_id": record["external_id"]},
            {"$set": record},
            upsert=True,
        )
        raw_inserted += 1

        processed = normalize_record(record)
        processed["processed_at"] = now
        await processed_collection.update_one(
            {"source": processed["source"], "external_id": processed["external_id"]},
            {"$set": processed},
            upsert=True,
        )
        processed_upserted += 1

    await build_analytics_snapshot()

    return {
        "raw_inserted": raw_inserted,
        "processed_upserted": processed_upserted,
    }


async def build_analytics_snapshot() -> None:
    processed_collection = mongo.db["processed_records"]
    snapshot_collection = mongo.db["analytics_snapshots"]

    pipeline = [
        {
            "$group": {
                "_id": "$category",
                "count": {"$sum": 1},
                "avg_content_length": {"$avg": "$content_length"},
            }
        }
    ]

    rows = await processed_collection.aggregate(pipeline).to_list(length=100)
    snapshot = {
        "generated_at": datetime.now(timezone.utc),
        "categories": [
            {
                "category": row["_id"],
                "count": row["count"],
                "avg_content_length": round(row["avg_content_length"], 2),
            }
            for row in rows
        ],
    }

    await snapshot_collection.insert_one(snapshot)
