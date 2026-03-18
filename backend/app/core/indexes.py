from pymongo import ASCENDING, DESCENDING

from app.core.database import mongo


async def ensure_indexes() -> None:
    await mongo.db["raw_records"].create_index(
        [("source", ASCENDING), ("external_id", ASCENDING)],
        unique=True,
        name="raw_source_external_unique",
    )
    await mongo.db["raw_records"].create_index(
        [("fetched_at", DESCENDING)],
        name="raw_fetched_at_desc",
    )

    await mongo.db["processed_records"].create_index(
        [("source", ASCENDING), ("external_id", ASCENDING)],
        unique=True,
        name="processed_source_external_unique",
    )
    await mongo.db["processed_records"].create_index(
        [("category", ASCENDING)],
        name="processed_category",
    )
    await mongo.db["processed_records"].create_index(
        [("processed_at", DESCENDING)],
        name="processed_processed_at_desc",
    )

    await mongo.db["analytics_snapshots"].create_index(
        [("generated_at", DESCENDING)],
        name="snapshot_generated_at_desc",
    )
