from fastapi import APIRouter

from app.core.database import mongo

router = APIRouter(prefix="/data", tags=["data"])


@router.get("/raw")
async def list_raw_data(limit: int = 20) -> dict:
    limit = min(max(limit, 1), 200)
    rows = await mongo.db["raw_records"].find({}, {"_id": 0}).limit(limit).to_list(length=limit)
    return {"items": rows, "count": len(rows)}


@router.get("/processed/summary")
async def processed_summary() -> dict:
    latest = await mongo.db["analytics_snapshots"].find_one(
        {},
        {"_id": 0},
        sort=[("generated_at", -1)],
    )
    if not latest:
        return {"generated_at": None, "categories": []}
    return latest
