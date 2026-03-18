from fastapi import APIRouter, HTTPException

from app.tasks.pipeline import run_crawl_ingest_pipeline

router = APIRouter(prefix="/crawl", tags=["crawl"])


@router.post("/trigger")
async def trigger_crawl() -> dict:
    try:
        result = await run_crawl_ingest_pipeline()
        return {"status": "ok", "result": result}
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc
