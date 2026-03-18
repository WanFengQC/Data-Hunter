from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class RawRecord(BaseModel):
    source: str
    external_id: str
    payload: dict[str, Any]
    fetched_at: datetime = Field(default_factory=datetime.utcnow)
