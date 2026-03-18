from datetime import datetime

from pydantic import BaseModel, Field


class ProcessedRecord(BaseModel):
    source: str
    external_id: str
    title: str
    category: str
    content_length: int
    processed_at: datetime = Field(default_factory=datetime.utcnow)
