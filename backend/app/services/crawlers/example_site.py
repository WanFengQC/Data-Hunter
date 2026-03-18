from typing import Any

import aiohttp

from app.core.config import settings
from app.services.crawlers.base import BaseCrawler


class JsonPlaceholderCrawler(BaseCrawler):
    async def fetch(self) -> list[dict[str, Any]]:
        timeout = aiohttp.ClientTimeout(total=settings.crawler_timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(settings.crawler_source_url) as response:
                response.raise_for_status()
                data = await response.json()

        if not isinstance(data, list):
            return []

        return data[: settings.crawler_batch_size]


def to_raw_records(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for item in items:
        external_id = str(item.get("id", ""))
        if not external_id:
            continue
        records.append(
            {
                "source": "jsonplaceholder",
                "external_id": external_id,
                "payload": item,
            }
        )
    return records
