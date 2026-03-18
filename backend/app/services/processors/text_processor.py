from typing import Any


def normalize_record(raw: dict[str, Any]) -> dict[str, Any]:
    payload = raw.get("payload", {})
    title = str(payload.get("title", "")).strip()
    body = str(payload.get("body", "")).strip()

    category = "long_text" if len(body) >= 100 else "short_text"

    return {
        "source": raw.get("source", "unknown"),
        "external_id": raw.get("external_id", ""),
        "title": title,
        "category": category,
        "content_length": len(body),
    }
