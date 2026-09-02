import hashlib
import json
from functools import lru_cache

from redis.asyncio import Redis, from_url

from app.config import get_settings

settings = get_settings()


@lru_cache
def get_redis() -> Redis:
    return from_url(settings.redis_url, decode_responses=True)


def make_cache_key(*parts: str) -> str:
    raw = "|".join(parts)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"{parts[0]}:{digest}"


async def cache_get_json(key: str) -> dict | list | None:
    redis = get_redis()
    raw = await redis.get(key)
    if raw is None:
        return None
    return json.loads(raw)


async def cache_set_json(key: str, value: dict | list, ttl_seconds: int) -> None:
    redis = get_redis()
    await redis.set(key, json.dumps(value), ex=ttl_seconds)


async def cache_invalidate_prefix(prefix: str) -> None:
    redis = get_redis()
    async for key in redis.scan_iter(match=f"{prefix}:*"):
        await redis.delete(key)
