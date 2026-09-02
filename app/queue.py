from arq import ArqRedis, create_pool
from arq.connections import RedisSettings

from app.config import get_settings

settings = get_settings()

_pool: ArqRedis | None = None


async def get_arq_pool() -> ArqRedis:
    global _pool
    if _pool is None:
        _pool = await create_pool(RedisSettings.from_dsn(settings.redis_url))
    return _pool
