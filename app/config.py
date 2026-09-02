from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://app:app@localhost:5432/filesearch"
    redis_url: str = "redis://localhost:6379/0"

    storage_dir: Path = Path("./storage")

    # Resumable upload: max bytes accepted in a single PATCH chunk request.
    upload_chunk_max_bytes: int = 16 * 1024 * 1024

    # Indexing: text is windowed into overlapping chunks for embedding.
    index_chunk_chars: int = 1000
    index_chunk_overlap_chars: int = 150
    # Bounded read buffer used while streaming the source file during indexing —
    # this (not file size) determines worker memory usage.
    index_read_buffer_bytes: int = 4 * 1024 * 1024

    embedding_batch_size: int = 32
    embedding_model_name: str = "all-MiniLM-L6-v2"
    embedding_dim: int = 384

    search_cache_ttl_seconds: int = 300
    section_cache_ttl_seconds: int = 300


@lru_cache
def get_settings() -> Settings:
    settings = Settings()
    settings.storage_dir.mkdir(parents=True, exist_ok=True)
    return settings
