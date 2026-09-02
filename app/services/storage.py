import uuid
from pathlib import Path

import aiofiles

from app.config import get_settings

settings = get_settings()


def storage_path_for(file_id: uuid.UUID) -> Path:
    return settings.storage_dir / f"{file_id}.bin"


async def write_chunk_at_offset(path: Path, offset: int, data: bytes) -> None:
    """Append a byte range at a known offset without ever holding the full file in memory."""
    async with aiofiles.open(path, "r+b" if path.exists() else "w+b") as f:
        await f.seek(offset)
        await f.write(data)


async def read_range(path: Path, start: int, end: int) -> bytes:
    async with aiofiles.open(path, "rb") as f:
        await f.seek(start)
        return await f.read(end - start)


async def iter_file_bytes(path: Path, buffer_size: int):
    """Stream a file in fixed-size chunks so indexing memory is bounded by buffer_size,
    independent of the source file's total size."""
    async with aiofiles.open(path, "rb") as f:
        while True:
            data = await f.read(buffer_size)
            if not data:
                break
            yield data
