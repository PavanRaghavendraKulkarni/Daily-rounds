import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.cache import cache_get_json, cache_set_json, make_cache_key
from app.config import get_settings
from app.constants import SECTION_CACHE_PREFIX
from app.db import get_db
from app.models import FileRecord
from app.schemas import SectionResponse, UploadStatusResponse
from app.services.storage import read_range, storage_path_for

router = APIRouter(prefix="/files", tags=["files"])
settings = get_settings()


@router.get("/{file_id}", response_model=UploadStatusResponse)
async def get_file_status(file_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    record = await db.get(FileRecord, file_id)
    if record is None:
        raise HTTPException(status_code=404, detail="file not found")
    return UploadStatusResponse.from_record(record)


@router.get("/{file_id}/sections", response_model=SectionResponse)
async def get_section(
    file_id: uuid.UUID,
    start: int = Query(ge=0),
    end: int = Query(gt=0),
    db: AsyncSession = Depends(get_db),
):
    if end <= start:
        raise HTTPException(status_code=400, detail="end must be greater than start")

    record = await db.get(FileRecord, file_id)
    if record is None:
        raise HTTPException(status_code=404, detail="file not found")
    if end > record.total_size:
        raise HTTPException(status_code=400, detail="range exceeds file size")

    cache_key = make_cache_key(f"{SECTION_CACHE_PREFIX}:{file_id}", str(start), str(end))
    cached = await cache_get_json(cache_key)
    if cached is not None:
        return SectionResponse(file_id=file_id, start=start, end=end, content=cached["content"], cached=True)

    path = storage_path_for(file_id)
    raw = await read_range(path, start, end)
    content = raw.decode("utf-8", errors="replace")

    await cache_set_json(cache_key, {"content": content}, settings.section_cache_ttl_seconds)
    return SectionResponse(file_id=file_id, start=start, end=end, content=content, cached=False)
