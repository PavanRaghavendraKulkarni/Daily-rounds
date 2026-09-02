import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.cache import cache_get_json, cache_set_json, make_cache_key
from app.config import get_settings
from app.db import get_db
from app.models import ChunkRecord, FileRecord, FileStatus
from app.schemas import SearchRequest, SearchResponse, SearchResult
from app.services.embeddings import embed_query

router = APIRouter(prefix="/files", tags=["search"])
settings = get_settings()


@router.post("/{file_id}/search", response_model=SearchResponse)
async def search_file(file_id: uuid.UUID, body: SearchRequest, db: AsyncSession = Depends(get_db)):
    record = await db.get(FileRecord, file_id)
    if record is None:
        raise HTTPException(status_code=404, detail="file not found")
    if record.status != FileStatus.READY:
        raise HTTPException(status_code=409, detail=f"file is not ready for search (status={record.status})")

    cache_key = make_cache_key(f"search:{file_id}", body.query.strip().lower(), str(body.top_k))
    cached = await cache_get_json(cache_key)
    if cached is not None:
        return SearchResponse(file_id=file_id, query=body.query, results=cached["results"], cached=True)

    query_embedding = embed_query(body.query)

    # pgvector cosine distance (<=>): smaller is more similar. Converted to a
    # 0..1-ish similarity score for the response.
    distance = ChunkRecord.embedding.cosine_distance(query_embedding)
    stmt = (
        select(ChunkRecord, distance.label("distance"))
        .where(ChunkRecord.file_id == file_id)
        .order_by(distance)
        .limit(body.top_k)
    )
    rows = (await db.execute(stmt)).all()

    results = [
        SearchResult(
            chunk_index=chunk.chunk_index,
            start_offset=chunk.start_offset,
            end_offset=chunk.end_offset,
            text=chunk.text,
            score=max(0.0, 1.0 - float(dist)),
        )
        for chunk, dist in rows
    ]

    await cache_set_json(
        cache_key, {"results": [r.model_dump() for r in results]}, settings.search_cache_ttl_seconds
    )
    return SearchResponse(file_id=file_id, query=body.query, results=results, cached=False)
