import uuid

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.models import FileRecord, FileStatus
from app.queue import get_arq_pool
from app.schemas import (
    CreateUploadRequest,
    CreateUploadResponse,
    UploadChunkResponse,
    UploadStatusResponse,
)
from app.services.storage import storage_path_for

router = APIRouter(prefix="/uploads", tags=["uploads"])


@router.post("", response_model=CreateUploadResponse, status_code=201)
async def create_upload(body: CreateUploadRequest, db: AsyncSession = Depends(get_db)):
    file_id = uuid.uuid4()
    path = storage_path_for(file_id)

    # Sparse-allocate the destination file up front. This is a metadata-only
    # operation on any modern filesystem (no data written, no RAM used) and lets
    # chunk writes land at arbitrary offsets, which is what makes resume possible.
    with open(path, "wb") as f:
        f.truncate(body.total_size)

    record = FileRecord(
        id=file_id,
        filename=body.filename,
        storage_path=str(path),
        total_size=body.total_size,
        bytes_received=0,
        status=FileStatus.UPLOADING,
    )
    db.add(record)
    await db.commit()

    return CreateUploadResponse(file_id=file_id, upload_url=f"/uploads/{file_id}")


@router.get("/{file_id}", response_model=UploadStatusResponse)
async def get_upload_status(file_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    record = await db.get(FileRecord, file_id)
    if record is None:
        raise HTTPException(status_code=404, detail="upload not found")
    return UploadStatusResponse.from_record(record)


@router.patch("/{file_id}", response_model=UploadChunkResponse)
async def upload_chunk(file_id: uuid.UUID, request: Request, db: AsyncSession = Depends(get_db)):
    """Accepts one resumable chunk. The client sends the byte offset it believes it's
    at via X-Upload-Offset; if that doesn't match what the server has recorded, the
    server rejects with 409 and the *actual* offset, so the client can seek and
    resume correctly after a dropped connection instead of re-sending the whole file.
    """
    record = await db.get(FileRecord, file_id)
    if record is None:
        raise HTTPException(status_code=404, detail="upload not found")
    if record.status != FileStatus.UPLOADING:
        raise HTTPException(status_code=409, detail=f"upload is not accepting chunks (status={record.status})")

    offset_header = request.headers.get("X-Upload-Offset")
    if offset_header is None:
        raise HTTPException(status_code=400, detail="X-Upload-Offset header is required")
    try:
        offset = int(offset_header)
    except ValueError:
        raise HTTPException(status_code=400, detail="X-Upload-Offset must be an integer")

    if offset != record.bytes_received:
        raise HTTPException(
            status_code=409,
            detail={"message": "offset mismatch, resume from expected_offset", "expected_offset": record.bytes_received},
        )

    path = storage_path_for(file_id)
    max_end = min(record.total_size, offset + 64 * 1024 * 1024)  # hard safety cap per request
    written = 0

    with open(path, "r+b") as f:
        f.seek(offset)
        async for piece in request.stream():
            if offset + written + len(piece) > max_end:
                raise HTTPException(status_code=413, detail="chunk exceeds maximum accepted size or file bounds")
            f.write(piece)
            written += len(piece)

    record.bytes_received = offset + written
    if record.bytes_received > record.total_size:
        raise HTTPException(status_code=400, detail="uploaded bytes exceed declared total_size")
    await db.commit()

    return UploadChunkResponse(
        file_id=file_id, bytes_received=record.bytes_received, total_size=record.total_size, status=record.status
    )


@router.post("/{file_id}/complete", response_model=UploadStatusResponse)
async def complete_upload(
    file_id: uuid.UUID, db: AsyncSession = Depends(get_db), pool=Depends(get_arq_pool)
):
    record = await db.get(FileRecord, file_id)
    if record is None:
        raise HTTPException(status_code=404, detail="upload not found")
    if record.bytes_received != record.total_size:
        raise HTTPException(
            status_code=400,
            detail=f"upload incomplete: {record.bytes_received}/{record.total_size} bytes received",
        )

    record.status = FileStatus.UPLOADED
    await db.commit()
    # updated_at is server-computed (onupdate=func.now()); after commit it's expired
    # and would otherwise trigger an implicit lazy-load outside the async context
    # when read below.
    await db.refresh(record)

    # Request handler returns immediately; the actual indexing work happens out of
    # band in the worker process so this call never blocks on CPU-heavy embedding.
    await pool.enqueue_job("process_file", str(file_id))

    return UploadStatusResponse.from_record(record)
