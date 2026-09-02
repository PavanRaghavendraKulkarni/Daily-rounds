import uuid
from datetime import datetime

from pydantic import BaseModel, Field

from app.models import FileRecord, FileStatus


class CreateUploadRequest(BaseModel):
    filename: str
    total_size: int = Field(gt=0)


class CreateUploadResponse(BaseModel):
    file_id: uuid.UUID
    upload_url: str


class UploadStatusResponse(BaseModel):
    file_id: uuid.UUID
    filename: str
    status: FileStatus
    total_size: int
    bytes_received: int
    chunks_indexed: int
    error_message: str | None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_record(cls, record: FileRecord) -> "UploadStatusResponse":
        # FileRecord's primary key attribute is `id`, not `file_id` — explicit
        # field-by-field mapping here avoids relying on model_validate's
        # attribute-name matching, which silently fails for renamed fields.
        return cls(
            file_id=record.id,
            filename=record.filename,
            status=record.status,
            total_size=record.total_size,
            bytes_received=record.bytes_received,
            chunks_indexed=record.chunks_indexed,
            error_message=record.error_message,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )


class UploadChunkResponse(BaseModel):
    file_id: uuid.UUID
    bytes_received: int
    total_size: int
    status: FileStatus


class SearchRequest(BaseModel):
    query: str = Field(min_length=1)
    top_k: int = Field(default=5, ge=1, le=50)


class SearchResult(BaseModel):
    chunk_index: int
    start_offset: int
    end_offset: int
    text: str
    score: float


class SearchResponse(BaseModel):
    file_id: uuid.UUID
    query: str
    results: list[SearchResult]
    cached: bool = False


class SectionResponse(BaseModel):
    file_id: uuid.UUID
    start: int
    end: int
    content: str
    cached: bool = False
