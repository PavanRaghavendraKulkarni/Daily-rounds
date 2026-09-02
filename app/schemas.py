import uuid
from datetime import datetime

from pydantic import BaseModel, Field

from app.models import FileStatus


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
