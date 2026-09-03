import logging
import uuid

from arq.connections import RedisSettings
from sqlalchemy import update

from app.cache import cache_invalidate_prefix
from app.config import get_settings
from app.constants import SEARCH_CACHE_PREFIX, SECTION_CACHE_PREFIX
from app.db import SessionLocal
from app.models import ChunkRecord, FileRecord, FileStatus
from app.services.chunker import chunk_byte_stream
from app.services.embeddings import embed_texts
from app.services.storage import iter_file_bytes, storage_path_for

logger = logging.getLogger(__name__)
settings = get_settings()


async def process_file(ctx, file_id: str) -> None:
    """Background job: stream the uploaded file off disk, split it into overlapping
    text windows, embed them in small batches, and persist incrementally.

    Nothing here ever holds the whole file — or the whole embedding set — in memory.
    `chunks_indexed` is updated after every batch commit, so a client polling status
    mid-run sees real progress, not just a final count once everything is done.

    This does NOT implement crash-resume: on failure, `status` is set to `failed`
    with the error captured, but re-running this job on the same file reprocesses
    it from the start (and would insert duplicate chunk rows) rather than resuming
    from the last committed batch. See the README's design discussion (§4) for why
    that's a reasonable next step rather than something implemented here.
    """
    fid = uuid.UUID(file_id)
    async with SessionLocal() as session:
        record = await session.get(FileRecord, fid)
        if record is None:
            logger.warning("process_file: file %s not found", file_id)
            return

        record.status = FileStatus.PROCESSING
        record.error_message = None
        record.chunks_indexed = 0
        await session.commit()

        path = storage_path_for(fid)
        pending: list = []

        try:
            byte_stream = iter_file_bytes(path, settings.index_read_buffer_bytes)
            async for text_chunk in chunk_byte_stream(
                byte_stream, settings.index_chunk_chars, settings.index_chunk_overlap_chars
            ):
                pending.append(text_chunk)
                if len(pending) >= settings.db_flush_batch_size:
                    await _flush_batch(session, record, pending)
                    pending = []

            if pending:
                await _flush_batch(session, record, pending)

            record.status = FileStatus.READY
            await session.commit()
        except Exception as exc:
            logger.exception("indexing failed for file %s", file_id)
            await session.execute(
                update(FileRecord)
                .where(FileRecord.id == fid)
                .values(status=FileStatus.FAILED, error_message=str(exc)[:2000])
            )
            await session.commit()
        finally:
            await cache_invalidate_prefix(f"{SEARCH_CACHE_PREFIX}:{file_id}")
            await cache_invalidate_prefix(f"{SECTION_CACHE_PREFIX}:{file_id}")


async def _flush_batch(session, record: FileRecord, pending: list) -> None:
    texts = [c.text for c in pending]
    embeddings = embed_texts(texts)

    for text_chunk, embedding in zip(pending, embeddings):
        session.add(
            ChunkRecord(
                file_id=record.id,
                chunk_index=text_chunk.chunk_index,
                start_offset=text_chunk.start_offset,
                end_offset=text_chunk.end_offset,
                text=text_chunk.text,
                embedding=embedding,
            )
        )
    record.chunks_indexed += len(pending)
    await session.flush()
    await session.commit()


class WorkerSettings:
    functions = [process_file]
    redis_settings = RedisSettings.from_dsn(settings.redis_url)
    # Bounds worker-process concurrency so simultaneous indexing jobs can't blow
    # past the 4 GB budget together; each job's own memory use is already bounded.
    max_jobs = settings.worker_max_jobs
    job_timeout = settings.worker_job_timeout_seconds
