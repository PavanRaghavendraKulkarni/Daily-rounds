import pytest

from app.services.chunker import chunk_byte_stream


async def _stream(data: bytes, buf_size: int):
    for i in range(0, len(data), buf_size):
        yield data[i : i + buf_size]


@pytest.mark.asyncio
async def test_chunks_cover_whole_text_with_byte_accurate_offsets():
    """Every chunk's recorded (start_offset, end_offset) must slice the exact
    text it claims to contain — this is what makes search results traceable
    back to real byte ranges via /sections."""
    text = "abcdefghijklmnopqrstuvwxyz" * 10  # 260 chars
    data = text.encode("utf-8")

    chunks = [c async for c in chunk_byte_stream(_stream(data, 7), chunk_chars=50, overlap_chars=10)]

    assert len(chunks) > 1
    for c in chunks:
        assert data[c.start_offset : c.end_offset].decode("utf-8") == c.text

    # Consecutive chunks overlap and together cover the full source with no gaps.
    for prev, nxt in zip(chunks, chunks[1:]):
        assert nxt.start_offset < prev.end_offset
        assert nxt.start_offset > prev.start_offset
    assert chunks[-1].end_offset == len(data)


@pytest.mark.asyncio
async def test_multibyte_characters_split_across_read_buffers_stay_accurate():
    """A tiny read buffer forces multi-byte UTF-8 characters to straddle buffer
    boundaries — the incremental decoder must still produce byte-accurate
    offsets, not just correct decoded text."""
    text = "café bar naïve résumé 日本語 " * 20
    data = text.encode("utf-8")

    chunks = [c async for c in chunk_byte_stream(_stream(data, 3), chunk_chars=40, overlap_chars=5)]

    assert chunks[0].start_offset == 0
    for c in chunks:
        assert data[c.start_offset : c.end_offset].decode("utf-8") == c.text
