import pytest

from app.services.chunker import chunk_byte_stream


async def _stream(data: bytes, buf_size: int):
    for i in range(0, len(data), buf_size):
        yield data[i : i + buf_size]


@pytest.mark.asyncio
async def test_chunks_cover_whole_text_with_overlap():
    text = "abcdefghijklmnopqrstuvwxyz" * 10  # 260 chars
    data = text.encode("utf-8")

    chunks = [c async for c in chunk_byte_stream(_stream(data, 7), chunk_chars=50, overlap_chars=10)]

    assert len(chunks) > 1
    # Every chunk's text matches the original bytes at its recorded offsets.
    for c in chunks:
        assert data[c.start_offset : c.end_offset].decode("utf-8") == c.text

    # Consecutive chunks overlap and together cover the full source with no gaps.
    for prev, nxt in zip(chunks, chunks[1:]):
        assert nxt.start_offset < prev.end_offset
        assert nxt.start_offset > prev.start_offset
    assert chunks[-1].end_offset == len(data)


@pytest.mark.asyncio
async def test_chunks_split_across_read_buffers_stay_byte_accurate_for_multibyte_chars():
    text = "café bar naïve résumé 日本語 " * 20
    data = text.encode("utf-8")

    # A tiny read buffer forces multi-byte characters to straddle buffer boundaries.
    chunks = [c async for c in chunk_byte_stream(_stream(data, 3), chunk_chars=40, overlap_chars=5)]

    reconstructed_start = chunks[0].start_offset
    assert reconstructed_start == 0
    for c in chunks:
        assert data[c.start_offset : c.end_offset].decode("utf-8") == c.text


@pytest.mark.asyncio
async def test_small_input_yields_single_chunk():
    data = b"short text"
    chunks = [c async for c in chunk_byte_stream(_stream(data, 1024), chunk_chars=1000, overlap_chars=150)]
    assert len(chunks) == 1
    assert chunks[0].text == "short text"
    assert chunks[0].start_offset == 0
    assert chunks[0].end_offset == len(data)


@pytest.mark.asyncio
async def test_empty_input_yields_no_chunks():
    async def empty_stream():
        return
        yield  # pragma: no cover

    chunks = [c async for c in chunk_byte_stream(empty_stream(), chunk_chars=100, overlap_chars=10)]
    assert chunks == []
