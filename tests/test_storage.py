import pytest

from app.services.storage import iter_file_bytes, read_range, write_chunk_at_offset


@pytest.mark.asyncio
async def test_write_chunk_at_offset_supports_out_of_order_resumable_writes(tmp_path):
    path = tmp_path / "upload.bin"
    with open(path, "wb") as f:
        f.truncate(20)  # sparse-allocate, as the upload endpoint does

    await write_chunk_at_offset(path, 10, b"0123456789")
    await write_chunk_at_offset(path, 0, b"abcdefghij")

    assert path.read_bytes() == b"abcdefghij0123456789"


@pytest.mark.asyncio
async def test_read_range_returns_exact_byte_slice(tmp_path):
    path = tmp_path / "file.bin"
    path.write_bytes(b"hello world")

    assert await read_range(path, 0, 5) == b"hello"
    assert await read_range(path, 6, 11) == b"world"


@pytest.mark.asyncio
async def test_iter_file_bytes_streams_in_bounded_buffers_and_covers_whole_file(tmp_path):
    path = tmp_path / "big.bin"
    content = b"x" * 10_000
    path.write_bytes(content)

    collected = b""
    max_piece = 0
    async for piece in iter_file_bytes(path, buffer_size=1000):
        max_piece = max(max_piece, len(piece))
        collected += piece

    assert collected == content
    assert max_piece <= 1000
