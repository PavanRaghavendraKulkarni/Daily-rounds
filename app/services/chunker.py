import codecs
from collections.abc import AsyncIterator
from dataclasses import dataclass


@dataclass
class TextChunk:
    chunk_index: int
    start_offset: int
    end_offset: int
    text: str


async def chunk_byte_stream(
    byte_stream: AsyncIterator[bytes],
    chunk_chars: int,
    overlap_chars: int,
) -> AsyncIterator[TextChunk]:
    """Turn a stream of raw file bytes into overlapping text windows with exact byte
    offsets, without ever materializing the whole file or the whole decoded text in
    memory. Memory usage is bounded by ~chunk_chars, not file size.

    Uses an incremental UTF-8 decoder so multi-byte characters split across read
    buffers are handled correctly, and offsets stay byte-accurate because each
    decoded piece, re-encoded, has exactly the byte length that was consumed to
    produce it (the decoder defers incomplete trailing sequences internally).
    """
    decoder = codecs.getincrementaldecoder("utf-8")(errors="strict")
    buffer = ""
    buffer_start_offset = 0
    running_offset = 0
    chunk_index = 0
    advance_chars = max(chunk_chars - overlap_chars, 1)

    async for raw in byte_stream:
        piece = decoder.decode(raw)
        if not piece:
            continue
        buffer += piece
        running_offset += len(piece.encode("utf-8"))

        while len(buffer) >= chunk_chars:
            chunk_text = buffer[:chunk_chars]
            end_offset = buffer_start_offset + len(chunk_text.encode("utf-8"))
            yield TextChunk(chunk_index, buffer_start_offset, end_offset, chunk_text)
            chunk_index += 1

            consumed = buffer[:advance_chars]
            buffer_start_offset += len(consumed.encode("utf-8"))
            buffer = buffer[advance_chars:]

    tail = decoder.decode(b"", final=True)
    if tail:
        buffer += tail

    if buffer:
        end_offset = buffer_start_offset + len(buffer.encode("utf-8"))
        yield TextChunk(chunk_index, buffer_start_offset, end_offset, buffer)
