import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app
from app.queue import get_arq_pool


class FakeArqPool:
    def __init__(self):
        self.enqueued = []

    async def enqueue_job(self, name, *args, **kwargs):
        self.enqueued.append((name, args))


@pytest.fixture
def fake_pool():
    """Swap the real arq/Redis pool for an in-memory stub — resume-protocol
    correctness doesn't depend on the queue, and this keeps the test from
    needing Redis up as well as Postgres."""
    pool = FakeArqPool()

    async def _get_fake_pool():
        return pool

    app.dependency_overrides[get_arq_pool] = _get_fake_pool
    yield pool
    app.dependency_overrides.pop(get_arq_pool, None)


@pytest.mark.asyncio
async def test_wrong_offset_is_rejected_with_the_expected_offset_to_resume_from(
    clean_db, fake_pool, tmp_path, monkeypatch
):
    """This is the core of the resumable-upload protocol: if a client's belief
    about how many bytes it already sent doesn't match the server's record, the
    server must refuse the write and tell the client exactly where to resume
    from — never silently accept a wrong offset and corrupt the file."""
    from app.config import get_settings
    from app.services import storage as storage_module

    monkeypatch.setattr(get_settings(), "storage_dir", tmp_path)
    monkeypatch.setattr(storage_module.settings, "storage_dir", tmp_path)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        content = b"0123456789" * 5  # 50 bytes
        resp = await client.post("/uploads", json={"filename": "f.bin", "total_size": len(content)})
        file_id = resp.json()["file_id"]

        # Send the first 20 bytes correctly.
        resp = await client.patch(f"/uploads/{file_id}", content=content[:20], headers={"X-Upload-Offset": "0"})
        assert resp.status_code == 200
        assert resp.json()["bytes_received"] == 20

        # Client (wrongly) believes it's still at offset 0 and retries from there.
        resp = await client.patch(f"/uploads/{file_id}", content=content[:20], headers={"X-Upload-Offset": "0"})
        assert resp.status_code == 409
        assert resp.json()["detail"]["expected_offset"] == 20


@pytest.mark.asyncio
async def test_resuming_from_the_reported_offset_completes_the_upload_correctly(
    clean_db, fake_pool, tmp_path, monkeypatch
):
    """After hitting the 409 above, resuming from the server-reported offset
    (rather than the client's original, wrong guess) must complete the upload
    with byte-exact content — no gaps, no duplication."""
    from app.config import get_settings
    from app.services import storage as storage_module

    monkeypatch.setattr(get_settings(), "storage_dir", tmp_path)
    monkeypatch.setattr(storage_module.settings, "storage_dir", tmp_path)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        content = b"0123456789" * 5  # 50 bytes
        resp = await client.post("/uploads", json={"filename": "f.bin", "total_size": len(content)})
        file_id = resp.json()["file_id"]

        await client.patch(f"/uploads/{file_id}", content=content[:20], headers={"X-Upload-Offset": "0"})

        # Simulate a dropped connection: client re-checks status instead of guessing.
        status = await client.get(f"/uploads/{file_id}")
        expected_offset = status.json()["bytes_received"]
        assert expected_offset == 20

        # Resume from the correct offset with the remaining bytes.
        resp = await client.patch(
            f"/uploads/{file_id}", content=content[20:], headers={"X-Upload-Offset": str(expected_offset)}
        )
        assert resp.status_code == 200
        assert resp.json()["bytes_received"] == len(content)

        resp = await client.post(f"/uploads/{file_id}/complete")
        assert resp.status_code == 200
        assert resp.json()["status"] == "uploaded"

        written = (tmp_path / f"{file_id}.bin").read_bytes()
        assert written == content
