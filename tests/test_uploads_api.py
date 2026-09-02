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
    pool = FakeArqPool()

    async def _get_fake_pool():
        return pool

    app.dependency_overrides[get_arq_pool] = _get_fake_pool
    yield pool
    app.dependency_overrides.pop(get_arq_pool, None)


@pytest.mark.asyncio
async def test_resumable_upload_full_lifecycle(clean_db, fake_pool, tmp_path, monkeypatch):
    from app.config import get_settings
    from app.services import storage as storage_module

    monkeypatch.setattr(get_settings(), "storage_dir", tmp_path)
    monkeypatch.setattr(storage_module.settings, "storage_dir", tmp_path)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        content = b"hello world, this is a test file for upload"
        resp = await client.post("/uploads", json={"filename": "test.txt", "total_size": len(content)})
        assert resp.status_code == 201
        file_id = resp.json()["file_id"]

        # Upload first half.
        first, second = content[:20], content[20:]
        resp = await client.patch(
            f"/uploads/{file_id}", content=first, headers={"X-Upload-Offset": "0"}
        )
        assert resp.status_code == 200
        assert resp.json()["bytes_received"] == 20

        # Wrong offset should be rejected with the expected offset to resume from.
        resp = await client.patch(
            f"/uploads/{file_id}", content=second, headers={"X-Upload-Offset": "0"}
        )
        assert resp.status_code == 409
        assert resp.json()["detail"]["expected_offset"] == 20

        # Resuming from the correct offset succeeds.
        resp = await client.patch(
            f"/uploads/{file_id}", content=second, headers={"X-Upload-Offset": "20"}
        )
        assert resp.status_code == 200
        assert resp.json()["bytes_received"] == len(content)

        resp = await client.post(f"/uploads/{file_id}/complete")
        assert resp.status_code == 200
        assert resp.json()["status"] == "uploaded"
        assert fake_pool.enqueued == [("process_file", (file_id,))]

        resp = await client.get(f"/uploads/{file_id}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["bytes_received"] == len(content)
        assert body["total_size"] == len(content)


@pytest.mark.asyncio
async def test_get_upload_status_404_for_unknown_id(clean_db):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/uploads/00000000-0000-0000-0000-000000000000")
        assert resp.status_code == 404
