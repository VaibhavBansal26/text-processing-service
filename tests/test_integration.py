from fastapi.testclient import TestClient

from api import app


def test_post_streams_creates_stream_id():
    client = TestClient(app)

    r = client.post("/streams")
    assert r.status_code == 200
    body = r.json()
    assert "stream_id" in body
    assert isinstance(body["stream_id"], str)
    assert len(body["stream_id"]) > 0


def test_stream_lifecycle_add_chunks_then_stats():
    client = TestClient(app)

    # Create stream
    stream_id = client.post("/streams").json()["stream_id"]

    # Add chunks
    r1 = client.post(f"/streams/{stream_id}/chunks", json={"text": "Hello wor", "priority": "high"})
    assert r1.status_code == 200
    assert r1.json()["status"] in ("accepted", "rate_limited")

    r2 = client.post(f"/streams/{stream_id}/chunks", json={"text": "ld ", "priority": "high"})
    assert r2.status_code == 200
    assert r2.json()["status"] in ("accepted", "rate_limited")

    # Get stats
    r3 = client.get(f"/streams/{stream_id}/stats")
    assert r3.status_code == 200
    stats = r3.json()

    assert stats["word_count"] == 2
    assert stats["unique_words"] == 2
    assert "avg_word_length" in stats
    assert "flags" in stats
    assert isinstance(stats["flags"], list)


def test_get_stats_stream_not_found_404():
    client = TestClient(app)

    r = client.get("/streams/not-real/stats")
    assert r.status_code == 404


def test_delete_stream_then_stats_is_404():
    client = TestClient(app)

    stream_id = client.post("/streams").json()["stream_id"]

    r = client.delete(f"/streams/{stream_id}")
    assert r.status_code == 200
    assert r.json()["ok"] is True

    r2 = client.get(f"/streams/{stream_id}/stats")
    assert r2.status_code == 404
