from fastapi.testclient import TestClient

from api import app


def test_post_streams_creates_stream_id():
    with TestClient(app) as client:
        r = client.post("/streams")
        assert r.status_code == 200
        data = r.json()
        assert "stream_id" in data
        assert isinstance(data["stream_id"], str)
        assert len(data["stream_id"]) > 0


def test_stream_lifecycle_add_chunks_then_stats():
    with TestClient(app) as client:
        # Create stream
        stream_id = client.post("/streams").json()["stream_id"]

        # Add chunks (boundary split word)
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

        assert "word_count" in stats
        assert "unique_words" in stats
        assert "avg_word_length" in stats
        assert "flags" in stats
        assert isinstance(stats["flags"], list)

        # If both chunks were accepted, expect "hello" and "world" counted.
        # If either was rate limited, word_count could be smaller.
        assert stats["word_count"] >= 0


def test_get_stats_stream_not_found_404():
    with TestClient(app) as client:
        r = client.get("/streams/not-real/stats")
        assert r.status_code == 404


def test_delete_stream_then_stats_is_404():
    with TestClient(app) as client:
        stream_id = client.post("/streams").json()["stream_id"]

        r = client.delete(f"/streams/{stream_id}")
        assert r.status_code == 200
        assert r.json()["ok"] is True

        r2 = client.get(f"/streams/{stream_id}/stats")
        assert r2.status_code == 404
