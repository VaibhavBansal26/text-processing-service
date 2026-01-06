from fastapi.testclient import TestClient

from api import app


def test_healthz_returns_dependency_status():
    with TestClient(app) as client:
        r = client.get("/healthz")
        assert r.status_code == 200

        body = r.json()
        assert body["ok"] is True
        assert "dependencies" in body
        assert body["dependencies"]["pipeline"] is True
        assert "sqlite" in body["dependencies"]
        assert isinstance(body["dependencies"]["sqlite"], bool)


def test_metrics_endpoint_returns_text_plain():
    with TestClient(app) as client:
        r = client.get("/metrics")
        assert r.status_code == 200
        # FastAPI sets content-type with charset
        assert r.headers["content-type"].startswith("text/plain")
        assert isinstance(r.text, str)


def test_correlation_id_header_is_returned():
    with TestClient(app) as client:
        r = client.get("/healthz", headers={"X-Correlation-ID": "test-cid-123"})
        assert r.status_code == 200
        assert r.headers.get("X-Correlation-ID") == "test-cid-123"


def test_correlation_id_header_is_generated_when_missing():
    with TestClient(app) as client:
        r = client.get("/healthz")
        assert r.status_code == 200
        cid = r.headers.get("X-Correlation-ID")
        assert cid is not None
        assert isinstance(cid, str)
        assert len(cid) > 0
