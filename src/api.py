from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Literal, Optional

from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel, Field

from pipeline import TextProcessingPipeline

from advance_features.observability import (
    configure_logging,
    new_correlation_id,
    set_correlation_id,
    set_stream_id,
    inc_counter,
    observe_latency_ms,
    render_metrics,
)

from advance_features.persistence import SQLitePersistenceStore

configure_logging()
log = logging.getLogger("api")


def _build_pipeline() -> TextProcessingPipeline:
    # persistence optional via SQLITE_DB_PATH
    db_path = os.environ.get("SQLITE_DB_PATH")
    store = SQLitePersistenceStore(db_path) if db_path else None
    return TextProcessingPipeline(store=store)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Make sure state exists for tests that use TestClient context manager
    app.state.pipeline_sync = _build_pipeline()
    yield


app = FastAPI(title="Text Processing Service", lifespan=lifespan)


# ----------------------------
# Middleware
# ----------------------------

@app.middleware("http")
async def tracing_and_metrics(request: Request, call_next):
    cid = request.headers.get("X-Correlation-ID") or new_correlation_id()
    set_correlation_id(cid)

    start = time.perf_counter()
    status_code = 500
    response: Optional[Response] = None
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        route_key = f"{request.method} {request.url.path}"

        inc_counter("http_requests_total", 1)
        inc_counter(f"http_requests_status_{status_code}", 1)
        observe_latency_ms(route_key, elapsed_ms)

        if response is not None:
            try:
                response.headers["X-Correlation-ID"] = cid
            except Exception:
                pass


def _get_pipeline_sync() -> TextProcessingPipeline:
    return app.state.pipeline_sync


# ----------------------------
# Observability endpoints
# ----------------------------

@app.get("/healthz")
def healthz():
    pipeline = _get_pipeline_sync()

    sqlite_ok = False
    try:
        store = getattr(pipeline, "_store", None)
        if store is not None and hasattr(store, "health_check"):
            sqlite_ok = bool(store.health_check())
    except Exception:
        sqlite_ok = False

    return {
        "ok": True,
        "dependencies": {
            "pipeline": True,
            "sqlite": sqlite_ok,
        },
    }


@app.get("/metrics")
def metrics() -> Response:
    return Response(content=render_metrics(), media_type="text/plain")


# ----------------------------
# API models
# ----------------------------

class CreateStreamResponse(BaseModel):
    stream_id: str


class AddChunkRequest(BaseModel):
    text: str = Field(default="")
    priority: Literal["normal", "high"] = "normal"


class AddChunkResponse(BaseModel):
    status: str  # accepted | rate_limited
    stream_id: str
    retry_after_seconds: Optional[float] = None
    message: str


class StatsResponse(BaseModel):
    word_count: int
    unique_words: int
    avg_word_length: float
    flags: List[Dict[str, Any]]


# ----------------------------
# Endpoints
# ----------------------------

@app.post("/streams", response_model=CreateStreamResponse)
def create_stream() -> CreateStreamResponse:
    stream_id = _get_pipeline_sync().create_stream()
    inc_counter("streams_created_total", 1)
    log.info("stream created", extra={"extra": {"stream_id": stream_id}})
    return CreateStreamResponse(stream_id=stream_id)


@app.post("/streams/{stream_id}/chunks", response_model=AddChunkResponse)
def add_chunk(stream_id: str, body: AddChunkRequest, response: Response) -> AddChunkResponse:
    set_stream_id(stream_id)

    result = _get_pipeline_sync().add_chunk(stream_id, body.text, priority=body.priority)

    if not result.ok and "not found" in result.message.lower():
        inc_counter("chunks_stream_not_found_total", 1)
        raise HTTPException(status_code=404, detail={"message": "Stream not found.", "stream_id": stream_id})

    if result.rate_limited:
        inc_counter("chunks_rate_limited_total", 1)
        response.headers["Retry-After"] = str(int(max(1.0, result.retry_after_seconds)))
        return AddChunkResponse(
            status="rate_limited",
            stream_id=stream_id,
            retry_after_seconds=result.retry_after_seconds,
            message="Rate limit exceeded. Retry later.",
        )

    inc_counter("chunks_accepted_total", 1)
    return AddChunkResponse(
        status="accepted",
        stream_id=stream_id,
        retry_after_seconds=None,
        message="Chunk accepted and processed.",
    )


@app.get("/streams/{stream_id}/stats", response_model=StatsResponse)
def get_stats(stream_id: str) -> StatsResponse:
    set_stream_id(stream_id)

    try:
        stats, flags = _get_pipeline_sync().get_stats(stream_id)
    except KeyError:
        raise HTTPException(status_code=404, detail={"message": "Stream not found.", "stream_id": stream_id})

    return StatsResponse(
        word_count=stats.word_count,
        unique_words=stats.unique_words,
        avg_word_length=stats.avg_word_length,
        flags=[{"rule_id": f.rule_id, "match": f.match, "confidence": f.confidence, "rule_type": f.rule_type} for f in flags],
    )


@app.delete("/streams/{stream_id}")
def delete_stream(stream_id: str) -> Dict[str, Any]:
    set_stream_id(stream_id)
    _get_pipeline_sync().delete_stream(stream_id)
    inc_counter("streams_deleted_total", 1)
    return {"ok": True, "stream_id": stream_id}
