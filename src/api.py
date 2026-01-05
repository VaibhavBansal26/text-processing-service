from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field

from pipeline import TextProcessingPipeline


app = FastAPI(title="Text Processing Service")

# In memory pipeline instance for this process.
pipeline = TextProcessingPipeline()


class CreateStreamResponse(BaseModel):
    stream_id: str


class AddChunkRequest(BaseModel):
    text: str = Field(default="")
    priority: Literal["normal", "high"] = "normal"


class AddChunkResponse(BaseModel):
    status: str  # "accepted" | "rate_limited"
    stream_id: str
    retry_after_seconds: Optional[float] = None
    message: str


class StatsResponse(BaseModel):
    word_count: int
    unique_words: int
    avg_word_length: float
    flags: List[Dict[str, Any]]


@app.post("/streams", response_model=CreateStreamResponse)
def create_stream() -> CreateStreamResponse:
    """
    Creates a new processing stream.
    Returns: stream_id
    """
    stream_id = pipeline.create_stream()
    return CreateStreamResponse(stream_id=stream_id)


@app.post("/streams/{stream_id}/chunks", response_model=AddChunkResponse)
def add_chunk(stream_id: str, body: AddChunkRequest, response: Response) -> AddChunkResponse:
    """
    Adds a text chunk to the stream.
    Body: {"text": "chunk content", "priority": "normal|high"}
    Returns: processing status
    """
    result = pipeline.add_chunk(stream_id, body.text, priority=body.priority)

    # Stream missing
    if not result.ok and "not found" in result.message.lower():
        raise HTTPException(status_code=404, detail={"message": "Stream not found.", "stream_id": stream_id})

    # Rate limited
    if result.rate_limited:
        # Best practice: include Retry-After header
        response.headers["Retry-After"] = str(int(max(1.0, result.retry_after_seconds)))
        return AddChunkResponse(
            status="rate_limited",
            stream_id=stream_id,
            retry_after_seconds=result.retry_after_seconds,
            message="Rate limit exceeded. Retry later.",
        )

    return AddChunkResponse(
        status="accepted",
        stream_id=stream_id,
        retry_after_seconds=None,
        message="Chunk accepted and processed.",
    )


@app.get("/streams/{stream_id}/stats", response_model=StatsResponse)
def get_stats(stream_id: str) -> StatsResponse:
    """
    Returns current statistics for the stream.
    Includes: word_count, unique_words, avg_word_length, flags
    """
    try:
        stats, flags = pipeline.get_stats(stream_id)
    except KeyError:
        raise HTTPException(status_code=404, detail={"message": "Stream not found.", "stream_id": stream_id})

    return StatsResponse(
        word_count=stats.word_count,
        unique_words=stats.unique_words,
        avg_word_length=stats.avg_word_length,
        flags=[
            {
                "rule_id": f.rule_id,
                "match": f.match,
                "confidence": f.confidence,
                "rule_type": f.rule_type,
            }
            for f in flags
        ],
    )


@app.delete("/streams/{stream_id}")
def delete_stream(stream_id: str) -> Dict[str, Any]:
    """
    Closes and cleans up a stream.
    """
    pipeline.delete_stream(stream_id)
    return {"ok": True, "stream_id": stream_id}
