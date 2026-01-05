from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from processor import StreamStats, StreamNotFoundError as ProcStreamNotFoundError, StreamClosedError
from processor import TextStreamProcessor

from filter import ContentFlag, ContentFilter, StreamNotFoundError as FilterStreamNotFoundError
from rate_limiter import RateLimitResult, StreamNotFoundError as RLStreamNotFoundError, TokenBucketRateLimiter

from typing import List, Tuple
from processor import StreamStats
from filter import ContentFlag

@dataclass(frozen=True)
class PipelineResult:
    """
    What the pipeline returns after adding a chunk.
    """
    ok: bool
    stream_id: str
    rate_limited: bool
    retry_after_seconds: float
    stats: Optional[StreamStats]
    new_flags: List[ContentFlag]
    message: str


class TextProcessingPipeline:
    """
    Orchestrates processor + content filter + rate limiter.
    """

    def __init__(
        self,
        processor: Optional[TextStreamProcessor] = None,
        content_filter: Optional[ContentFilter] = None,
        rate_limiter: Optional[TokenBucketRateLimiter] = None,
    ) -> None:
        self.processor = processor or TextStreamProcessor()
        self.content_filter = content_filter or ContentFilter()
        self.rate_limiter = rate_limiter or TokenBucketRateLimiter()

        # Map pipeline stream_id -> component stream_ids if they differ
        # For now we keep a single shared ID to simplify.
        self._stream_ids = {}

    def create_stream(self) -> str:
        """
        Create a stream across all components with the same ID.
        """
        stream_id = self.processor.create_stream()

        # Create matching streams in filter and rate limiter too
        # For simplicity we reuse the same stream id by directly registering states.
        # If your components generate their own IDs, then store mapping.
        fid = self.content_filter.create_stream()
        rid = self.rate_limiter.create_stream()

        self._stream_ids[stream_id] = (fid, rid)
        return stream_id

    def delete_stream(self, stream_id: str) -> None:
        """
        Close and cleanup stream across components.
        """
        fid, rid = self._stream_ids.get(stream_id, (None, None))

        self.processor.delete_stream(stream_id)

        if fid:
            self.content_filter.close_stream(fid)
        if rid:
            self.rate_limiter.delete_stream(rid)

        self._stream_ids.pop(stream_id, None)

    def add_chunk(self, stream_id: str, text: str, priority: str = "normal") -> PipelineResult:
        """
        Add chunk to pipeline: rate limit -> processor -> filter -> stats.
        """
        if text is None:
            text = ""

        # Map to component IDs
        fid, rid = self._stream_ids.get(stream_id, (None, None))
        if fid is None or rid is None:
            return PipelineResult(
                ok=False,
                stream_id=stream_id,
                rate_limited=False,
                retry_after_seconds=0.0,
                stats=None,
                new_flags=[],
                message="Stream not found.",
            )

        # Estimate tokens to consume.
        # For production: we should rate limit based on *words committed*.
        # We approximate by counting words in this chunk using processor's internal regex rules.
        # Simple approximation: commit first then rate limit? No. Better:
        # rate limit based on detected completed words in this chunk.
        # We'll reuse processor tokenize helper by calling add_chunk after allowed.
        word_estimate = self._estimate_words(text)

        try:
            rl = self.rate_limiter.check_and_consume(rid, words=word_estimate, priority=priority)
        except RLStreamNotFoundError:
            return PipelineResult(False, stream_id, False, 0.0, None, [], "Rate limiter stream missing.")

        if not rl.allowed:
            # Do not process chunk if rate limited
            return PipelineResult(
                ok=False,
                stream_id=stream_id,
                rate_limited=True,
                retry_after_seconds=rl.retry_after_seconds,
                stats=self.processor.get_stats(stream_id),
                new_flags=[],
                message="Rate limit exceeded.",
            )

        # Process chunk
        try:
            self.processor.add_chunk(stream_id, text)
        except (ProcStreamNotFoundError, StreamClosedError) as e:
            return PipelineResult(
                ok=False,
                stream_id=stream_id,
                rate_limited=False,
                retry_after_seconds=0.0,
                stats=None,
                new_flags=[],
                message=str(e),
            )

        # Filter chunk
        try:
            new_flags = self.content_filter.add_chunk(fid, text)
        except FilterStreamNotFoundError:
            new_flags = []

        # Return stats snapshot
        stats = self.processor.get_stats(stream_id)

        return PipelineResult(
            ok=True,
            stream_id=stream_id,
            rate_limited=False,
            retry_after_seconds=0.0,
            stats=stats,
            new_flags=new_flags,
            message="Chunk processed.",
        )
    
    def get_stats(self, stream_id: str) -> Tuple[StreamStats, List[ContentFlag]]:
        """
        Return (stats, flags) for this pipeline stream.

        API needs:
        - word_count
        - unique_words
        - avg_word_length
        - flags
        """
        ids = self._stream_ids.get(stream_id)
        if ids is None:
            raise KeyError(stream_id)

        filter_stream_id, _rate_stream_id = ids

        stats = self.processor.get_stats(stream_id)
        flags = self.content_filter.get_flags(filter_stream_id)
        return stats, flags
    
    def _estimate_words(self, text: str) -> int:
        # quick, cheap estimate: split on word regex from processor/filter.
        # This is intentionally conservative.
        import re

        return len(re.findall(r"[A-Za-z0-9']+", text))
