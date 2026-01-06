from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from threading import RLock
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class RateLimitResult:
    """
    Result of a rate limit check.

    allowed: True if request can proceed.
    retry_after_seconds: how long the caller should wait before retrying (if not allowed).
    remaining_tokens: best effort estimate of tokens after this decision.
    """
    allowed: bool
    retry_after_seconds: float = 0.0
    remaining_tokens: float = 0.0
    message: str = ""


class StreamNotFoundError(KeyError):
    """Raised when a stream_id does not exist."""

    code = "STREAM_NOT_FOUND"
    http_status = 404

    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.hint = "Create a new stream and retry."
        super().__init__(f"Stream '{stream_id}' not found.")

    def to_dict(self) -> dict:
        return {
            "error": {
                "code": self.code,
                "message": str(self),
                "stream_id": self.stream_id,
                "hint": self.hint,
            }
        }


@dataclass(frozen=True)
class RateLimiterConfig:
    """
    Rate limits per priority.

    units: tokens per minute, where 1 token = 1 word.
    """
    normal_tokens_per_minute: float = 1000.0
    high_tokens_per_minute: float = 5000.0

    # Burst capacity: max tokens bucket can hold.
    normal_burst: Optional[float] = None
    high_burst: Optional[float] = None


@dataclass
class _TokenBucket:
    """
    One token bucket for one stream and priority level.
    """
    tokens: float
    last_refill_ts: float
    refill_rate_per_sec: float
    capacity: float
    lock: RLock = field(default_factory=RLock)


class TokenBucketRateLimiter:
    """
    Token bucket rate limiter.

    - Per stream buckets
    - Priority based limits (normal/high)
    - Thread safe
    - Returns retry-after when blocked
    """

    def __init__(self, config: Optional[RateLimiterConfig] = None) -> None:
        self._cfg = config or RateLimiterConfig()
        self._streams: Dict[str, Dict[str, _TokenBucket]] = {}
        self._registry_lock = RLock()

        # Precompute rates/capacities
        self._normal_rate = self._cfg.normal_tokens_per_minute / 60.0
        self._high_rate = self._cfg.high_tokens_per_minute / 60.0

        self._normal_cap = (
            float(self._cfg.normal_burst)
            if self._cfg.normal_burst is not None
            else float(self._cfg.normal_tokens_per_minute)
        )
        self._high_cap = (
            float(self._cfg.high_burst)
            if self._cfg.high_burst is not None
            else float(self._cfg.high_tokens_per_minute)
        )

    def create_stream(self) -> str:
        stream_id = str(uuid.uuid4())
        self.create_stream_with_id(stream_id)
        return stream_id

    def create_stream_with_id(self, stream_id: str) -> str:
        """
        Create buckets using a caller-provided id (persistence restore).
        """
        now = time.monotonic()
        with self._registry_lock:
            if stream_id not in self._streams:
                self._streams[stream_id] = {
                    "normal": _TokenBucket(
                        tokens=self._normal_cap,
                        last_refill_ts=now,
                        refill_rate_per_sec=self._normal_rate,
                        capacity=self._normal_cap,
                    ),
                    "high": _TokenBucket(
                        tokens=self._high_cap,
                        last_refill_ts=now,
                        refill_rate_per_sec=self._high_rate,
                        capacity=self._high_cap,
                    ),
                }
        return stream_id

    def export_state(self, stream_id: str) -> Dict[str, Any]:
        """
        Export JSON-serializable bucket state.

        We persist token counts. We do NOT persist monotonic timestamps.
        On restore we set last_refill_ts to current time.monotonic() so downtime
        does not "mint" tokens (safe behavior).
        """
        buckets = self._get_stream_buckets(stream_id)
        out: Dict[str, Any] = {"buckets": {}}

        for priority, bucket in buckets.items():
            with bucket.lock:
                out["buckets"][priority] = {
                    "tokens": float(bucket.tokens),
                    "capacity": float(bucket.capacity),
                    "refill_rate_per_sec": float(bucket.refill_rate_per_sec),
                }

        return out

    def import_state(self, stream_id: str, data: Dict[str, Any]) -> None:
        """
        Restore bucket tokens from persistence.
        """
        self.create_stream_with_id(stream_id)
        buckets = self._get_stream_buckets(stream_id)

        raw = data.get("buckets", {})
        if not isinstance(raw, dict):
            return

        now = time.monotonic()
        for priority, payload in raw.items():
            if priority not in buckets:
                continue
            if not isinstance(payload, dict):
                continue

            bucket = buckets[priority]
            with bucket.lock:
                bucket.tokens = float(payload.get("tokens", bucket.tokens))
                bucket.tokens = max(0.0, min(bucket.capacity, bucket.tokens))
                bucket.last_refill_ts = now

    def delete_stream(self, stream_id: str) -> None:
        with self._registry_lock:
            self._streams.pop(stream_id, None)

    def check_and_consume(self, stream_id: str, words: int, priority: str = "normal") -> RateLimitResult:
        """
        Attempt to consume `words` tokens from stream bucket.
        """
        if words <= 0:
            return RateLimitResult(allowed=True, remaining_tokens=0.0, message="No tokens needed.")

        buckets = self._get_stream_buckets(stream_id)
        if priority not in buckets:
            priority = "normal"

        bucket = buckets[priority]

        with bucket.lock:
            self._refill(bucket)

            if bucket.tokens >= words:
                bucket.tokens -= float(words)
                return RateLimitResult(
                    allowed=True,
                    remaining_tokens=bucket.tokens,
                    message="Allowed.",
                )

            needed = float(words) - bucket.tokens
            retry_after = needed / bucket.refill_rate_per_sec if bucket.refill_rate_per_sec > 0 else 60.0

            return RateLimitResult(
                allowed=False,
                retry_after_seconds=retry_after,
                remaining_tokens=bucket.tokens,
                message="Rate limit exceeded.",
            )

    # ---------------- helpers ----------------

    def _get_stream_buckets(self, stream_id: str) -> Dict[str, _TokenBucket]:
        with self._registry_lock:
            buckets = self._streams.get(stream_id)
        if buckets is None:
            raise StreamNotFoundError(stream_id)
        return buckets

    def _refill(self, bucket: _TokenBucket) -> None:
        now = time.monotonic()
        elapsed = now - bucket.last_refill_ts
        if elapsed <= 0:
            return

        added = elapsed * bucket.refill_rate_per_sec
        bucket.tokens = min(bucket.capacity, bucket.tokens + added)
        bucket.last_refill_ts = now
