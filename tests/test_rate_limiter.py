import pytest

from rate_limiter import (
    TokenBucketRateLimiter,
    RateLimiterConfig,
    StreamNotFoundError,
)


def test_allows_small_request_within_capacity():
    rl = TokenBucketRateLimiter(RateLimiterConfig(normal_tokens_per_minute=60))
    sid = rl.create_stream()

    # Capacity defaults to 60 tokens (1 min worth)
    res = rl.check_and_consume(sid, words=10, priority="normal")
    assert res.allowed is True
    assert res.retry_after_seconds == 0.0
    assert res.remaining_tokens >= 49.0  # float math, should be ~50


def test_blocks_when_exceeding_capacity():
    rl = TokenBucketRateLimiter(RateLimiterConfig(normal_tokens_per_minute=60))
    sid = rl.create_stream()

    # Try to consume more than capacity in one go
    res = rl.check_and_consume(sid, words=100, priority="normal")
    assert res.allowed is False
    assert res.retry_after_seconds > 0.0
    assert "Rate limit" in res.message


def test_refill_over_time(monkeypatch):
    """
    Use monkeypatch to control time.monotonic and simulate refill.
    normal_tokens_per_minute=60 => 1 token/sec
    """
    # Controlled clock
    t = {"now": 1000.0}

    def fake_monotonic():
        return t["now"]

    monkeypatch.setattr("rate_limiter.time.monotonic", fake_monotonic)

    rl = TokenBucketRateLimiter(RateLimiterConfig(normal_tokens_per_minute=60))
    sid = rl.create_stream()

    # Consume 60 tokens -> empty bucket
    res1 = rl.check_and_consume(sid, words=60, priority="normal")
    assert res1.allowed is True

    # Immediately try 1 more -> should block (no time passed)
    res2 = rl.check_and_consume(sid, words=1, priority="normal")
    assert res2.allowed is False

    # Advance time by 10 seconds -> should refill ~10 tokens
    t["now"] += 10.0

    res3 = rl.check_and_consume(sid, words=5, priority="normal")
    assert res3.allowed is True  # should now be allowed


def test_high_priority_has_higher_limit(monkeypatch):
    """
    normal=60/min (1/sec), high=600/min (10/sec)
    """
    t = {"now": 2000.0}

    def fake_monotonic():
        return t["now"]

    monkeypatch.setattr("rate_limiter.time.monotonic", fake_monotonic)

    cfg = RateLimiterConfig(
        normal_tokens_per_minute=60,
        high_tokens_per_minute=600,
    )
    rl = TokenBucketRateLimiter(cfg)
    sid = rl.create_stream()

    # Consume normal bucket fully
    res1 = rl.check_and_consume(sid, words=60, priority="normal")
    assert res1.allowed is True

    # Normal bucket should now block
    res2 = rl.check_and_consume(sid, words=1, priority="normal")
    assert res2.allowed is False

    # High bucket should still allow a big burst (capacity defaults to high_tokens_per_minute)
    res3 = rl.check_and_consume(sid, words=200, priority="high")
    assert res3.allowed is True


def test_zero_or_negative_words_is_allowed():
    rl = TokenBucketRateLimiter(RateLimiterConfig(normal_tokens_per_minute=60))
    sid = rl.create_stream()

    res = rl.check_and_consume(sid, words=0, priority="normal")
    assert res.allowed is True

    res = rl.check_and_consume(sid, words=-10, priority="normal")
    assert res.allowed is True


def test_unknown_priority_defaults_to_normal():
    rl = TokenBucketRateLimiter(RateLimiterConfig(normal_tokens_per_minute=60))
    sid = rl.create_stream()

    res = rl.check_and_consume(sid, words=10, priority="weird")
    assert res.allowed is True


def test_stream_not_found_error_has_metadata():
    rl = TokenBucketRateLimiter(RateLimiterConfig(normal_tokens_per_minute=60))

    with pytest.raises(StreamNotFoundError) as exc:
        rl.check_and_consume("missing", words=1, priority="normal")

    e = exc.value
    assert getattr(e, "code", None) == "STREAM_NOT_FOUND"
    assert getattr(e, "http_status", None) == 404
    payload = e.to_dict()
    assert payload["error"]["code"] == "STREAM_NOT_FOUND"
    assert payload["error"]["stream_id"] == "missing"
