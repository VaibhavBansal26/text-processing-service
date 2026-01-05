import re
import concurrent.futures
import pytest

from pipeline import TextProcessingPipeline
from filter import ContentFilter, ContentFilterConfig
from rate_limiter import TokenBucketRateLimiter, RateLimiterConfig


def make_pipeline_for_tests():
    """
    Helper to build a pipeline with deterministic filter + low rate limits
    so tests are reliable.
    """
    # Content filter: word list + regex rule
    cfg = ContentFilterConfig(
        word_list={"ssn": 0.95},
        regex_rules=[("credit_card", re.compile(r"credit\s*card", re.IGNORECASE), 0.9)],
        lookback_chars=64,
    )
    content_filter = ContentFilter(cfg)

    # Rate limiter: very small limits so we can trigger 429 logic
    rl_cfg = RateLimiterConfig(
        normal_tokens_per_minute=6,  # 0.1 token/sec
        high_tokens_per_minute=60,   # 1 token/sec
    )
    rate_limiter = TokenBucketRateLimiter(rl_cfg)

    return TextProcessingPipeline(content_filter=content_filter, rate_limiter=rate_limiter)


def test_pipeline_word_boundary_and_stats():
    p = make_pipeline_for_tests()
    sid = p.create_stream()

    r1 = p.add_chunk(sid, "Hello wor", priority="high")  # high so rate limiter won't block
    assert r1.ok is True
    assert r1.stats.word_count == 1  # only "Hello" committed so far (wor is buffered)

    r2 = p.add_chunk(sid, "ld ", priority="high")
    assert r2.ok is True
    assert r2.stats.word_count == 2  # now "world" committed
    assert r2.stats.unique_words == 2


def test_pipeline_content_filter_boundary_spanning_regex():
    p = make_pipeline_for_tests()
    sid = p.create_stream()

    # Split "credit card" across chunks
    r1 = p.add_chunk(sid, "my cred", priority="high")
    assert r1.ok is True
    assert r1.new_flags == []

    r2 = p.add_chunk(sid, "it card is here", priority="high")
    assert r2.ok is True
    assert any(f.rule_id == "regex:credit_card" for f in r2.new_flags)


def test_pipeline_content_filter_word_list_across_chunks():
    p = make_pipeline_for_tests()
    sid = p.create_stream()

    r1 = p.add_chunk(sid, "my s", priority="high")
    assert r1.ok is True

    r2 = p.add_chunk(sid, "sn is 123", priority="high")
    assert r2.ok is True
    assert any(f.rule_id == "word:ssn" for f in r2.new_flags)


def test_pipeline_rate_limiter_blocks_and_returns_retry_after():
    """
    Use normal priority with tiny limit (6/min).
    We send a chunk with >6 words and expect rate limit.
    """
    p = make_pipeline_for_tests()
    sid = p.create_stream()

    # chunk has 10 words
    r = p.add_chunk(sid, "one two three four five six seven eight nine ten", priority="normal")
    assert r.ok is False
    assert r.rate_limited is True
    assert r.retry_after_seconds > 0.0
    assert "Rate limit" in r.message


def test_pipeline_delete_stream_cleans_up():
    p = make_pipeline_for_tests()
    sid = p.create_stream()

    r1 = p.add_chunk(sid, "Hello world", priority="high")
    assert r1.ok is True

    p.delete_stream(sid)

    # After delete, stream should be treated as missing
    r2 = p.add_chunk(sid, "again", priority="high")
    assert r2.ok is False
    assert r2.stats is None or r2.message.lower().find("not found") >= 0


def test_pipeline_concurrent_streams_do_not_leak_state():
    """
    Multiple streams processed in parallel should not share buffers or flags.
    """
    p = make_pipeline_for_tests()

    def run_one(i: int) -> int:
        sid = p.create_stream()
        p.add_chunk(sid, "Hello wor", priority="high")
        p.add_chunk(sid, "ld ", priority="high")
        out = p.add_chunk(sid, "ss", priority="high")
        out2 = p.add_chunk(sid, "n ", priority="high")
        # final should contain ssn flag at least once in the last two calls
        found = any(f.rule_id == "word:ssn" for f in (out.new_flags + out2.new_flags))
        assert found is True
        return out2.stats.word_count

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
        results = list(ex.map(run_one, range(50)))

    # each stream should have: Hello world ssn -> 3 words
    assert all(r == 3 for r in results)
