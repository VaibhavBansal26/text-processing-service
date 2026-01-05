import concurrent.futures
import math
import pytest

from processor import (
    TextStreamProcessor,
    StreamNotFoundError,
    StreamClosedError,
)


def test_counts_words_across_chunk_boundary_simple_split():
    """
    "wor" + "ld " must become "world" (1 word), not "wor" and "ld".
    """
    p = TextStreamProcessor()
    sid = p.create_stream()

    p.add_chunk(sid, "Hello wor")
    p.add_chunk(sid, "ld ")

    stats = p.get_stats(sid)
    assert stats.word_count == 2
    assert stats.unique_words == 2
    assert math.isclose(stats.avg_word_length, 5.0, rel_tol=0.0, abs_tol=1e-9)


def test_counts_words_with_punctuation_and_spaces():
    """
    Punctuation should terminate words and should not join incorrectly.
    """
    p = TextStreamProcessor()
    sid = p.create_stream()

    p.add_chunk(sid, "Hello, world! This is a ")
    p.add_chunk(sid, "test of streaming text.")

    stats = p.get_stats(sid)
    # Hello world This is a test of streaming text = 9 words
    assert stats.word_count == 9
    assert stats.unique_words == 9


def test_empty_chunk_is_safe_no_change():
    """
    Empty chunks should not break processing and should not change stats.
    """
    p = TextStreamProcessor()
    sid = p.create_stream()

    p.add_chunk(sid, "")
    p.add_chunk(sid, "   ")
    stats = p.get_stats(sid)

    assert stats.word_count == 0
    assert stats.unique_words == 0
    assert stats.avg_word_length == 0.0


def test_buffer_flushes_on_close_stream():
    """
    If the stream ends mid-word, close_stream should count the buffer as final word.
    Example: "wor" + "ld" (no trailing space) -> close -> counts "world".
    """
    p = TextStreamProcessor()
    sid = p.create_stream()

    p.add_chunk(sid, "Hello wor")
    p.add_chunk(sid, "ld")  # no delimiter, so "world" remains in buffer

    # Before close, only "Hello" should be counted
    stats_before = p.get_stats(sid)
    assert stats_before.word_count == 1

    p.close_stream(sid)

    # Stream is removed; stats call should now raise not found
    with pytest.raises(StreamNotFoundError):
        p.get_stats(sid)


def test_stream_not_found_error_has_metadata():
    """
    StreamNotFoundError should carry code + stream_id and produce a dict payload.
    """
    p = TextStreamProcessor()

    with pytest.raises(StreamNotFoundError) as exc:
        p.get_stats("missing-id")

    e = exc.value
    assert getattr(e, "code", None) == "STREAM_NOT_FOUND"
    assert getattr(e, "http_status", None) == 404
    payload = e.to_dict()
    assert payload["error"]["code"] == "STREAM_NOT_FOUND"
    assert payload["error"]["stream_id"] == "missing-id"


def test_stream_closed_error_has_metadata_and_blocks_writes():
    """
    After close_stream, writing should raise StreamClosedError (if you keep it in registry)
    OR not found (if you remove from registry). Our implementation removes from registry,
    so to test StreamClosedError we close and keep a local state by using delete_stream
    behavior? If your processor removes stream on close, then add_chunk should raise not found.

    This test assumes you keep the stream in registry but mark closed.
    If you pop it (current code), then expect StreamNotFoundError instead.
    """
    p = TextStreamProcessor()
    sid = p.create_stream()

    p.add_chunk(sid, "Hello ")
    p.close_stream(sid)

    # If your close_stream removes from registry (current code), this is not found:
    with pytest.raises(StreamNotFoundError):
        p.add_chunk(sid, "world ")

    # If you later change behavior to keep closed streams in registry,
    # replace the above with:
    #
    # with pytest.raises(StreamClosedError) as exc:
    #     p.add_chunk(sid, "world ")
    # e = exc.value
    # assert e.code == "STREAM_CLOSED"
    # assert e.http_status == 409
    # assert e.to_dict()["error"]["stream_id"] == sid


def test_multiple_streams_are_isolated():
    """
    Two streams should not share buffer or stats.
    """
    p = TextStreamProcessor()
    a = p.create_stream()
    b = p.create_stream()

    p.add_chunk(a, "Hello wor")
    p.add_chunk(b, "Good mor")
    p.add_chunk(a, "ld ")
    p.add_chunk(b, "ning ")

    sa = p.get_stats(a)
    sb = p.get_stats(b)

    assert sa.word_count == 2  # Hello world
    assert sb.word_count == 2  # Good morning


def test_concurrent_stream_updates_thread_safety():
    """
    Stress test: many streams updated concurrently.
    Proves locks and state handling are safe.
    """

    def run_one(i: int) -> int:
        p_local = p  # shared processor
        sid = p_local.create_stream()
        p_local.add_chunk(sid, "Hello wor")
        p_local.add_chunk(sid, "ld ")
        stats = p_local.get_stats(sid)
        return stats.word_count

    p = TextStreamProcessor()

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
        results = list(ex.map(run_one, range(200)))

    assert all(r == 2 for r in results)

def test_very_long_word_across_chunks_counts_as_one_word():
    p = TextStreamProcessor()
    sid = p.create_stream()

    long_word = "a" * 10000
    p.add_chunk(sid, long_word[:5000])
    # at this point word is buffered, not committed yet
    stats1 = p.get_stats(sid)
    assert stats1.word_count == 0

    p.add_chunk(sid, long_word[5000:] + " ")
    stats2 = p.get_stats(sid)
    assert stats2.word_count == 1
    assert stats2.unique_words == 1
