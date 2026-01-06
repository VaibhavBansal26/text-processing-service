import os
import importlib
import pytest

pytestmark = pytest.mark.integration


@pytest.mark.skipif(not os.environ.get("REDIS_URL"), reason="REDIS_URL not set")
def test_distributed_state_shared_across_instances(monkeypatch):
    # Force distributed mode ONLY for this test
    monkeypatch.setenv("DISTRIBUTED_MODE", "1")

    # IMPORTANT:
    # If pipeline reads DISTRIBUTED_MODE at import time, we must reload it
    # after setting the env var (because other tests import pipeline earlier).
    import pipeline as pipeline_mod
    importlib.reload(pipeline_mod)

    TextProcessingPipeline = pipeline_mod.TextProcessingPipeline

    p1 = TextProcessingPipeline()
    p2 = TextProcessingPipeline()

    stream_id = p1.create_stream()

    r = p2.add_chunk(stream_id, "hello wor", priority="normal")
    assert r.ok, r.message

    r = p1.add_chunk(stream_id, "ld again", priority="normal")
    assert r.ok, r.message

    stats, flags = p2.get_stats(stream_id)
    assert stats.word_count == 3
    assert stats.unique_words >= 3
    assert stats.avg_word_length > 0.0
    assert isinstance(flags, list)
