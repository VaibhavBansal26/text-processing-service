import time
import pytest

from pipeline import TextProcessingPipeline


@pytest.mark.performance
def test_pipeline_handles_many_streams_quickly():
    """
    Performance test.

    Run explicitly:
      python -m pytest -q -m performance -s
    """
    p = TextProcessingPipeline()

    num_streams = 300
    chunks_per_stream = 20
    chunk_text = "hello world this is a streaming test "

    stream_ids = [p.create_stream() for _ in range(num_streams)]

    start = time.perf_counter()

    for sid in stream_ids:
        for _ in range(chunks_per_stream):
            res = p.add_chunk(sid, chunk_text, priority="high")
            assert res.rate_limited is False

    elapsed = time.perf_counter() - start
    total_chunks = num_streams * chunks_per_stream
    throughput = total_chunks / elapsed if elapsed > 0 else float("inf")

    print(f"Processed {total_chunks} chunks in {elapsed:.2f}s -> {throughput:.1f} chunks/sec")

    # Loose bound so it does not flap across machines
    assert elapsed < 15.0
