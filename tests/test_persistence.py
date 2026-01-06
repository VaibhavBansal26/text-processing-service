import os
from pathlib import Path

import pytest

from pipeline import TextProcessingPipeline
from advance_features.persistence import SQLitePersistenceStore


@pytest.mark.integration
def test_restart_recovery_restores_stream_state(tmp_path: Path):
    """
    Simulates:
    - process 1: create pipeline, create stream, add chunks
    - "restart": create a new pipeline instance pointing at same SQLite DB
    - verify: stats + flags are restored

    Note:
    This does not start the API server. It tests pipeline + persistence directly.
    """
    db_path = tmp_path / "service.db"
    store = SQLitePersistenceStore(str(db_path))

    # "First run"
    p1 = TextProcessingPipeline(store=store)
    sid = p1.create_stream()
    p1.add_chunk(sid, "Hello wor", priority="high")
    p1.add_chunk(sid, "ld test", priority="high")

    stats1, _flags1 = p1.get_stats(sid)
    assert stats1.word_count >= 2  # "Hello" and "world" should be counted

    # "Restart" - new pipeline instance, same DB
    p2 = TextProcessingPipeline(store=store)

    # should be restored
    stats2, flags2 = p2.get_stats(sid)
    assert stats2.word_count == stats1.word_count
    assert stats2.unique_words == stats1.unique_words
    assert stats2.avg_word_length == stats1.avg_word_length

    # Make sure we can continue from restored state
    p2.add_chunk(sid, " again", priority="high")
    stats3, _flags3 = p2.get_stats(sid)
    assert stats3.word_count == stats2.word_count + 1


@pytest.mark.integration
def test_delete_stream_removes_from_persistence(tmp_path: Path):
    db_path = tmp_path / "service.db"
    store = SQLitePersistenceStore(str(db_path))

    p = TextProcessingPipeline(store=store)
    sid = p.create_stream()
    p.add_chunk(sid, "one two", priority="normal")

    # delete should remove state row
    p.delete_stream(sid)

    # new pipeline should not restore it
    p2 = TextProcessingPipeline(store=store)
    with pytest.raises(Exception):
        p2.get_stats(sid)
