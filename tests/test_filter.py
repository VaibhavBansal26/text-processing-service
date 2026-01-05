import re
import pytest

from filter import ContentFilter, ContentFilterConfig, StreamNotFoundError


def test_word_list_detects_split_word_across_chunks():
    cfg = ContentFilterConfig(word_list={"ssn": 0.95}, lookback_chars=8)
    f = ContentFilter(cfg)
    sid = f.create_stream()

    flags1 = f.add_chunk(sid, "my s")
    flags2 = f.add_chunk(sid, "sn is 123")

    # It should flag ssn once it appears fully in scan text.
    combined = flags1 + flags2
    assert any(fl.rule_id == "word:ssn" for fl in combined)
    flagged = [fl for fl in combined if fl.rule_id == "word:ssn"][0]
    assert abs(flagged.confidence - 0.95) < 1e-9


def test_regex_detects_boundary_spanning_phrase():
    pattern = re.compile(r"credit\s*card", re.IGNORECASE)
    cfg = ContentFilterConfig(regex_rules=[("credit_card", pattern, 0.9)], lookback_chars=32)
    f = ContentFilter(cfg)
    sid = f.create_stream()

    flags1 = f.add_chunk(sid, "my cred")
    flags2 = f.add_chunk(sid, "it card is here")

    combined = flags1 + flags2
    assert any(fl.rule_id == "regex:credit_card" for fl in combined)
    flagged = [fl for fl in combined if fl.rule_id == "regex:credit_card"][0]
    assert abs(flagged.confidence - 0.9) < 1e-9


def test_multiple_streams_are_isolated():
    cfg = ContentFilterConfig(word_list={"password": 0.8}, lookback_chars=32)
    f = ContentFilter(cfg)

    a = f.create_stream()
    b = f.create_stream()

    f.add_chunk(a, "my pass")
    f.add_chunk(b, "hello ")
    flags_a = f.add_chunk(a, "word is here")
    flags_b = f.add_chunk(b, "world")

    assert any(fl.rule_id == "word:password" for fl in flags_a)
    assert not any(fl.rule_id == "word:password" for fl in flags_b)


def test_stream_not_found_raises():
    f = ContentFilter()
    with pytest.raises(StreamNotFoundError):
        f.add_chunk("missing", "hello")
