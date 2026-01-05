from __future__ import annotations

import re
from dataclasses import dataclass, field
from threading import RLock
from typing import Dict, List, Optional, Pattern, Tuple


@dataclass(frozen=True)
class ContentFlag:
    """
    One flagged finding detected by ContentFilter.
    """
    rule_id: str
    match: str
    confidence: float
    rule_type: str  # "word" or "regex"


@dataclass(frozen=True)
class ContentFilterConfig:
    """
    Configurable rules for the ContentFilter.
    """
    # Simple word rules, case insensitive.
    # Example: {"ssn": 0.95, "password": 0.90}
    word_list: Dict[str, float] = field(default_factory=dict)

    # Regex rules with fixed confidence.
    # Each rule is a tuple: (rule_id, compiled_pattern, confidence)
    regex_rules: List[Tuple[str, Pattern[str], float]] = field(default_factory=list)

    # How many characters to keep as tail between chunks, to catch cross boundary matches.
    lookback_chars: int = 128

    # If true, normalize words to lowercase for word list matching.
    case_insensitive: bool = True


class ContentFilterError(Exception):
    pass


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


@dataclass
class _FilterState:
    """
    Internal per stream state for ContentFilter.
    """
    tail: str = ""  # rolling window used to catch boundary spanning matches
    flags: List[ContentFlag] = field(default_factory=list)
    seen: set[Tuple[str, str]] = field(default_factory=set)  # (rule_id, normalized_match)
    closed: bool = False
    lock: RLock = field(default_factory=RLock)


class ContentFilter:
    """
    Detects potentially problematic content in streaming text data.

    Important: this class is boundary aware.
    It keeps a tail window of previous data so patterns split across chunks are still caught.
    """

    _WORD_RE = re.compile(r"[A-Za-z0-9']+")

    def __init__(self, config: Optional[ContentFilterConfig] = None) -> None:
        self._config = config or ContentFilterConfig()
        self._streams: Dict[str, _FilterState] = {}
        self._registry_lock = RLock()

        # Pre normalized word list for fast lookup
        if self._config.case_insensitive:
            self._word_list = {w.lower(): c for w, c in self._config.word_list.items()}
        else:
            self._word_list = dict(self._config.word_list)

    def create_stream(self) -> str:
        import uuid

        stream_id = str(uuid.uuid4())
        with self._registry_lock:
            self._streams[stream_id] = _FilterState()
        return stream_id

    def close_stream(self, stream_id: str) -> None:
        state = self._get_state(stream_id)
        with state.lock:
            state.closed = True
        with self._registry_lock:
            self._streams.pop(stream_id, None)

    def add_chunk(self, stream_id: str, chunk: str) -> List[ContentFlag]:
        """
        Scan chunk content and return new flags detected in this call.

        Boundary handling:
        We scan (tail + chunk) and then update tail to last lookback_chars.
        """
        if chunk is None:
            chunk = ""

        state = self._get_state(stream_id)

        with state.lock:
            if state.closed:
                return []

            scan_text = state.tail + chunk

            new_flags: List[ContentFlag] = []
            new_flags.extend(self._scan_word_list(state, scan_text))
            new_flags.extend(self._scan_regex_rules(state, scan_text))

            # Update rolling tail window
            if self._config.lookback_chars <= 0:
                state.tail = ""
            else:
                state.tail = scan_text[-self._config.lookback_chars :]

            # Save flags to stream history
            if new_flags:
                state.flags.extend(new_flags)

            return new_flags

    def get_flags(self, stream_id: str) -> List[ContentFlag]:
        state = self._get_state(stream_id)
        with state.lock:
            return list(state.flags)

    # ---------------- internal helpers ----------------

    def _get_state(self, stream_id: str) -> _FilterState:
        with self._registry_lock:
            state = self._streams.get(stream_id)
        if state is None:
            raise StreamNotFoundError(stream_id)
        return state

    def _normalize_match(self, s: str) -> str:
        return s.lower() if self._config.case_insensitive else s

    def _scan_word_list(self, state: _FilterState, text: str) -> List[ContentFlag]:
        """
        Word list scanning.
        This uses word tokenization instead of substring matching to avoid false positives.
        """
        matches = self._WORD_RE.findall(text)
        found: List[ContentFlag] = []

        for token in matches:
            key = self._normalize_match(token)
            confidence = self._word_list.get(key)
            if confidence is None:
                continue

            dedup_key = ("word:" + key, key)
            if dedup_key in state.seen:
                continue

            state.seen.add(dedup_key)
            found.append(
                ContentFlag(
                    rule_id=f"word:{key}",
                    match=token,
                    confidence=float(confidence),
                    rule_type="word",
                )
            )

        return found

    def _scan_regex_rules(self, state: _FilterState, text: str) -> List[ContentFlag]:
        """
        Regex scanning.
        Each regex rule has its own confidence.
        """
        found: List[ContentFlag] = []

        for rule_id, pattern, confidence in self._config.regex_rules:
            for m in pattern.finditer(text):
                match_text = m.group(0)
                norm = self._normalize_match(match_text)

                dedup_key = (f"regex:{rule_id}", norm)
                if dedup_key in state.seen:
                    continue

                state.seen.add(dedup_key)
                found.append(
                    ContentFlag(
                        rule_id=f"regex:{rule_id}",
                        match=match_text,
                        confidence=float(confidence),
                        rule_type="regex",
                    )
                )

        return found
