from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from threading import RLock
from typing import Dict, Optional, Set


@dataclass(frozen=True)
class StreamStats:
    stream_id: str
    word_count: int
    unique_words: int
    avg_word_length: float


class StreamNotFoundError(KeyError):
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


class StreamClosedError(RuntimeError):
    code = "STREAM_CLOSED"
    http_status = 409

    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.hint = "Create a new stream if you want to continue writing."
        super().__init__(f"Stream '{stream_id}' is closed.")

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
class _ProcessorState:
    buffer: str = ""
    word_count: int = 0
    total_word_length: int = 0
    unique_words: Set[str] = field(default_factory=set)
    closed: bool = False
    lock: RLock = field(default_factory=RLock)


class TextStreamProcessor:
    """
    Boundary-aware streaming word stats processor.
    - Keeps trailing partial token in buffer.
    - Counts only completed tokens.
    - On close_stream: flush buffer into stats, then delete stream (so it cannot be used again).
    """

    _WORD_RE = re.compile(r"[A-Za-z0-9']+")

    def __init__(self) -> None:
        self._streams: Dict[str, _ProcessorState] = {}
        self._registry_lock = RLock()

    def create_stream(self, stream_id: Optional[str] = None) -> str:
        sid = stream_id or str(uuid.uuid4())
        with self._registry_lock:
            self._streams[sid] = _ProcessorState()
        return sid

    def delete_stream(self, stream_id: str) -> None:
        with self._registry_lock:
            self._streams.pop(stream_id, None)

    def close_stream(self, stream_id: str) -> None:
        """
        Flush any pending buffered token into counts, then remove stream.
        Tests expect that after close_stream, the stream behaves as not found.
        """
        st = self._get_state(stream_id)
        with st.lock:
            # flush buffer if it contains a token
            buf = st.buffer or ""
            if buf:
                # treat the remaining buffer as a completed word on close
                st.word_count += 1
                st.total_word_length += len(buf)
                st.unique_words.add(buf.lower())
                st.buffer = ""
            st.closed = True

        # after flushing, delete so future calls raise StreamNotFoundError
        self.delete_stream(stream_id)

    def add_chunk(self, stream_id: str, chunk: str) -> None:
        if chunk is None:
            chunk = ""

        st = self._get_state(stream_id)
        with st.lock:
            if st.closed:
                raise StreamClosedError(stream_id)

            text = st.buffer + chunk

            tokens = self._WORD_RE.findall(text)
            if not tokens:
                # No word tokens; if ends with word char, keep buffer, else clear.
                st.buffer = text if (text and text[-1].isalnum()) else ""
                return

            ends_with_word_char = bool(text) and bool(re.match(r"[A-Za-z0-9']$", text[-1]))
            if ends_with_word_char:
                completed = tokens[:-1]
                st.buffer = tokens[-1]
            else:
                completed = tokens
                st.buffer = ""

            for w in completed:
                st.word_count += 1
                st.total_word_length += len(w)
                st.unique_words.add(w.lower())

    def get_stats(self, stream_id: str) -> StreamStats:
        st = self._get_state(stream_id)
        with st.lock:
            wc = int(st.word_count)
            uw = int(len(st.unique_words))
            avg = (float(st.total_word_length) / wc) if wc > 0 else 0.0
            return StreamStats(stream_id=stream_id, word_count=wc, unique_words=uw, avg_word_length=avg)

    # -------------------------
    # Persistence helpers
    # -------------------------

    def export_state(self, stream_id: str) -> Dict[str, object]:
        st = self._get_state(stream_id)
        with st.lock:
            return {
                "buffer": st.buffer,
                "word_count": st.word_count,
                "total_word_length": st.total_word_length,
                "unique_words": sorted(list(st.unique_words)),
                "closed": st.closed,
            }

    def import_state(self, stream_id: str, state: Dict[str, object]) -> None:
        with self._registry_lock:
            if stream_id not in self._streams:
                self._streams[stream_id] = _ProcessorState()

        st = self._streams[stream_id]
        with st.lock:
            st.buffer = str(state.get("buffer", "") or "")
            st.word_count = int(state.get("word_count", 0) or 0)
            st.total_word_length = int(state.get("total_word_length", 0) or 0)

            uw = state.get("unique_words") or []
            if isinstance(uw, list):
                st.unique_words = set(str(x).lower() for x in uw)
            else:
                st.unique_words = set()

            st.closed = bool(state.get("closed", False))

    # -------------------------
    # internals
    # -------------------------

    def _get_state(self, stream_id: str) -> _ProcessorState:
        with self._registry_lock:
            st = self._streams.get(stream_id)
        if st is None:
            raise StreamNotFoundError(stream_id)
        return st
