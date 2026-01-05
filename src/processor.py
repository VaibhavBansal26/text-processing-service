from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from threading import RLock
from typing import Dict, List, Tuple


_WORD_RE = re.compile(r"[A-Za-z0-9']+")


@dataclass(frozen=True)
class StreamStats:
    """
    Immutable statistics snapshot returned by get_stats().
    """
    stream_id: str
    word_count: int
    unique_words: int
    avg_word_length: float


# class StreamNotFoundError(KeyError):
#     """Raised when a stream_id does not exist."""


# class StreamClosedError(RuntimeError):
#     """Raised when attempting to add a chunk to a closed stream."""

class StreamNotFoundError(KeyError):
    """Raised when a stream_id does not exist."""

    code = "STREAM_NOT_FOUND"
    http_status = 404

    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.hint = "Create a new stream (POST /streams) and retry the request."
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
    """Raised when attempting to add a chunk to a closed stream."""

    code = "STREAM_CLOSED"
    http_status = 409

    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        self.hint = "This stream is closed. Create a new stream and continue there."
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
class _StreamState:
    """
    Internal mutable state for a single stream.
    """
    # Buffer holds a trailing partial word across chunk boundaries.
    # Example: chunk1="wor", chunk2="ld " -> buffer carries "wor" into next chunk.
    buffer: str = ""

    word_count: int = 0
    total_word_length: int = 0
    unique_words: set[str] = field(default_factory=set)

    closed: bool = False

    # Per stream lock: multiple streams can be processed concurrently.
    lock: RLock = field(default_factory=RLock)


class TextStreamProcessor:
    """
    Accepts text in chunks per stream and maintains streaming word statistics.

    Responsibilities in Part 1.1:
    - create_stream(): create unique stream IDs
    - add_chunk(): accept streaming chunks and tokenize words with boundary correctness
    - get_stats(): return word_count, unique_words, avg_word_length
    - close_stream(): finalize and clean up stream
    """

    def __init__(self) -> None:
        self._streams: Dict[str, _StreamState] = {}
        self._registry_lock = RLock()  # protects _streams dict operations

    def create_stream(self) -> str:
        """
        Create a new stream and return its unique ID.
        """
        stream_id = str(uuid.uuid4())
        with self._registry_lock:
            self._streams[stream_id] = _StreamState()
        return stream_id

    def add_chunk(self, stream_id: str, chunk: str) -> None:
        """
        Add a chunk of text to the stream.

        Chunk boundary correctness:
        - We prepend any previous trailing partial token (buffer) to this chunk.
        - If the chunk ends in the middle of a word, we keep that trailing word in buffer.
        - We only commit words that we know are complete.
        """
        if chunk is None:
            chunk = ""

        state = self._get_state(stream_id)

        with state.lock:
            if state.closed:
                raise StreamClosedError(f"Stream {stream_id} is closed")

            completed_words, trailing_buffer = self._tokenize_with_boundary(state.buffer, chunk)

            # Save trailing partial word for next chunk.
            state.buffer = trailing_buffer

            # Commit completed words into stats.
            for w in completed_words:
                self._commit_word(state, w)

    def get_stats(self, stream_id: str) -> StreamStats:
        """
        Return a consistent snapshot of current statistics for the stream.
        """
        state = self._get_state(stream_id)

        with state.lock:
            avg = (state.total_word_length / state.word_count) if state.word_count else 0.0
            return StreamStats(
                stream_id=stream_id,
                word_count=state.word_count,
                unique_words=len(state.unique_words),
                avg_word_length=avg,
            )

    def close_stream(self, stream_id: str) -> None:
        """
        Close the stream and clean it up.

        Important: if there's a leftover buffer (unfinished word), end of stream means
        we commit it as the final word.
        """
        state = self._get_state(stream_id)

        with state.lock:
            if state.closed:
                return

            # Flush leftover partial word at end of stream.
            if state.buffer:
                self._commit_word(state, state.buffer)
                state.buffer = ""

            state.closed = True

        with self._registry_lock:
            self._streams.pop(stream_id, None)

    # Alias that might match your API naming later
    def delete_stream(self, stream_id: str) -> None:
        self.close_stream(stream_id)

    # ---------------- internal helpers ----------------

    def _get_state(self, stream_id: str) -> _StreamState:
        """
        Thread safe lookup of stream state.
        """
        with self._registry_lock:
            state = self._streams.get(stream_id)
        if state is None:
            raise StreamNotFoundError(stream_id)
        return state

    def _commit_word(self, state: _StreamState, word: str) -> None:
        """
        Update stream statistics for a finalized word.
        """
        state.word_count += 1
        state.total_word_length += len(word)
        state.unique_words.add(word.lower())

    def _tokenize_with_boundary(self, carry: str, chunk: str) -> Tuple[List[str], str]:
        """
        Tokenize (carry + chunk) into words, but keep trailing partial word in buffer.

        Returns:
        - completed_words: words safe to count now
        - trailing_buffer: last token if it may be incomplete
        """
        text = carry + chunk
        matches = list(_WORD_RE.finditer(text))

        # No matches means no complete word.
        # If last char is wordish, we might be mid word: keep as buffer.
        if not matches:
            if text and (text[-1].isalnum() or text[-1] == "'"):
                return [], text
            return [], ""

        last = matches[-1]
        completed: List[str] = []
        trailing = ""

        # If the last match reaches the end of `text` AND the chunk ends with a word character,
        # we treat it as incomplete and keep it in the buffer.
        ends_at_end = last.end() == len(text)
        chunk_ends_wordish = bool(chunk) and (chunk[-1].isalnum() or chunk[-1] == "'")
        last_is_partial = ends_at_end and chunk_ends_wordish

        for m in matches:
            token = m.group(0)
            if last_is_partial and m is last:
                trailing = token
            else:
                completed.append(token)

        return completed, trailing
