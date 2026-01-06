from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from filter import ContentFlag, ContentFilter
from processor import StreamNotFoundError as ProcStreamNotFoundError
from processor import StreamStats, TextStreamProcessor
from rate_limiter import StreamNotFoundError as RLStreamNotFoundError
from rate_limiter import TokenBucketRateLimiter

# Distributed mode (Option D)
try:
    from advance_features.distributed import DistributedManager, RedisUnavailable
except Exception:  # pragma: no cover
    DistributedManager = None  # type: ignore
    RedisUnavailable = Exception  # type: ignore


@dataclass(frozen=True)
class PipelineResult:
    ok: bool
    stream_id: str
    rate_limited: bool
    retry_after_seconds: float
    stats: Optional[StreamStats]
    new_flags: List[ContentFlag]
    message: str


class TextProcessingPipeline:
    """
    Orchestrates:
      - processor (word stats)
      - content filter
      - rate limiter

    Local mode (default):
      - Uses in-memory processor/filter/rate limiter.
      - If store is provided, persists stream state and supports lazy restore.

    Distributed mode (Option D):
      - Enabled when DISTRIBUTED_MODE=1 and REDIS_URL is set.
      - Uses Redis shared state + distributed rate limiting.
    """

    _WORD_RE = re.compile(r"[A-Za-z0-9']+")
    _WORDCHAR_RE = re.compile(r"[A-Za-z0-9']")

    def __init__(
        self,
        processor: Optional[TextStreamProcessor] = None,
        content_filter: Optional[ContentFilter] = None,
        rate_limiter: Optional[TokenBucketRateLimiter] = None,
        store: Optional[Any] = None,
    ) -> None:
        self.processor = processor or TextStreamProcessor()
        self.content_filter = content_filter or ContentFilter()
        self.rate_limiter = rate_limiter or TokenBucketRateLimiter()

        self._store = store
        self._stream_ids: Dict[str, Tuple[str, str]] = {}  # pipeline_stream_id -> (filter_id, rate_id)

        # Distributed toggle is per instance, read at runtime (NOT import time)
        self._distributed = bool(os.environ.get("DISTRIBUTED_MODE") == "1" and os.environ.get("REDIS_URL"))
        self._dist: Optional[DistributedManager] = None
        if self._distributed and DistributedManager is not None:
            self._dist = DistributedManager()

    # -------------------------
    # Stream lifecycle
    # -------------------------

    def create_stream(self) -> str:
        # Distributed mode
        if self._distributed:
            if self._dist is None:
                raise RuntimeError("Distributed mode enabled but DistributedManager not available.")
            return self._dist.create_stream()

        # Local mode
        stream_id = self.processor.create_stream()
        fid = self.content_filter.create_stream()
        rid = self.rate_limiter.create_stream()
        self._stream_ids[stream_id] = (fid, rid)

        self._persist_stream(stream_id)
        return stream_id

    def delete_stream(self, stream_id: str) -> None:
        # Distributed mode
        if self._distributed:
            if self._dist is None:
                return
            try:
                self._dist.delete_stream(stream_id)
            finally:
                self._store_delete(stream_id)
            return

        # Local mode
        ids = self._stream_ids.pop(stream_id, None)

        try:
            self.processor.delete_stream(stream_id)
        except Exception:
            pass

        if ids is not None:
            fid, rid = ids
            try:
                self.content_filter.close_stream(fid)
            except Exception:
                pass
            try:
                self.rate_limiter.delete_stream(rid)
            except Exception:
                pass

        self._store_delete(stream_id)

    # -------------------------
    # Main processing
    # -------------------------

    def add_chunk(self, stream_id: str, text: str, priority: str = "normal") -> PipelineResult:
        # Distributed mode
        if self._distributed:
            return self._add_chunk_distributed(stream_id, text, priority)

        # Local mode
        if text is None:
            text = ""

        if stream_id not in self._stream_ids:
            if not self._try_restore(stream_id):
                return PipelineResult(False, stream_id, False, 0.0, None, [], "Stream not found.")

        fid, rid = self._stream_ids[stream_id]

        words_estimate = self._estimate_words(text)
        try:
            rl = self.rate_limiter.check_and_consume(rid, words=words_estimate, priority=priority)
        except RLStreamNotFoundError:
            return PipelineResult(False, stream_id, False, 0.0, None, [], "Rate limiter stream missing.")

        if not rl.allowed:
            return PipelineResult(
                ok=False,
                stream_id=stream_id,
                rate_limited=True,
                retry_after_seconds=rl.retry_after_seconds,
                stats=self.safe_get_stats(stream_id),
                new_flags=[],
                message="Rate limit exceeded.",
            )

        try:
            self.processor.add_chunk(stream_id, text)
        except ProcStreamNotFoundError:
            return PipelineResult(False, stream_id, False, 0.0, None, [], "Stream not found.")

        try:
            new_flags = self.content_filter.add_chunk(fid, text)
        except Exception:
            new_flags = []

        self._persist_stream(stream_id)

        return PipelineResult(
            ok=True,
            stream_id=stream_id,
            rate_limited=False,
            retry_after_seconds=0.0,
            stats=self.safe_get_stats(stream_id),
            new_flags=new_flags,
            message="Chunk processed.",
        )

    def get_stats(self, stream_id: str) -> Tuple[StreamStats, List[ContentFlag]]:
        # Distributed mode
        if self._distributed:
            if self._dist is None:
                raise KeyError(stream_id)
            stats = self._get_stats_distributed(stream_id)
            flags_dicts = self._dist.filter_get_flags(stream_id)
            flags = [ContentFlag(**d) for d in flags_dicts] if flags_dicts else []
            return stats, flags

        # Local mode
        if stream_id not in self._stream_ids:
            if not self._try_restore(stream_id):
                raise KeyError(stream_id)

        fid, _rid = self._stream_ids[stream_id]
        stats = self.processor.get_stats(stream_id)
        flags = self.content_filter.get_flags(fid)
        return stats, flags

    def safe_get_stats(self, stream_id: str) -> Optional[StreamStats]:
        try:
            return self.processor.get_stats(stream_id)
        except Exception:
            return None

    # -------------------------
    # Distributed mode internals
    # -------------------------

    def _add_chunk_distributed(self, stream_id: str, text: str, priority: str) -> PipelineResult:
        if self._dist is None:
            return PipelineResult(False, stream_id, False, 0.0, None, [], "Redis unavailable.")

        if text is None:
            text = ""

        try:
            if not self._dist.stream_exists(stream_id):
                return PipelineResult(False, stream_id, False, 0.0, None, [], "Stream not found.")
        except RedisUnavailable:
            return PipelineResult(False, stream_id, False, 0.0, None, [], "Redis unavailable.")

        # distributed rate limiting
        words_est = self._estimate_words(text)
        try:
            allowed, retry_after = self._dist.rate_limit(stream_id, words_est, priority)
        except RedisUnavailable:
            return PipelineResult(False, stream_id, False, 0.0, None, [], "Redis unavailable.")

        if not allowed:
            stats = self._get_stats_distributed(stream_id)
            return PipelineResult(False, stream_id, True, retry_after, stats, [], "Rate limit exceeded.")

        # processor update
        try:
            pstate = self._dist.processor_get_state(stream_id)
        except Exception:
            return PipelineResult(False, stream_id, False, 0.0, None, [], "Stream not found.")

        carry = str(pstate.get("buffer") or "")
        completed, trailing = self._tokenize_with_boundary(carry, text)

        wc_inc = len(completed)
        len_inc = sum(len(w) for w in completed)

        pstate["buffer"] = trailing
        pstate["word_count"] = int(pstate.get("word_count", 0)) + wc_inc
        pstate["total_word_length"] = int(pstate.get("total_word_length", 0)) + len_inc

        try:
            self._dist.processor_set_state(stream_id, pstate)
            if completed:
                self._dist.processor_add_unique_words(stream_id, [w.lower() for w in completed])
        except RedisUnavailable:
            return PipelineResult(False, stream_id, False, 0.0, None, [], "Redis unavailable.")

        # filter update (best effort; keeps flags as list and shared tail)
        new_flags: List[ContentFlag] = []
        try:
            fstate = self._dist.filter_get_state(stream_id)
        except Exception:
            fstate = {"tail": ""}

        scan_text = str(fstate.get("tail") or "") + text
        new_flags = self._scan_distributed_flags(stream_id, scan_text)

        lookback = int(getattr(getattr(self.content_filter, "_config", None), "lookback_chars", 0) or 0)
        fstate["tail"] = "" if lookback <= 0 else scan_text[-lookback:]

        try:
            self._dist.filter_set_state(stream_id, fstate)
            if new_flags:
                self._dist.filter_add_flags(
                    stream_id,
                    [
                        {
                            "rule_id": f.rule_id,
                            "match": f.match,
                            "confidence": f.confidence,
                            "rule_type": f.rule_type,
                        }
                        for f in new_flags
                    ],
                )
        except RedisUnavailable:
            # If filter write fails, still return processor stats
            pass

        stats = self._get_stats_distributed(stream_id)
        return PipelineResult(True, stream_id, False, 0.0, stats, new_flags, "Chunk processed.")

    def _get_stats_distributed(self, stream_id: str) -> StreamStats:
        if self._dist is None:
            raise KeyError(stream_id)
        pstate = self._dist.processor_get_state(stream_id)
        wc = int(pstate.get("word_count", 0))
        total_len = int(pstate.get("total_word_length", 0))
        uniq = int(self._dist.processor_get_unique_count(stream_id))
        avg = (total_len / wc) if wc else 0.0
        return StreamStats(stream_id=stream_id, word_count=wc, unique_words=uniq, avg_word_length=avg)

    def _tokenize_with_boundary(self, carry: str, text: str) -> Tuple[List[str], str]:
        """
        Returns (completed_words, trailing_buffer_word)

        trailing buffer keeps an incomplete last word when the combined text ends on a word char.
        """
        combined = (carry or "") + (text or "")
        if not combined:
            return [], ""

        matches = list(self._WORD_RE.finditer(combined))
        if not matches:
            # If it's all delimiters, buffer becomes empty
            return [], ""

        last = matches[-1]
        ends_at_end = last.end() == len(combined)
        ends_on_word_char = bool(self._WORDCHAR_RE.match(combined[-1]))

        if ends_at_end and ends_on_word_char:
            completed = [m.group(0) for m in matches[:-1]]
            trailing = last.group(0)
            return completed, trailing

        completed = [m.group(0) for m in matches]
        return completed, ""

    def _scan_distributed_flags(self, stream_id: str, text: str) -> List[ContentFlag]:
        """
        Best-effort shared flag scanning using ContentFilter internal config if present.
        If anything is missing, returns [] (does not break distributed mode).
        """
        if self._dist is None:
            return []

        cfg = getattr(self.content_filter, "_config", None)
        word_re = getattr(self.content_filter, "_WORD_RE", self._WORD_RE)
        word_list = getattr(self.content_filter, "_word_list", {}) or {}

        case_insensitive = bool(getattr(cfg, "case_insensitive", True)) if cfg is not None else True
        regex_rules = getattr(cfg, "regex_rules", []) if cfg is not None else []

        found: List[ContentFlag] = []

        # wordlist rules
        try:
            tokens = word_re.findall(text)
            for token in tokens:
                key = token.lower() if case_insensitive else token
                confidence = word_list.get(key)
                if confidence is None:
                    continue

                rule_id = f"word:{key}"
                norm = key
                try:
                    if self._dist.filter_seen(stream_id, rule_id, norm):
                        continue
                except Exception:
                    pass

                found.append(ContentFlag(rule_id=rule_id, match=token, confidence=float(confidence), rule_type="word"))
        except Exception:
            pass

        # regex rules (expect (rule_id, compiled_pattern, confidence))
        try:
            for rule_id_raw, pattern, confidence in regex_rules:
                for m in pattern.finditer(text):
                    match_text = m.group(0)
                    norm = match_text.lower() if case_insensitive else match_text
                    rule_id = f"regex:{rule_id_raw}"
                    try:
                        if self._dist.filter_seen(stream_id, rule_id, norm):
                            continue
                    except Exception:
                        pass

                    found.append(
                        ContentFlag(
                            rule_id=rule_id,
                            match=match_text,
                            confidence=float(confidence),
                            rule_type="regex",
                        )
                    )
        except Exception:
            pass

        return found

    # -------------------------
    # Persistence
    # -------------------------

    def _persist_stream(self, stream_id: str) -> None:
        if self._store is None:
            return

        try:
            processor_state = self.processor.export_state(stream_id)
        except Exception:
            processor_state = {}

        filter_state: Dict[str, Any] = {"tail": "", "flags": [], "seen": [], "closed": False}
        try:
            fid, _rid = self._stream_ids.get(stream_id, ("", ""))
            st = self.content_filter._streams.get(fid)  # type: ignore[attr-defined]
            if st is not None:
                filter_state = {
                    "tail": getattr(st, "tail", ""),
                    "flags": [
                        {"rule_id": f.rule_id, "match": f.match, "confidence": f.confidence, "rule_type": f.rule_type}
                        for f in list(getattr(st, "flags", []))
                    ],
                    "seen": list(getattr(st, "seen", set())),
                    "closed": bool(getattr(st, "closed", False)),
                }
        except Exception:
            pass

        meta = {"component_ids": {"filter": self._stream_ids[stream_id][0], "rate": self._stream_ids[stream_id][1]}}

        save_fn = getattr(self._store, "save_stream", None)
        if callable(save_fn):
            try:
                save_fn(stream_id, processor_state=processor_state, filter_state=filter_state, meta=meta)
            except TypeError:
                save_fn(stream_id, {"processor_state": processor_state, "filter_state": filter_state, "meta": meta})
            return

        for name in ("upsert_stream", "save", "put", "upsert"):
            fn = getattr(self._store, name, None)
            if callable(fn):
                try:
                    fn(stream_id, {"processor_state": processor_state, "filter_state": filter_state, "meta": meta})
                except TypeError:
                    fn(
                        {
                            "stream_id": stream_id,
                            "processor_state": processor_state,
                            "filter_state": filter_state,
                            "meta": meta,
                        }
                    )
                return

    def _try_restore(self, stream_id: str) -> bool:
        if stream_id in self._stream_ids:
            return True
        if self._store is None:
            return False

        load_fn = getattr(self._store, "load_stream", None)
        if not callable(load_fn):
            for name in ("get_stream", "read_stream", "load", "get"):
                load_fn = getattr(self._store, name, None)
                if callable(load_fn):
                    break
        if not callable(load_fn):
            return False

        try:
            row = load_fn(stream_id)
        except TypeError:
            row = load_fn(stream_id=stream_id)

        if not row:
            return False

        processor_state = row.get("processor_state") or {}
        filter_state = row.get("filter_state") or {}

        try:
            self.processor.get_stats(stream_id)
        except Exception:
            # create same stream id if supported; fallback to plain create_stream
            try:
                self.processor.create_stream(stream_id=stream_id)
            except TypeError:
                sid2 = self.processor.create_stream()
                if sid2 != stream_id:
                    # if processor cannot create with id, we cannot safely restore
                    return False

        try:
            self.processor.import_state(stream_id, processor_state)
        except Exception:
            pass

        fid = self.content_filter.create_stream()
        rid = self.rate_limiter.create_stream()
        self._stream_ids[stream_id] = (fid, rid)

        try:
            st = self.content_filter._streams.get(fid)  # type: ignore[attr-defined]
            if st is not None:
                st.tail = str(filter_state.get("tail", "") or "")
                st.closed = bool(filter_state.get("closed", False))
                st.seen = set(tuple(x) for x in (filter_state.get("seen") or []))

                flags_in = filter_state.get("flags") or []
                rebuilt: List[ContentFlag] = []
                for f in flags_in:
                    try:
                        rebuilt.append(
                            ContentFlag(
                                rule_id=str(f.get("rule_id", "")),
                                match=str(f.get("match", "")),
                                confidence=float(f.get("confidence", 0.0)),
                                rule_type=str(f.get("rule_type", "regex")),
                            )
                        )
                    except Exception:
                        continue
                st.flags = rebuilt
        except Exception:
            pass

        return True

    def _store_delete(self, stream_id: str) -> None:
        if self._store is None:
            return
        for name in ("delete_stream", "delete", "remove"):
            fn = getattr(self._store, name, None)
            if callable(fn):
                try:
                    fn(stream_id)
                except TypeError:
                    fn(stream_id=stream_id)
                return

    # -------------------------
    # Helpers
    # -------------------------

    def _estimate_words(self, text: str) -> int:
        return len(self._WORD_RE.findall(text or ""))
