from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


@dataclass
class SQLitePersistenceStore:
    """
    SQLite persistence for stream state.

    Supports:
      - save_stream(stream_id, data_dict) where dict includes processor_state/filter_state OR processor/filter
      - save_stream(stream_id, processor_state=..., filter_state=..., meta=...)
    """

    db_path: str

    def __post_init__(self) -> None:
        resolved = os.path.expanduser(os.path.expandvars(self.db_path))
        p = Path(resolved)
        if p.parent and not p.parent.exists():
            p.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = str(p)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS streams (
                    stream_id TEXT PRIMARY KEY,
                    processor_state_json TEXT NOT NULL,
                    filter_state_json TEXT NOT NULL,
                    meta_json TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def health_check(self) -> bool:
        try:
            with self._connect() as conn:
                conn.execute("SELECT 1")
            return True
        except Exception:
            return False

    def _split_states(
        self,
        data: Optional[Dict[str, Any]],
        processor_state: Optional[Dict[str, Any]],
        filter_state: Optional[Dict[str, Any]],
        meta: Optional[Dict[str, Any]],
    ) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        if data is None:
            data = {}

        if processor_state is None:
            processor_state = (
                data.get("processor_state")
                or data.get("processor")
                or data.get("text_processor_state")
                or {}
            )

        if filter_state is None:
            filter_state = (
                data.get("filter_state")
                or data.get("filter")
                or data.get("content_filter_state")
                or {}
            )

        if meta is None:
            meta = data.get("meta") or data.get("component_ids") or {}

        if not isinstance(processor_state, dict):
            processor_state = {}
        if not isinstance(filter_state, dict):
            filter_state = {}
        if not isinstance(meta, dict):
            meta = {}

        return processor_state, filter_state, meta

    def save_stream(
        self,
        stream_id: str,
        data: Optional[Dict[str, Any]] = None,
        *,
        processor_state: Optional[Dict[str, Any]] = None,
        filter_state: Optional[Dict[str, Any]] = None,
        meta: Optional[Dict[str, Any]] = None,
        **_kwargs: Any,
    ) -> None:
        ps, fs, m = self._split_states(data, processor_state, filter_state, meta)

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO streams(stream_id, processor_state_json, filter_state_json, meta_json)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(stream_id) DO UPDATE SET
                    processor_state_json=excluded.processor_state_json,
                    filter_state_json=excluded.filter_state_json,
                    meta_json=excluded.meta_json
                """,
                (
                    stream_id,
                    json.dumps(ps),
                    json.dumps(fs),
                    json.dumps(m),
                ),
            )
            conn.commit()

    def load_stream(self, stream_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT stream_id, processor_state_json, filter_state_json, meta_json FROM streams WHERE stream_id=?",
                (stream_id,),
            ).fetchone()

        if row is None:
            return None

        try:
            ps = json.loads(row["processor_state_json"])
        except Exception:
            ps = {}

        try:
            fs = json.loads(row["filter_state_json"])
        except Exception:
            fs = {}

        try:
            meta = json.loads(row["meta_json"])
        except Exception:
            meta = {}

        return {"stream_id": row["stream_id"], "processor_state": ps, "filter_state": fs, "meta": meta}

    def delete_stream(self, stream_id: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM streams WHERE stream_id=?", (stream_id,))
            conn.commit()
