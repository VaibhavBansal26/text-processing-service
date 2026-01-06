from __future__ import annotations

import json
import logging
import os
import time
import uuid
from contextvars import ContextVar
from dataclasses import dataclass, field
from threading import RLock
from typing import Dict, Optional


# -----------------------------
# Correlation and stream context
# -----------------------------

_correlation_id: ContextVar[str] = ContextVar("correlation_id", default="")
_stream_id: ContextVar[str] = ContextVar("stream_id", default="")


def new_correlation_id() -> str:
    return str(uuid.uuid4())


def set_correlation_id(value: str) -> None:
    _correlation_id.set(value)


def get_correlation_id() -> str:
    return _correlation_id.get() or ""


def set_stream_id(value: str) -> None:
    _stream_id.set(value)


def get_stream_id() -> str:
    return _stream_id.get() or ""


# -----------------------------
# Structured JSON logging
# -----------------------------

class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": get_correlation_id(),
            "stream_id": get_stream_id(),
        }

        extra = getattr(record, "extra", None)
        if isinstance(extra, dict):
            payload.update(extra)

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(payload)


def configure_logging() -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    root = logging.getLogger()
    root.setLevel(level)

    if root.handlers:
        return

    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(JsonLogFormatter())
    root.addHandler(handler)


# -----------------------------
# Minimal metrics (Prometheus text)
# -----------------------------

@dataclass
class _MetricState:
    lock: RLock = field(default_factory=RLock)
    counters: Dict[str, int] = field(default_factory=dict)
    latency_sum_ms: Dict[str, float] = field(default_factory=dict)
    latency_count: Dict[str, int] = field(default_factory=dict)


_metrics = _MetricState()


def inc_counter(name: str, amount: int = 1) -> None:
    with _metrics.lock:
        _metrics.counters[name] = _metrics.counters.get(name, 0) + amount


def observe_latency_ms(route_key: str, elapsed_ms: float) -> None:
    with _metrics.lock:
        _metrics.latency_sum_ms[route_key] = _metrics.latency_sum_ms.get(route_key, 0.0) + elapsed_ms
        _metrics.latency_count[route_key] = _metrics.latency_count.get(route_key, 0) + 1


def render_metrics() -> str:
    """
    Very small Prometheus compatible output.

    Exposes:
    - app_counter{name="..."} <int>
    - http_latency_ms_sum{route="METHOD PATH"} <float>
    - http_latency_ms_count{route="METHOD PATH"} <int>
    """
    lines = []

    with _metrics.lock:
        lines.append("# TYPE app_counter counter")
        for name, value in sorted(_metrics.counters.items()):
            lines.append(f'app_counter{{name="{name}"}} {value}')

        lines.append("# TYPE http_latency_ms_sum counter")
        for route, value in sorted(_metrics.latency_sum_ms.items()):
            lines.append(f'http_latency_ms_sum{{route="{route}"}} {value}')

        lines.append("# TYPE http_latency_ms_count counter")
        for route, value in sorted(_metrics.latency_count.items()):
            lines.append(f'http_latency_ms_count{{route="{route}"}} {value}')

    return "\n".join(lines) + "\n"
