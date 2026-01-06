from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Dict, Optional

from pipeline import PipelineResult, TextProcessingPipeline
from processor import StreamNotFoundError, StreamClosedError


@dataclass(frozen=True)
class AsyncPipelineConfig:
    """
    Controls async behavior.

    per_stream_queue_maxsize:
        Backpressure limit for each stream queue.
        If queue is full, chunk ingestion will return 429.
    shutdown_timeout_seconds:
        How long we wait for workers to stop during app shutdown.
    """
    per_stream_queue_maxsize: int = 200
    shutdown_timeout_seconds: float = 5.0


class BackpressureError(RuntimeError):
    """
    Raised when a per-stream queue is full (client is sending faster than we can process).
    """
    code = "BACKPRESSURE"
    http_status = 429

    def __init__(self, stream_id: str, retry_after_seconds: float = 1.0) -> None:
        self.stream_id = stream_id
        self.retry_after_seconds = float(retry_after_seconds)
        super().__init__(f"Stream '{stream_id}' is busy. Retry later.")

    def to_dict(self) -> dict:
        return {
            "error": {
                "code": self.code,
                "message": str(self),
                "stream_id": self.stream_id,
                "retry_after_seconds": self.retry_after_seconds,
                "hint": "Slow down chunk ingestion or increase queue size.",
            }
        }


@dataclass(frozen=True)
class _ChunkJob:
    text: str
    priority: str
    enqueued_at: float


class _StreamWorker:
    """
    Owns:
    - queue for chunks
    - a background task that processes chunks in order
    - last_result for quick stats responses
    """
    def __init__(self, stream_id: str, queue_maxsize: int) -> None:
        self.stream_id = stream_id
        self.queue: asyncio.Queue[_ChunkJob] = asyncio.Queue(maxsize=queue_maxsize)
        self.task: Optional[asyncio.Task] = None
        self.closed: bool = False
        self.last_result: Optional[PipelineResult] = None

        # Used for fast /stats responses without locking deep internals
        self._lock = asyncio.Lock()

    async def set_last_result(self, result: PipelineResult) -> None:
        async with self._lock:
            self.last_result = result

    async def get_last_result(self) -> Optional[PipelineResult]:
        async with self._lock:
            return self.last_result


class AsyncTextProcessingPipeline:
    """
    Async wrapper around your existing (sync) TextProcessingPipeline.

    Core idea:
    - API endpoints enqueue chunks quickly
    - background workers do the real processing sequentially per stream
    - processing uses asyncio.to_thread(...) so the event loop remains responsive
    """

    def __init__(self, base: Optional[TextProcessingPipeline] = None, cfg: Optional[AsyncPipelineConfig] = None) -> None:
        self._base = base or TextProcessingPipeline()
        self._cfg = cfg or AsyncPipelineConfig()
        self._workers: Dict[str, _StreamWorker] = {}
        self._registry_lock = asyncio.Lock()
        self._shutdown = asyncio.Event()

    async def create_stream(self) -> str:
        stream_id = self._base.create_stream()
        worker = _StreamWorker(stream_id=stream_id, queue_maxsize=self._cfg.per_stream_queue_maxsize)

        async with self._registry_lock:
            self._workers[stream_id] = worker
            worker.task = asyncio.create_task(self._run_worker(worker), name=f"stream-worker:{stream_id}")

        return stream_id

    async def delete_stream(self, stream_id: str) -> None:
        worker = await self._get_worker(stream_id)
        worker.closed = True

        # Drain quickly: we do not want to keep processing old queued chunks
        # after user asked to delete.
        try:
            while True:
                worker.queue.get_nowait()
                worker.queue.task_done()
        except asyncio.QueueEmpty:
            pass

        # Close underlying stream state
        # (runs in thread to avoid blocking event loop)
        await asyncio.to_thread(self._base.delete_stream, stream_id)

        async with self._registry_lock:
            self._workers.pop(stream_id, None)

        # Cancel worker task if still alive
        if worker.task and not worker.task.done():
            worker.task.cancel()

    async def add_chunk(self, stream_id: str, text: str, priority: str = "normal") -> dict:
        """
        Enqueue a chunk for async processing.

        Returns a quick status:
        - accepted: queued
        - backpressure: queue full
        """
        if text is None:
            text = ""

        worker = await self._get_worker(stream_id)
        if worker.closed:
            raise StreamClosedError(stream_id)

        job = _ChunkJob(text=text, priority=priority, enqueued_at=time.time())

        try:
            worker.queue.put_nowait(job)
        except asyncio.QueueFull:
            raise BackpressureError(stream_id, retry_after_seconds=1.0)

        return {"status": "accepted", "queued": True}

    async def get_stats(self, stream_id: str) -> dict:
        """
        Stats come from the base pipeline (source of truth), plus flags.
        Also includes queue depth so you can see backpressure.
        """
        worker = await self._get_worker(stream_id)

        stats, flags = await asyncio.to_thread(self._base.get_stats, stream_id)
        return {
            "stream_id": stream_id,
            "word_count": stats.word_count,
            "unique_words": stats.unique_words,
            "avg_word_length": stats.avg_word_length,
            "flags": [
                {"rule_id": f.rule_id, "match": f.match, "confidence": f.confidence, "rule_type": f.rule_type}
                for f in flags
            ],
            "queue_depth": worker.queue.qsize(),
        }

    async def shutdown(self) -> None:
        """
        Graceful shutdown:
        - signal shutdown
        - stop workers
        - best effort close streams
        """
        self._shutdown.set()

        async with self._registry_lock:
            workers = list(self._workers.values())

        # ask workers to stop
        for w in workers:
            w.closed = True

        # wait for tasks briefly
        deadline = time.time() + self._cfg.shutdown_timeout_seconds
        for w in workers:
            if w.task is None:
                continue
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            try:
                await asyncio.wait_for(w.task, timeout=remaining)
            except Exception:
                # cancel if still running
                if not w.task.done():
                    w.task.cancel()

        # close all streams in base pipeline best effort
        for w in workers:
            try:
                await asyncio.to_thread(self._base.delete_stream, w.stream_id)
            except Exception:
                pass

        async with self._registry_lock:
            self._workers.clear()

    async def _get_worker(self, stream_id: str) -> _StreamWorker:
        async with self._registry_lock:
            worker = self._workers.get(stream_id)
        if worker is None:
            raise StreamNotFoundError(stream_id)
        return worker

    async def _run_worker(self, worker: _StreamWorker) -> None:
        """
        Worker loop: sequentially processes chunks in order for one stream.
        """
        while not self._shutdown.is_set() and not worker.closed:
            try:
                job = await asyncio.wait_for(worker.queue.get(), timeout=0.2)
            except asyncio.TimeoutError:
                continue

            try:
                # run sync pipeline in a thread
                result: PipelineResult = await asyncio.to_thread(
                    self._base.add_chunk,
                    worker.stream_id,
                    job.text,
                    job.priority,
                )
                await worker.set_last_result(result)
            except Exception:
                # Swallow worker errors so one bad chunk doesn't kill the worker.
                # In production, you'd log this with correlation id.
                pass
            finally:
                worker.queue.task_done()
