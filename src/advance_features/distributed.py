from __future__ import annotations

import json
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Callable, Union

import redis  # pip install redis


@dataclass(frozen=True)
class DistributedConfig:
    redis_url: str
    key_prefix: str = "tps"
    redis_socket_timeout_s: float = 0.2
    redis_connect_timeout_s: float = 0.2

    # Partition policy when Redis is unavailable
    # "fail_closed" -> return 503 / block
    # "fail_open" -> allow without shared guarantees
    # "degraded" -> allow with small local allowance
    partition_policy: str = "fail_closed"

    # Optional for degraded mode
    degraded_allow_requests: int = 50


class RedisUnavailable(RuntimeError):
    pass


class DistributedManager:
    """
    Redis shared state + distributed rate limiting.

    What lives in Redis per stream_id:
      processor state: JSON blob {buffer, word_count, total_word_length}
      unique words: Redis SET
      filter state: JSON blob {tail}
      flags: Redis LIST (JSON entries) + seen_flags SET for dedup
      rate limiting: token buckets stored in Redis (Lua script, per stream_id + priority)
    """

    def __init__(self, cfg: Optional[DistributedConfig] = None) -> None:
        cfg = cfg or DistributedConfig(
            redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
            partition_policy=os.environ.get("REDIS_PARTITION_POLICY", "fail_closed"),
        )
        self.cfg = cfg
        self.r = redis.Redis.from_url(
            cfg.redis_url,
            socket_timeout=cfg.redis_socket_timeout_s,
            socket_connect_timeout=cfg.redis_connect_timeout_s,
            decode_responses=True,
        )

        # tiny in-process fallback counter used only in degraded mode
        self._degraded_used = 0

        # optional: load Lua script for atomic rate limiting
        self._lua_rl_sha: Optional[str] = None
        self._load_rate_limit_script()

    # -------------------------
    # Key helpers
    # -------------------------

    def _k(self, *parts: str) -> str:
        return ":".join([self.cfg.key_prefix, *parts])

    # -------------------------
    # Stream lifecycle
    # -------------------------

    def create_stream(self) -> str:
        stream_id = str(uuid.uuid4())
        now = time.time()

        processor_state = {"buffer": "", "word_count": 0, "total_word_length": 0}
        filter_state = {"tail": ""}

        pipe = self.r.pipeline()
        pipe.set(self._k("stream", stream_id, "processor"), json.dumps(processor_state))
        pipe.set(self._k("stream", stream_id, "filter"), json.dumps(filter_state))

        # unique words and flags
        pipe.delete(self._k("stream", stream_id, "unique_words"))
        pipe.delete(self._k("stream", stream_id, "seen_flags"))
        pipe.delete(self._k("stream", stream_id, "flags"))

        # metadata
        pipe.hset(self._k("stream", stream_id, "meta"), mapping={"created_at": str(now)})

        # IMPORTANT: execute the pipeline
        self._redis_exec(pipe.execute)
        return stream_id

    def delete_stream(self, stream_id: str) -> None:
        keys = [
            self._k("stream", stream_id, "processor"),
            self._k("stream", stream_id, "filter"),
            self._k("stream", stream_id, "unique_words"),
            self._k("stream", stream_id, "seen_flags"),
            self._k("stream", stream_id, "flags"),
            self._k("stream", stream_id, "meta"),
            self._k("rl", stream_id, "normal"),
            self._k("rl", stream_id, "high"),
        ]
        self._redis_exec(lambda: self.r.delete(*keys))

    def stream_exists(self, stream_id: str) -> bool:
        return bool(self._redis_exec(lambda: self.r.exists(self._k("stream", stream_id, "processor"))))

    # -------------------------
    # Processor state in Redis
    # -------------------------

    def processor_get_state(self, stream_id: str) -> Dict[str, Any]:
        raw = self._redis_exec(lambda: self.r.get(self._k("stream", stream_id, "processor")))
        if not raw:
            raise KeyError(stream_id)
        return json.loads(raw)

    def processor_set_state(self, stream_id: str, state: Dict[str, Any]) -> None:
        self._redis_exec(lambda: self.r.set(self._k("stream", stream_id, "processor"), json.dumps(state)))

    def processor_add_unique_words(self, stream_id: str, words_lower: List[str]) -> int:
        if not words_lower:
            return int(self._redis_exec(lambda: self.r.scard(self._k("stream", stream_id, "unique_words"))))

        pipe = self.r.pipeline()
        for w in words_lower:
            pipe.sadd(self._k("stream", stream_id, "unique_words"), w)
        pipe.scard(self._k("stream", stream_id, "unique_words"))

        res = self._redis_exec(pipe.execute)
        return int(res[-1])

    def processor_get_unique_count(self, stream_id: str) -> int:
        return int(self._redis_exec(lambda: self.r.scard(self._k("stream", stream_id, "unique_words"))))

    # -------------------------
    # Filter state in Redis
    # -------------------------

    def filter_get_state(self, stream_id: str) -> Dict[str, Any]:
        raw = self._redis_exec(lambda: self.r.get(self._k("stream", stream_id, "filter")))
        if not raw:
            raise KeyError(stream_id)
        return json.loads(raw)

    def filter_set_state(self, stream_id: str, state: Dict[str, Any]) -> None:
        self._redis_exec(lambda: self.r.set(self._k("stream", stream_id, "filter"), json.dumps(state)))

    def filter_add_flags(self, stream_id: str, flags: List[Dict[str, Any]], cap: int = 200) -> None:
        if not flags:
            return

        pipe = self.r.pipeline()
        for f in flags:
            key = f"{f.get('rule_id','')}|{str(f.get('match','')).lower()}"
            pipe.sadd(self._k("stream", stream_id, "seen_flags"), key)
            pipe.rpush(self._k("stream", stream_id, "flags"), json.dumps(f))
        if cap > 0:
            pipe.ltrim(self._k("stream", stream_id, "flags"), -cap, -1)

        self._redis_exec(pipe.execute)

    def filter_get_flags(self, stream_id: str, limit: int = 200) -> List[Dict[str, Any]]:
        raw = self._redis_exec(lambda: self.r.lrange(self._k("stream", stream_id, "flags"), -limit, -1))
        return [json.loads(x) for x in raw] if raw else []

    def filter_seen(self, stream_id: str, rule_id: str, match_norm: str) -> bool:
        key = f"{rule_id}|{match_norm}"
        return bool(self._redis_exec(lambda: self.r.sismember(self._k("stream", stream_id, "seen_flags"), key)))

    # -------------------------
    # Distributed rate limiting
    # -------------------------

    def _load_rate_limit_script(self) -> None:
        lua = """
        -- KEYS[1] = bucket key
        -- ARGV[1] = now_ms
        -- ARGV[2] = refill_rate_per_ms
        -- ARGV[3] = capacity
        -- ARGV[4] = cost
        local key = KEYS[1]
        local now = tonumber(ARGV[1])
        local rate = tonumber(ARGV[2])
        local cap = tonumber(ARGV[3])
        local cost = tonumber(ARGV[4])

        local data = redis.call("HMGET", key, "tokens", "ts")
        local tokens = tonumber(data[1])
        local ts = tonumber(data[2])

        if tokens == nil then
          tokens = cap
          ts = now
        end

        local elapsed = now - ts
        if elapsed > 0 then
          tokens = math.min(cap, tokens + elapsed * rate)
          ts = now
        end

        local allowed = 0
        local retry_ms = 0
        if tokens >= cost then
          tokens = tokens - cost
          allowed = 1
        else
          local needed = cost - tokens
          if rate > 0 then
            retry_ms = math.ceil(needed / rate)
          else
            retry_ms = 60000
          end
        end

        redis.call("HMSET", key, "tokens", tokens, "ts", ts)
        redis.call("PEXPIRE", key, 120000)
        return {allowed, retry_ms, tokens}
        """
        try:
            self._lua_rl_sha = self._redis_exec(lambda: self.r.script_load(lua))
        except Exception:
            self._lua_rl_sha = None

    def rate_limit(self, stream_id: str, words: int, priority: str) -> Tuple[bool, float]:
        """
        Returns (allowed, retry_after_seconds)
        """
        if words <= 0:
            return True, 0.0

        if priority == "high":
            tokens_per_min = float(os.environ.get("HIGH_TOKENS_PER_MIN", "5000"))
            cap = float(os.environ.get("HIGH_BURST", str(tokens_per_min)))
            bucket_key = self._k("rl", stream_id, "high")
        else:
            tokens_per_min = float(os.environ.get("NORMAL_TOKENS_PER_MIN", "1000"))
            cap = float(os.environ.get("NORMAL_BURST", str(tokens_per_min)))
            bucket_key = self._k("rl", stream_id, "normal")

        rate_per_ms = (tokens_per_min / 60.0) / 1000.0
        now_ms = int(time.monotonic() * 1000)

        if not self._lua_rl_sha:
            # No lua, apply partition policy
            if self.cfg.partition_policy == "fail_open":
                return True, 0.0
            if self.cfg.partition_policy == "degraded":
                return self._degraded_allow()
            return False, 1.0

        try:
            allowed, retry_ms, _remaining = self._redis_exec(
                lambda: self.r.evalsha(
                    self._lua_rl_sha,
                    1,
                    bucket_key,
                    now_ms,
                    rate_per_ms,
                    cap,
                    float(words),
                )
            )
            return bool(int(allowed) == 1), float(retry_ms) / 1000.0
        except Exception:
            if self.cfg.partition_policy == "fail_open":
                return True, 0.0
            if self.cfg.partition_policy == "degraded":
                return self._degraded_allow()
            return False, 1.0

    def _degraded_allow(self) -> Tuple[bool, float]:
        self._degraded_used += 1
        if self._degraded_used <= self.cfg.degraded_allow_requests:
            return True, 0.0
        return False, 1.0

    # -------------------------
    # Health
    # -------------------------

    def health_check(self) -> bool:
        try:
            return self._redis_exec(self.r.ping) is True
        except Exception:
            return False

    # -------------------------
    # Internal wrapper
    # -------------------------

    def _redis_exec(self, op: Union[Callable[[], Any], Any]) -> Any:
        try:
            if callable(op):
                return op()
            return op
        except redis.RedisError as e:
            raise RedisUnavailable(str(e))
