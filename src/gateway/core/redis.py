from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import TypeVar

import redis.asyncio as aioredis
from redis.exceptions import RedisError

from gateway.core.circuit import CircuitBreaker
from gateway.core.config import Settings
from gateway.core.logging import log_event
from gateway.services.metrics import Metrics

T = TypeVar("T")
log = logging.getLogger("gateway.redis")


class RedisUnavailable(Exception):
    """Raised when Redis cannot be used (error, timeout or open breaker)."""


class RedisGuard:
    """Redis client wrapped in a circuit breaker with tight timeouts.

    Every governance call goes through `call()`. If Redis is slow or down the
    breaker opens after N consecutive failures, after which calls fail
    instantly (no socket wait) so the request path never stalls on Redis.
    """

    def __init__(self, settings: Settings, metrics: Metrics) -> None:
        self.metrics = metrics
        pool = aioredis.BlockingConnectionPool.from_url(
            settings.redis_url,
            max_connections=settings.redis_max_connections,
            timeout=settings.redis_socket_timeout,  # wait for a free pool slot
            socket_timeout=settings.redis_socket_timeout,
            socket_connect_timeout=settings.redis_connect_timeout,
            decode_responses=True,
        )
        self.client: aioredis.Redis = aioredis.Redis(connection_pool=pool)
        self.breaker = CircuitBreaker(
            "redis", settings.redis_breaker_threshold, settings.redis_breaker_recovery_seconds
        )
        self._publish_state()

    def _publish_state(self) -> None:
        self.metrics.breaker_state.labels("redis").set(int(self.breaker.state))

    async def call(self, op: str, fn: Callable[[aioredis.Redis], Awaitable[T]]) -> T:
        if not self.breaker.allow():
            self.metrics.redis_errors.labels(op).inc()
            raise RedisUnavailable("circuit open")
        try:
            result = await fn(self.client)
        except (RedisError, OSError, TimeoutError) as exc:
            self.breaker.record_failure()
            self._publish_state()
            self.metrics.redis_errors.labels(op).inc()
            log_event(log, "redis call failed", logging.WARNING, op=op, error=repr(exc),
                      breaker=self.breaker.state.name)
            raise RedisUnavailable(str(exc)) from exc
        self.breaker.record_success()
        self._publish_state()
        return result

    async def ping(self) -> bool:
        try:
            return bool(await self.call("ping", lambda r: r.ping()))
        except RedisUnavailable:
            return False

    async def close(self) -> None:
        await self.client.aclose()
