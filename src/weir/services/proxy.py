"""Upstream HTTP client: pooled async connections, circuit breaker, and a
zero-copy SSE usage tap."""

from __future__ import annotations

import itertools
import json
import logging
import time

import httpx

from weir.core.circuit import CircuitBreaker
from weir.core.config import Settings
from weir.core.logging import log_event
from weir.services.metrics import Metrics
from weir.services.transport import FastTransport

log = logging.getLogger("weir.upstream")


class UpstreamError(Exception):
    def __init__(self, status: int, message: str, code: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message
        self.code = code


class UpstreamProxy:
    def __init__(self, settings: Settings, metrics: Metrics,
                 transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.settings = settings
        self.metrics = metrics
        timeout = httpx.Timeout(
            connect=settings.upstream_connect_timeout,
            read=settings.upstream_read_timeout,
            write=30.0,
            pool=settings.upstream_connect_timeout,
        )
        if transport is not None or settings.upstream_transport == "fast":
            # FastTransport pools in O(1), so a single client is enough.
            shards = 1
            transports = [transport or FastTransport(settings.upstream_max_connections)]
        else:
            shards = max(1, settings.upstream_pool_shards)
            transports = [None] * shards
        n = max(1, settings.upstream_max_connections // shards)
        self.clients = [
            httpx.AsyncClient(
                base_url=settings.upstream_base_url.rstrip("/") + "/",
                timeout=timeout,
                limits=httpx.Limits(max_connections=n, max_keepalive_connections=n,
                                    keepalive_expiry=60.0),
                transport=t,
                trust_env=False,
            )
            for t in transports
        ]
        self._next = itertools.cycle(range(shards))
        self.breaker = CircuitBreaker(
            "upstream", settings.upstream_breaker_threshold,
            settings.upstream_breaker_recovery_seconds,
        )
        self.metrics.breaker_state.labels("upstream").set(0)

    @property
    def client(self) -> httpx.AsyncClient:
        """Next pool shard (round robin)."""
        return self.clients[next(self._next)]

    async def close(self) -> None:
        for c in self.clients:
            await c.aclose()

    def _headers(self, request_id: str) -> dict[str, str]:
        h = {
            "accept-encoding": "identity",  # SSE must not be compressed for passthrough
            "x-request-id": request_id,
        }
        if self.settings.upstream_api_key:
            h["authorization"] = f"Bearer {self.settings.upstream_api_key}"
        return h

    async def send(self, method: str, path: str, request_id: str,
                   json_body: dict | None = None) -> httpx.Response:
        """Open a streamed upstream response. Caller must aclose() it."""
        if not self.breaker.allow():
            raise UpstreamError(503, "Upstream provider circuit is open; retry shortly.",
                                "upstream_unavailable")
        content = None
        headers = self._headers(request_id)
        if json_body is not None:
            content = json.dumps(json_body, separators=(",", ":")).encode()
            headers["content-type"] = "application/json"
        client = self.client
        req = client.build_request(method, path.lstrip("/"), content=content, headers=headers)
        t0 = time.perf_counter()
        try:
            resp = await client.send(req, stream=True)
        except httpx.TimeoutException as exc:
            self._fail(exc)
            raise UpstreamError(504, "Upstream provider timed out.", "upstream_timeout") from exc
        except httpx.HTTPError as exc:
            self._fail(exc)
            raise UpstreamError(502, f"Could not reach upstream provider: {exc.__class__.__name__}",
                                "upstream_unreachable") from exc
        self.metrics.upstream_ttfb_seconds.observe(time.perf_counter() - t0)
        if resp.status_code >= 500:
            self._fail(None)
        else:
            self.breaker.record_success()
            self.metrics.breaker_state.labels("upstream").set(int(self.breaker.state))
        return resp

    def _fail(self, exc: Exception | None) -> None:
        self.breaker.record_failure()
        self.metrics.breaker_state.labels("upstream").set(int(self.breaker.state))
        if exc is not None:
            log_event(log, "upstream request failed", logging.WARNING, error=repr(exc),
                      breaker=self.breaker.state.name)


class SSEUsageTap:
    """Watches SSE bytes as they are forwarded, without holding them.

    Only the current partial line is retained. JSON is parsed solely for the
    (rare) event that carries a non-null `usage` object, so the per-chunk cost
    is a split and a substring check.
    """

    __slots__ = ("_tail", "usage", "events")
    MAX_LINE = 1 << 20

    def __init__(self) -> None:
        self._tail = b""
        self.usage: tuple[int, int] | None = None
        self.events = 0

    def feed(self, chunk: bytes) -> None:
        data = self._tail + chunk if self._tail else chunk
        lines = data.split(b"\n")
        self._tail = lines.pop()
        if len(self._tail) > self.MAX_LINE:
            self._tail = b""
        for line in lines:
            self._line(line)

    def close(self) -> None:
        if self._tail:
            self._line(self._tail)
            self._tail = b""

    def _line(self, line: bytes) -> None:
        if not line.startswith(b"data:"):
            return
        payload = line[5:].strip()
        if not payload or payload == b"[DONE]":
            return
        self.events += 1
        if b'"usage"' not in payload or b'"usage":null' in payload or b'"usage": null' in payload:
            return
        try:
            usage = json.loads(payload).get("usage")
        except (ValueError, AttributeError):
            return
        if isinstance(usage, dict):
            self.usage = (int(usage.get("prompt_tokens") or 0),
                          int(usage.get("completion_tokens") or 0))


def usage_from_json(content: bytes) -> tuple[dict | None, tuple[int, int] | None]:
    try:
        obj = json.loads(content)
    except ValueError:
        return None, None
    if not isinstance(obj, dict):
        return None, None
    u = obj.get("usage")
    if isinstance(u, dict):
        return obj, (int(u.get("prompt_tokens") or 0), int(u.get("completion_tokens") or 0))
    return obj, None
