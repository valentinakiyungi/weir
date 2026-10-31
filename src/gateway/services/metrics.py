"""Prometheus metrics exposed on GET /metrics.

A dedicated registry is used so multiple app instances (e.g. in tests) don't
collide on the global default registry.
"""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


class Metrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry(auto_describe=True)
        self.requests = Counter(
            "gateway_requests_total",
            "Requests handled by the gateway",
            ["tenant", "route", "status"],
            registry=self.registry,
        )
        self.rejections = Counter(
            "gateway_rejections_total",
            "Requests rejected by governance before reaching upstream",
            ["tenant", "reason"],
            registry=self.registry,
        )
        self.tokens = Counter(
            "gateway_tokens_total",
            "Tokens billed to tenants (actual usage after settlement)",
            ["tenant", "kind"],
            registry=self.registry,
        )
        self.clamped = Counter(
            "gateway_max_tokens_clamped_total",
            "Requests whose max_tokens was reduced to fit the remaining budget",
            ["tenant"],
            registry=self.registry,
        )
        self.governance_seconds = Histogram(
            "gateway_governance_seconds",
            "Time spent in auth + admission control before the upstream call",
            buckets=(0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25),
            registry=self.registry,
        )
        self.upstream_ttfb_seconds = Histogram(
            "gateway_upstream_ttfb_seconds",
            "Upstream time to response headers",
            buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
            registry=self.registry,
        )
        self.active_streams = Gauge(
            "gateway_active_streams",
            "Currently open SSE streams",
            registry=self.registry,
        )
        self.redis_errors = Counter(
            "gateway_redis_errors_total",
            "Redis operations that failed or were short-circuited",
            ["op"],
            registry=self.registry,
        )
        self.degraded = Counter(
            "gateway_degraded_admissions_total",
            "Requests admitted while Redis governance was unavailable (fail-open)",
            registry=self.registry,
        )
        self.breaker_state = Gauge(
            "gateway_breaker_state",
            "Circuit breaker state (0=closed, 1=half-open, 2=open)",
            ["breaker"],
            registry=self.registry,
        )
