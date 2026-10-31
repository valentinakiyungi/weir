from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from weir import __version__
from weir.api import admin, errors, v1
from weir.api.deps import Services
from weir.core.circuit import BreakerState
from weir.core.config import Settings, get_settings
from weir.core.logging import configure_logging, log_event
from weir.core.redis import RedisGuard
from weir.services.limiter import Governor
from weir.services.metrics import Metrics
from weir.services.proxy import UpstreamProxy
from weir.services.tenants import TenantRegistry

log = logging.getLogger("weir")


def create_app(settings: Settings | None = None,
               upstream_transport: httpx.AsyncBaseTransport | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        metrics = Metrics()
        redis = RedisGuard(settings, metrics)
        tenants = TenantRegistry(settings, redis)
        governor = Governor(settings, redis, metrics)
        upstream = UpstreamProxy(settings, metrics, transport=upstream_transport)
        app.state.services = Services(settings, metrics, redis, tenants, governor, upstream)
        await tenants.load_seed()
        log_event(log, "weir started", version=__version__,
                  upstream=settings.upstream_base_url, redis_ok=await redis.ping(),
                  fail_open=settings.fail_open, budget_period=settings.budget_period)
        try:
            yield
        finally:
            await upstream.close()
            await redis.close()

    app = FastAPI(title="Enterprise LLM Gateway", version=__version__, lifespan=lifespan)
    errors.register(app)
    app.include_router(v1.router)
    app.include_router(admin.router)

    @app.get("/health", tags=["ops"])
    async def health():
        return {"status": "healthy", "service": "weir", "version": __version__}

    @app.get("/ready", tags=["ops"])
    async def ready(request: Request):
        svc = request.app.state.services
        redis_ok = await svc.redis.ping()
        upstream_open = svc.upstream.breaker.state is BreakerState.OPEN
        status = "ready" if redis_ok else ("degraded" if svc.settings.fail_open else "unavailable")
        code = 503 if status == "unavailable" else 200
        return JSONResponse({
            "status": status,
            "redis": "ok" if redis_ok else "unavailable",
            "redis_breaker": svc.redis.breaker.state.name.lower(),
            "upstream_breaker": svc.upstream.breaker.state.name.lower(),
            "upstream_circuit_open": upstream_open,
        }, status_code=code)

    @app.get("/metrics", tags=["ops"])
    async def metrics(request: Request):
        reg = request.app.state.services.metrics.registry
        return Response(generate_latest(reg), media_type=CONTENT_TYPE_LATEST)

    return app


app = create_app()


def run() -> None:
    import uvicorn

    uvicorn.run(
        "weir.main:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8000")),
        workers=int(os.getenv("WORKERS", "1")),
        access_log=False,
        timeout_keep_alive=75,
        backlog=4096,
    )
