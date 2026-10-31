"""Test fixtures.

Tests run against a *real* Redis (the Lua scripts are the core of the system,
so faking Redis would test nothing). By default a throwaway redis-server is
started on a free port; set TEST_REDIS_URL to use an existing instance.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import time
from pathlib import Path

import httpx
import pytest
import redis

import mock_upstream.server as mock_server
from weir.core.config import Settings
from weir.main import create_app


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def redis_url():
    url = os.getenv("TEST_REDIS_URL")
    if url:
        yield url
        return
    binary = shutil.which("redis-server")
    if not binary:
        pytest.skip("redis-server not installed and TEST_REDIS_URL not set")
    port = _free_port()
    proc = subprocess.Popen([binary, "--port", str(port), "--save", "", "--appendonly", "no"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    url = f"redis://127.0.0.1:{port}/15"
    for _ in range(50):
        try:
            redis.Redis.from_url(url).ping()
            break
        except redis.ConnectionError:
            time.sleep(0.1)
    yield url
    proc.terminate()
    proc.wait(5)


@pytest.fixture(autouse=True)
def _flush(redis_url):
    redis.Redis.from_url(redis_url).flushdb()


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def fast_mock(monkeypatch):
    monkeypatch.setattr(mock_server, "TOKEN_DELAY", 0.0)
    monkeypatch.setattr(mock_server, "TTFT", 0.0)
    monkeypatch.setattr(mock_server, "COMPLETION_TOKENS", 64)


class CountingTransport(httpx.ASGITransport):
    def __init__(self, app):
        super().__init__(app=app)
        self.calls: list[httpx.Request] = []

    async def handle_async_request(self, request):
        self.calls.append(request)
        return await super().handle_async_request(request)


SEED = {
    "tenants": [
        {"id": "acme", "name": "Acme", "rpm": 1000, "tpm": 1_000_000, "token_budget": 10_000_000,
         "api_keys": ["weir-acme"]},
        {"id": "small", "name": "Small", "rpm": 3, "tpm": 1_000_000, "token_budget": 10_000_000,
         "api_keys": ["weir-small"]},
        {"id": "restricted", "rpm": 100, "tpm": 100_000, "token_budget": 100_000,
         "allowed_models": ["gpt-4o-mini"], "api_keys": ["weir-restricted"]},
        {"id": "off", "enabled": False, "api_keys": ["weir-off"]},
    ]
}


@pytest.fixture
def seed_file(tmp_path: Path) -> str:
    p = tmp_path / "tenants.json"
    p.write_text(json.dumps(SEED))
    return str(p)


@pytest.fixture
def make_settings(redis_url, seed_file):
    def _make(**overrides) -> Settings:
        base = dict(
            redis_url=redis_url,
            upstream_base_url="http://upstream/v1",
            upstream_api_key="sk-upstream-secret",
            tenants_file=seed_file,
            admin_api_key="admin-secret",
            tenant_cache_ttl=0.0,
            log_level="WARNING",
        )
        base.update(overrides)
        return Settings(**base)

    return _make


@pytest.fixture
async def make_client(make_settings):
    """Factory returning (client, upstream_transport, app) with lifespan running."""
    opened = []

    async def _make(use_mock: bool = True, **overrides):
        transport = CountingTransport(mock_server.app) if use_mock else None
        app = create_app(make_settings(**overrides), upstream_transport=transport)
        ctx = app.router.lifespan_context(app)
        await ctx.__aenter__()
        client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                   base_url="http://gateway")
        opened.append((ctx, client))
        return client, transport, app

    yield _make
    for ctx, client in reversed(opened):
        await client.aclose()
        await ctx.__aexit__(None, None, None)


@pytest.fixture
async def gw(make_client):
    return await make_client()


def auth(key: str) -> dict:
    return {"authorization": f"Bearer {key}"}


def chat_body(**kw) -> dict:
    body = {"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "hello world"}]}
    body.update(kw)
    return body
