"""End-to-end over real sockets: real uvicorn servers for both the gateway and
the mock provider, so streaming behaviour (incremental delivery, client
disconnects) is exercised exactly as in production."""

import socket
import time

import anyio
import httpx
import pytest
import uvicorn

import mock_upstream.server as mock_server
from tests.conftest import auth, chat_body
from weir.main import create_app

pytestmark = pytest.mark.anyio


def _port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(params=["fast", "httpx"])
async def live(make_settings, monkeypatch, request):
    monkeypatch.setattr(mock_server, "TOKEN_DELAY", 0.05)
    monkeypatch.setattr(mock_server, "TTFT", 0.0)
    mport, gport = _port(), _port()
    app = create_app(make_settings(upstream_base_url=f"http://127.0.0.1:{mport}/v1",
                                   upstream_transport=request.param))
    servers = [
        uvicorn.Server(uvicorn.Config(mock_server.app, port=mport, log_level="warning")),
        uvicorn.Server(uvicorn.Config(app, port=gport, log_level="warning")),
    ]
    async with anyio.create_task_group() as tg:
        for s in servers:
            tg.start_soon(s.serve)
        while not all(s.started for s in servers):  # noqa: ASYNC110
            await anyio.sleep(0.02)
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{gport}", timeout=10) as c:
            yield c
        for s in servers:
            s.should_exit = True


async def test_chunks_are_delivered_incrementally(live):
    t0 = time.perf_counter()
    arrivals = []
    async with live.stream("POST", "/v1/chat/completions", headers=auth("weir-acme"),
                           json=chat_body(stream=True, max_tokens=10)) as r:
        assert r.status_code == 200
        async for line in r.aiter_lines():
            if line.startswith("data: "):
                arrivals.append(time.perf_counter() - t0)
    total = arrivals[-1]
    assert total >= 0.45  # 10 tokens x 50ms
    assert arrivals[0] < 0.2  # first event long before the stream finishes (no buffering)
    assert arrivals[5] < total - 0.15  # mid-stream events arrive mid-stream


async def test_client_disconnect_releases_reservation_and_bills_partial(live):
    async with live.stream("POST", "/v1/chat/completions", headers=auth("weir-acme"),
                           json=chat_body(stream=True, max_tokens=40)) as r:
        n = 0
        async for line in r.aiter_lines():
            if line.startswith("data: "):
                n += 1
            if n == 4:
                break  # hang up mid-stream
    for _ in range(50):
        u = (await live.get("/admin/tenants/acme/usage", headers=auth("admin-secret"))).json()
        if u["requests"] == 1:
            break
        await anyio.sleep(0.05)
    assert u["reserved_in_flight"] == 0
    assert 0 < u["completion_tokens"] < 40
    assert u["requests"] == 1
