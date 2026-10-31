"""FastTransport against a real HTTP server (uvicorn + the mock provider) and
against a raw socket server for edge cases uvicorn won't produce."""

import asyncio
import json
import socket

import anyio
import httpx
import pytest
import uvicorn

import mock_upstream.server as mock_server
from gateway.services.transport import FastTransport

pytestmark = pytest.mark.anyio


def _port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
async def mock_url():
    port = _port()
    server = uvicorn.Server(uvicorn.Config(mock_server.app, port=port, log_level="warning"))
    async with anyio.create_task_group() as tg:
        tg.start_soon(server.serve)
        while not server.started:  # noqa: ASYNC110
            await anyio.sleep(0.01)
        yield f"http://127.0.0.1:{port}/v1/"
        server.should_exit = True


async def test_json_stream_and_keepalive_reuse(mock_url):
    t = FastTransport(max_connections=4)
    async with httpx.AsyncClient(transport=t, base_url=mock_url) as c:
        r = await c.post("chat/completions", json={"model": "m", "max_tokens": 3,
                                                   "messages": [{"content": "hi"}]})
        assert r.status_code == 200 and r.json()["usage"]["completion_tokens"] == 3
        async with c.stream("POST", "chat/completions", json={
                "model": "m", "stream": True, "max_tokens": 4,
                "messages": [{"content": "hi"}]}) as s:
            body = b"".join([x async for x in s.aiter_raw()])
        assert body.endswith(b"data: [DONE]\n\n")
        assert body.count(b"data: ") == 7  # role + 4 tokens + finish + DONE
        r = await c.get("models")
        assert r.status_code == 200 and r.json()["data"]
        r = await c.post("chat/completions", json={"model": "mock-error-500", "messages": []})
        assert r.status_code == 500
        assert t.open_connections == 1  # everything reused one keep-alive connection


async def test_early_close_drops_connection(mock_url):
    t = FastTransport()
    async with httpx.AsyncClient(transport=t, base_url=mock_url) as c:
        async with c.stream("POST", "chat/completions", json={
                "model": "m", "stream": True, "messages": [{"content": "hi"}]}) as s:
            async for _ in s.aiter_raw():
                break
        assert t.open_connections == 0  # half-read connection must not be pooled
        r = await c.get("models")
        assert r.status_code == 200


async def test_connect_error_maps_to_httpx():
    async with httpx.AsyncClient(transport=FastTransport(), base_url="http://127.0.0.1:1/") as c:
        with pytest.raises(httpx.ConnectError):
            await c.get("x")


async def _raw_server(handler):
    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1]


async def test_read_timeout_and_stale_retry():
    calls = {"n": 0}

    async def handler(reader, writer):
        calls["n"] += 1
        await reader.readuntil(b"\r\n\r\n")
        if calls["n"] == 1:
            # answer, keep-alive, then silently close the idle connection
            writer.write(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
            await writer.drain()
            await asyncio.sleep(0.05)
            writer.close()
            return
        if calls["n"] == 2:
            writer.write(b"HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\nfresh")
            await writer.drain()
            await reader.readuntil(b"\r\n\r\n")
            await asyncio.sleep(5)  # never answers the next request
        writer.close()

    server, port = await _raw_server(handler)
    async with server:
        async with httpx.AsyncClient(transport=FastTransport(), timeout=httpx.Timeout(0.3),
                                     base_url=f"http://127.0.0.1:{port}/") as c:
            assert (await c.get("a")).text == "ok"
            await asyncio.sleep(0.15)  # server has closed the pooled connection
            assert (await c.get("b")).text == "fresh"  # transparently retried
            with pytest.raises(httpx.ReadTimeout):
                await c.get("c")


async def test_close_delimited_and_chunked_trailers():
    async def handler(reader, writer):
        line = await reader.readuntil(b"\r\n\r\n")
        if b"/eof" in line:
            writer.write(b"HTTP/1.1 200 OK\r\nconnection: close\r\n\r\nuntil-eof")
        else:
            writer.write(b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n"
                         b"3;ext=1\r\nabc\r\n2\r\nde\r\n0\r\nx-trailer: 1\r\n\r\n")
        await writer.drain()
        writer.close()

    server, port = await _raw_server(handler)
    async with server:
        async with httpx.AsyncClient(transport=FastTransport(),
                                     base_url=f"http://127.0.0.1:{port}/") as c:
            assert (await c.get("eof")).text == "until-eof"
            assert (await c.get("chunked")).text == "abcde"


async def test_request_body_and_headers_are_sent():
    seen = {}

    async def handler(reader, writer):
        head = await reader.readuntil(b"\r\n\r\n")
        length = int([ln for ln in head.split(b"\r\n") if ln.lower().startswith(
            b"content-length")][0].split(b":")[1])
        seen["head"] = head
        seen["body"] = await reader.readexactly(length)
        writer.write(b"HTTP/1.1 204 No Content\r\n\r\n")
        await writer.drain()
        writer.close()

    server, port = await _raw_server(handler)
    async with server:
        async with httpx.AsyncClient(transport=FastTransport(),
                                     base_url=f"http://127.0.0.1:{port}/v1/") as c:
            r = await c.post("chat?x=1", json={"a": 1}, headers={"x-test": "yes"})
    assert r.status_code == 204
    assert seen["head"].startswith(b"POST /v1/chat?x=1 HTTP/1.1\r\n")
    assert b"x-test: yes" in seen["head"]
    assert json.loads(seen["body"]) == {"a": 1}
