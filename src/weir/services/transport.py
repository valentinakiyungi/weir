"""A lean HTTP/1.1 transport for httpx, tuned for long-lived SSE streams.

httpx's default transport (httpcore + h11 + anyio) costs tens of microseconds
of pure-Python work per received chunk: an anyio cancel scope per read, h11
event parsing and connection-pool bookkeeping that is O(n) in the number of
open connections. For an SSE proxy holding thousands of streams that is the
dominant CPU cost (profiling showed ~45% of gateway CPU in that stack).

This transport keeps the httpx API (AsyncClient, Request, Response, timeouts,
exceptions) but does the wire work directly on asyncio streams:

* keep-alive pooling per origin with O(1) acquire/release
* Content-Length, chunked and close-delimited bodies
* one reschedulable asyncio timeout per response (no per-read allocation)
* transparent single retry when a pooled idle connection turns out to be dead
* TLS via the stdlib ssl module with certifi roots

It deliberately does not implement HTTP/2, proxies, or 100-continue.
"""

from __future__ import annotations

import asyncio
import ssl
import time
from collections import deque

import httpx

try:
    import certifi

    _CAFILE: str | None = certifi.where()
except ImportError:  # pragma: no cover
    _CAFILE = None

_NO_BODY_STATUS = {204, 304}


class _Conn:
    __slots__ = ("reader", "writer", "key", "idle_since")

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, key) -> None:
        self.reader = reader
        self.writer = writer
        self.key = key
        self.idle_since = 0.0

    def usable(self, now: float, expiry: float) -> bool:
        return (not self.writer.is_closing() and not self.reader.at_eof()
                and now - self.idle_since < expiry)

    def close(self) -> None:
        if not self.writer.is_closing():
            self.writer.close()


class _ResponseStream(httpx.AsyncByteStream):
    def __init__(self, transport: FastTransport, conn: _Conn, request: httpx.Request,
                 mode: str, length: int, keep_alive: bool, read_timeout: float | None) -> None:
        self._t = transport
        self._conn = conn
        self._request = request
        self._mode = mode  # "length" | "chunked" | "eof" | "empty"
        self._remaining = length
        self._keep_alive = keep_alive
        self._read_timeout = read_timeout
        self._done = mode == "empty"
        self._closed = False

    async def __aiter__(self):
        if self._done:
            self._finish()
            return
        reader = self._conn.reader
        loop = asyncio.get_running_loop()
        rt = self._read_timeout
        try:
            async with asyncio.timeout(None) as tm:
                if self._mode == "chunked":
                    while True:
                        if rt is not None:
                            tm.reschedule(loop.time() + rt)
                        line = await reader.readuntil(b"\r\n")
                        size = int(line.split(b";", 1)[0], 16)
                        if size == 0:
                            # trailers (normally none) end with an empty line
                            while (await reader.readuntil(b"\r\n")) != b"\r\n":
                                pass
                            break
                        if rt is not None:
                            tm.reschedule(loop.time() + rt)
                        data = await reader.readexactly(size + 2)
                        yield data[:-2]
                elif self._mode == "length":
                    while self._remaining > 0:
                        if rt is not None:
                            tm.reschedule(loop.time() + rt)
                        data = await reader.read(min(self._remaining, 65536))
                        if not data:
                            raise httpx.RemoteProtocolError(
                                "peer closed connection before body completed",
                                request=self._request)
                        self._remaining -= len(data)
                        yield data
                else:  # read until EOF
                    while True:
                        if rt is not None:
                            tm.reschedule(loop.time() + rt)
                        data = await reader.read(65536)
                        if not data:
                            break
                        yield data
                    self._keep_alive = False
        except TimeoutError as exc:
            self._abort()
            raise httpx.ReadTimeout("upstream read timed out", request=self._request) from exc
        except (asyncio.IncompleteReadError, ConnectionError, OSError, ValueError) as exc:
            self._abort()
            raise httpx.ReadError(f"upstream read failed: {exc!r}", request=self._request) from exc
        self._done = True
        self._finish()

    def _finish(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._done and self._keep_alive:
            self._t._release(self._conn)
        else:
            self._t._discard(self._conn)

    def _abort(self) -> None:
        self._keep_alive = False
        self._done = False
        self._finish()

    async def aclose(self) -> None:
        # Closing before the body was fully read means the connection is in an
        # unknown state: drop it (this is also how an upstream generation is
        # cancelled when the downstream client disconnects).
        if not self._done:
            self._keep_alive = False
        self._finish()


class FastTransport(httpx.AsyncBaseTransport):
    def __init__(self, max_connections: int = 4096, keepalive_expiry: float = 60.0,
                 verify: bool | ssl.SSLContext = True) -> None:
        self._idle: dict[tuple, deque[_Conn]] = {}
        self._sem = asyncio.Semaphore(max_connections) if max_connections else None
        self._expiry = keepalive_expiry
        self._open = 0
        if isinstance(verify, ssl.SSLContext):
            self._ssl = verify
        elif verify:
            self._ssl = ssl.create_default_context(cafile=_CAFILE)
        else:
            self._ssl = ssl._create_unverified_context()
        self._ssl.set_alpn_protocols(["http/1.1"])

    @property
    def open_connections(self) -> int:
        return self._open

    # ----------------------------------------------------------------- pool
    def _release(self, conn: _Conn) -> None:
        conn.idle_since = time.monotonic()
        self._idle.setdefault(conn.key, deque()).append(conn)

    def _discard(self, conn: _Conn) -> None:
        conn.close()
        self._open -= 1
        if self._sem is not None:
            self._sem.release()

    async def _acquire(self, key, request: httpx.Request, connect_timeout: float | None,
                       pool_timeout: float | None) -> tuple[_Conn, bool]:
        idle = self._idle.get(key)
        now = time.monotonic()
        while idle:
            conn = idle.pop()  # LIFO: warmest connection first
            if conn.usable(now, self._expiry):
                return conn, True
            self._discard(conn)
        if self._sem is not None:
            try:
                async with asyncio.timeout(pool_timeout):
                    await self._sem.acquire()
            except TimeoutError as exc:
                raise httpx.PoolTimeout("no free upstream connection", request=request) from exc
        scheme, host, port = key
        try:
            async with asyncio.timeout(connect_timeout):
                reader, writer = await asyncio.open_connection(
                    host, port,
                    ssl=self._ssl if scheme == "https" else None,
                    server_hostname=host if scheme == "https" else None,
                    limit=1 << 20,
                )
        except TimeoutError as exc:
            if self._sem is not None:
                self._sem.release()
            raise httpx.ConnectTimeout(f"connect to {host}:{port} timed out",
                                       request=request) from exc
        except OSError as exc:
            if self._sem is not None:
                self._sem.release()
            raise httpx.ConnectError(f"connect to {host}:{port} failed: {exc}",
                                     request=request) from exc
        self._open += 1
        return _Conn(reader, writer, key), False

    # ------------------------------------------------------------- request
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url = request.url
        scheme = url.scheme
        port = url.port or (443 if scheme == "https" else 80)
        key = (scheme, url.host, port)
        timeouts = request.extensions.get("timeout", {})
        body = await request.aread()

        head = [request.method.encode(), b" ", url.raw_path, b" HTTP/1.1\r\n"]
        has_length = False
        for name, value in request.headers.raw:
            lname = name.lower()
            if lname == b"content-length":
                has_length = True
            elif lname == b"transfer-encoding":
                continue  # body is always sent with a length
            head += [name, b": ", value, b"\r\n"]
        if body and not has_length:
            head += [b"content-length: ", str(len(body)).encode(), b"\r\n"]
        head.append(b"\r\n")
        payload = b"".join(head) + body

        for attempt in (0, 1):
            conn, reused = await self._acquire(key, request, timeouts.get("connect"),
                                               timeouts.get("pool"))
            try:
                return await self._exchange(conn, request, payload, timeouts)
            except _StaleConnection:
                self._discard(conn)
                if not reused or attempt:
                    raise httpx.RemoteProtocolError(
                        "server closed connection without response", request=request) from None
            except BaseException:
                self._discard(conn)
                raise
        raise AssertionError("unreachable")  # pragma: no cover

    async def _exchange(self, conn: _Conn, request: httpx.Request, payload: bytes,
                        timeouts: dict) -> httpx.Response:
        read_timeout = timeouts.get("read")
        try:
            conn.writer.write(payload)
            async with asyncio.timeout(timeouts.get("write")):
                await conn.writer.drain()
            async with asyncio.timeout(read_timeout):
                head = await conn.reader.readuntil(b"\r\n\r\n")
        except asyncio.IncompleteReadError as exc:
            if not exc.partial:
                raise _StaleConnection from exc
            raise httpx.RemoteProtocolError("truncated response head", request=request) from exc
        except (ConnectionResetError, BrokenPipeError) as exc:
            raise _StaleConnection from exc
        except TimeoutError as exc:
            raise httpx.ReadTimeout("upstream response timed out", request=request) from exc
        except (OSError, asyncio.LimitOverrunError, ValueError) as exc:
            raise httpx.ReadError(f"upstream read failed: {exc!r}", request=request) from exc

        lines = head[:-4].split(b"\r\n")
        try:
            version, status_s, *_ = lines[0].split(b" ", 2)
            status = int(status_s)
        except ValueError as exc:
            raise httpx.RemoteProtocolError("malformed status line", request=request) from exc

        headers: list[tuple[bytes, bytes]] = []
        length = -1
        chunked = False
        keep_alive = version == b"HTTP/1.1"
        for line in lines[1:]:
            name, _, value = line.partition(b":")
            value = value.strip()
            headers.append((name, value))
            lname = name.lower()
            if lname == b"content-length":
                length = int(value)
            elif lname == b"transfer-encoding":
                chunked = b"chunked" in value.lower()
            elif lname == b"connection":
                v = value.lower()
                if b"close" in v:
                    keep_alive = False
                elif b"keep-alive" in v:
                    keep_alive = True

        if request.method == "HEAD" or status in _NO_BODY_STATUS or 100 <= status < 200:
            mode, length = "empty", 0
        elif chunked:
            mode = "chunked"
        elif length >= 0:
            mode = "length" if length else "empty"
        else:
            mode = "eof"

        stream = _ResponseStream(self, conn, request, mode, max(length, 0), keep_alive,
                                 read_timeout)
        return httpx.Response(status, headers=headers, stream=stream,
                              extensions={"http_version": version})

    async def aclose(self) -> None:
        for idle in self._idle.values():
            while idle:
                self._discard(idle.pop())
        self._idle.clear()


class _StaleConnection(Exception):
    pass
