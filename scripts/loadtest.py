#!/usr/bin/env python3
"""Concurrent SSE load test for the gateway.

Opens N concurrent streaming chat completions (each held open for the whole
generation), repeats for R rounds per connection, and reports time-to-first-
token (TTFT) and total-duration percentiles. Run it against the upstream
directly and through the gateway to get the latency the gateway adds:

  python scripts/loadtest.py --compare http://127.0.0.1:9200/v1 \
      --url http://127.0.0.1:8000/v1 --key weir-loadtest-dev-key \
      --concurrency 2000 --rounds 2

The client speaks raw HTTP/1.1 on asyncio streams (keep-alive, chunked
decoding) instead of using an HTTP library, so the load generator itself
costs as little CPU as possible and doesn't distort the measurement.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from urllib.parse import urlsplit


def pct(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    values = sorted(values)
    k = max(0, min(len(values) - 1, round(p / 100 * (len(values) - 1))))
    return values[k]


class Conn:
    def __init__(self, host: str, port: int) -> None:
        self.host, self.port = host, port
        self.r: asyncio.StreamReader | None = None
        self.w: asyncio.StreamWriter | None = None

    async def ensure(self) -> None:
        if self.w is None or self.w.is_closing():
            self.r, self.w = await asyncio.open_connection(self.host, self.port, limit=1 << 20)

    def close(self) -> None:
        if self.w is not None:
            self.w.close()
            self.w = None


async def stream_once(conn: Conn, request: bytes, res: dict) -> None:
    t0 = time.perf_counter()
    await conn.ensure()
    r, w = conn.r, conn.w
    w.write(request)
    await w.drain()
    head = await r.readuntil(b"\r\n\r\n")
    status = int(head.split(b" ", 2)[1])
    lower = head.lower()
    if status != 200:
        res["status"][status] = res["status"].get(status, 0) + 1
        conn.close()
        return
    if b"transfer-encoding: chunked" not in lower:
        raise RuntimeError("expected chunked SSE response")
    ttft = None
    events = 0
    done = False
    while True:
        size_line = await r.readuntil(b"\r\n")
        size = int(size_line.split(b";")[0], 16)
        if size == 0:
            await r.readuntil(b"\r\n")
            break
        data = await r.readexactly(size + 2)
        n = data.count(b"data: ")
        if n:
            if ttft is None:
                ttft = time.perf_counter() - t0
            events += n
            if b"[DONE]" in data:
                done = True
    res["status"][200] = res["status"].get(200, 0) + 1
    if not done:
        res["errors"]["no_done"] = res["errors"].get("no_done", 0) + 1
    res["ttft"].append(ttft or 0.0)
    res["total"].append(time.perf_counter() - t0)
    res["events"] += events


async def run(url: str, key: str | None, concurrency: int, rounds: int, max_tokens: int,
              model: str, ramp: float) -> dict:
    u = urlsplit(url)
    host, port = u.hostname, u.port or 80
    path = u.path.rstrip("/") + "/chat/completions"
    body = json.dumps({"model": model, "stream": True, "max_tokens": max_tokens,
                       "messages": [{"role": "user",
                                     "content": "Write a short story about a lighthouse."}]}
                      ).encode()
    hdrs = [f"POST {path} HTTP/1.1", f"host: {host}:{port}", "content-type: application/json",
            f"content-length: {len(body)}", "accept: text/event-stream"]
    if key:
        hdrs.append(f"authorization: Bearer {key}")
    request = ("\r\n".join(hdrs) + "\r\n\r\n").encode() + body

    res: dict = {"ttft": [], "total": [], "status": {}, "errors": {}, "events": 0}
    state = {"open": 0, "max": 0}

    async def worker(i: int) -> None:
        if ramp:
            await asyncio.sleep(ramp * i / concurrency)
        conn = Conn(host, port)
        try:
            for _ in range(rounds):
                state["open"] += 1
                state["max"] = max(state["max"], state["open"])
                try:
                    await asyncio.wait_for(stream_once(conn, request, res), 300)
                except (TimeoutError, OSError, asyncio.IncompleteReadError, RuntimeError,
                        ValueError) as exc:
                    name = type(exc).__name__
                    res["errors"][name] = res["errors"].get(name, 0) + 1
                    conn.close()
                finally:
                    state["open"] -= 1
        finally:
            conn.close()

    t0 = time.perf_counter()
    await asyncio.gather(*(worker(i) for i in range(concurrency)))
    wall = time.perf_counter() - t0
    ok = len(res["total"])
    return {
        "url": url,
        "concurrency": concurrency,
        "peak_concurrent_streams": state["max"],
        "requests": concurrency * rounds,
        "ok": ok,
        "status": res["status"],
        "errors": res["errors"],
        "wall_seconds": round(wall, 2),
        "streams_per_second": round(ok / wall, 1) if wall else 0,
        "chunks_per_second": round(res["events"] / wall) if wall else 0,
        "ttft_ms": {p: round(pct(res["ttft"], p) * 1000, 1) for p in (50, 95, 99)},
        "total_ms": {p: round(pct(res["total"], p) * 1000, 1) for p in (50, 95, 99)},
    }


def show(r: dict) -> None:
    print(f"\n== {r['url']}")
    print(f"   streams ok {r['ok']}/{r['requests']}  peak concurrent {r['peak_concurrent_streams']}"
          f"  status {r['status']}  errors {r['errors'] or '-'}")
    print(f"   wall {r['wall_seconds']}s  {r['streams_per_second']} streams/s  "
          f"{r['chunks_per_second']} SSE events/s")
    print(f"   TTFT  p50 {r['ttft_ms'][50]}ms  p95 {r['ttft_ms'][95]}ms  p99 {r['ttft_ms'][99]}ms")
    print(f"   total p50 {r['total_ms'][50]}ms  p95 {r['total_ms'][95]}ms  "
          f"p99 {r['total_ms'][99]}ms")


async def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", required=True, help="gateway (or upstream) base URL ending in /v1")
    ap.add_argument("--key", help="API key to send")
    ap.add_argument("--compare", help="upstream base URL to benchmark first, for overhead")
    ap.add_argument("--compare-key", default="sk-mock")
    ap.add_argument("--concurrency", type=int, default=500)
    ap.add_argument("--rounds", type=int, default=2)
    ap.add_argument("--max-tokens", type=int, default=20)
    ap.add_argument("--model", default="gpt-4o-mini")
    ap.add_argument("--ramp", type=float, default=1.0, help="seconds to spread connection opens")
    ap.add_argument("--json", help="write results to this file")
    a = ap.parse_args()

    results = []
    if a.compare:
        base = await run(a.compare, a.compare_key, a.concurrency, a.rounds, a.max_tokens,
                         a.model, a.ramp)
        show(base)
        results.append(base)
    gw = await run(a.url, a.key, a.concurrency, a.rounds, a.max_tokens, a.model, a.ramp)
    show(gw)
    results.append(gw)

    if a.compare:
        print("\n== gateway overhead (gateway minus direct)")
        for label in ("ttft_ms", "total_ms"):
            d = {p: round(gw[label][p] - base[label][p], 1) for p in (50, 95, 99)}
            print(f"   {label:8s} p50 {d[50]:+}ms  p95 {d[95]:+}ms  p99 {d[99]:+}ms")
    if a.json:
        with open(a.json, "w") as f:  # noqa: ASYNC230
            json.dump(results, f, indent=2)
    return 0 if gw["ok"] == gw["requests"] else 1


if __name__ == "__main__":
    try:  # the load generator must not be the bottleneck
        import uvloop

        uvloop.install()
    except ImportError:
        pass
    sys.exit(asyncio.run(main()))
