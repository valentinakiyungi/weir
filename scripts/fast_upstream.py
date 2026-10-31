#!/usr/bin/env python3
"""Minimal, very cheap OpenAI-style SSE upstream for load testing.

The FastAPI mock in mock_upstream/ is convenient but costs more CPU per chunk
than the gateway itself, so at high concurrency it becomes the bottleneck and
hides the number you actually want. This server writes pre-encoded chunks on
raw asyncio streams (HTTP/1.1 keep-alive, chunked encoding) so the gateway is
what gets measured.

  python scripts/fast_upstream.py --port 9200 --token-delay-ms 100 --ttft-ms 50
"""

from __future__ import annotations

import argparse
import asyncio
import json

WORD = b"tok "


def sse(obj: dict) -> bytes:
    data = b"data: " + json.dumps(obj, separators=(",", ":")).encode() + b"\n\n"
    return b"%x\r\n%s\r\n" % (len(data), data)


async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, args) -> None:
    try:
        while True:
            head = await reader.readuntil(b"\r\n\r\n")
            lines = head.split(b"\r\n")
            method, path, _ = lines[0].split(b" ", 2)
            length = 0
            for ln in lines[1:]:
                if ln[:15].lower() == b"content-length:":
                    length = int(ln[15:])
            body = json.loads(await reader.readexactly(length)) if length else {}

            if path.endswith(b"/models"):
                payload = b'{"object":"list","data":[{"id":"gpt-4o-mini","object":"model"}]}'
                writer.write(b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n"
                             b"content-length: %d\r\n\r\n%s" % (len(payload), payload))
                await writer.drain()
                continue

            n = min(int(body.get("max_tokens") or args.tokens), args.tokens)
            prompt = 20
            writer.write(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n"
                         b"cache-control: no-cache\r\ntransfer-encoding: chunked\r\n\r\n")
            base = {"id": "chatcmpl-load", "object": "chat.completion.chunk", "created": 0,
                    "model": body.get("model", "m")}
            token_chunk = sse({**base, "choices": [{"index": 0, "delta": {"content": "tok "},
                                                    "finish_reason": None}]})
            await asyncio.sleep(args.ttft)
            writer.write(sse({**base, "choices": [{"index": 0, "delta": {"role": "assistant"},
                                                   "finish_reason": None}]}))
            await writer.drain()
            for _ in range(n):
                await asyncio.sleep(args.delay)
                writer.write(token_chunk)
                await writer.drain()
            writer.write(sse({**base, "choices": [], "usage": {
                "prompt_tokens": prompt, "completion_tokens": n, "total_tokens": prompt + n}}))
            data = b"data: [DONE]\n\n"
            writer.write(b"%x\r\n%s\r\n0\r\n\r\n" % (len(data), data))
            await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionError, ValueError):
        pass
    finally:
        writer.close()


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9200)
    ap.add_argument("--token-delay-ms", type=float, default=100)
    ap.add_argument("--ttft-ms", type=float, default=50)
    ap.add_argument("--tokens", type=int, default=20)
    args = ap.parse_args()
    args.delay = args.token_delay_ms / 1000
    args.ttft = args.ttft_ms / 1000
    server = await asyncio.start_server(lambda r, w: handle(r, w, args), "0.0.0.0", args.port,
                                        backlog=8192)
    print(f"fast upstream on :{args.port}", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        import uvloop

        uvloop.install()
    except ImportError:
        pass
    asyncio.run(main())
