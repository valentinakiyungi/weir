"""A small OpenAI-compatible mock provider for local development, tests and
load testing. It honours max_tokens, streams SSE chunks at a configurable
cadence and reports usage exactly like the real API.

Env:
  MOCK_TOKEN_DELAY_MS     delay between streamed tokens (default 20)
  MOCK_COMPLETION_TOKENS  tokens generated when max_tokens allows (default 64)
  MOCK_TTFT_MS            delay before the first token (default 50)

Special models: "mock-error-500" -> 500, "mock-error-429" -> 429.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

TOKEN_DELAY = float(os.getenv("MOCK_TOKEN_DELAY_MS", "20")) / 1000
COMPLETION_TOKENS = int(os.getenv("MOCK_COMPLETION_TOKENS", "64"))
TTFT = float(os.getenv("MOCK_TTFT_MS", "50")) / 1000
WORDS = ("lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor "
         "incididunt ut labore et dolore magna aliqua").split()

app = FastAPI(title="Mock LLM upstream")


def _prompt_tokens(body: dict) -> int:
    # Different from the gateway's estimator on purpose, so reservation and
    # settlement genuinely differ in tests.
    text = " ".join(str(m.get("content", "")) for m in body.get("messages", []))
    return 7 + 4 * len(body.get("messages", [])) + int(len(text.split()) * 1.3)


def _n_tokens(body: dict) -> int:
    cap = body.get("max_completion_tokens") or body.get("max_tokens") or COMPLETION_TOKENS
    return max(0, min(int(cap), COMPLETION_TOKENS))


def _error(status: int, msg: str):
    return JSONResponse({"error": {"message": msg, "type": "mock_error", "code": None}},
                        status_code=status)


@app.post("/v1/chat/completions")
async def chat(request: Request):
    body = await request.json()
    model = body.get("model", "mock")
    if model == "mock-error-500":
        return _error(500, "mock upstream failure")
    if model == "mock-error-429":
        return _error(429, "mock upstream rate limit")
    n = _n_tokens(body)
    prompt = _prompt_tokens(body)
    cid = "chatcmpl-" + uuid.uuid4().hex[:24]
    created = int(time.time())
    usage = {"prompt_tokens": prompt, "completion_tokens": n, "total_tokens": prompt + n}

    if not body.get("stream"):
        await asyncio.sleep(TTFT + TOKEN_DELAY * n)
        text = " ".join(WORDS[i % len(WORDS)] for i in range(n))
        return {
            "id": cid, "object": "chat.completion", "created": created, "model": model,
            "choices": [{"index": 0, "message": {"role": "assistant", "content": text},
                         "finish_reason": "length" if n < COMPLETION_TOKENS else "stop"}],
            "usage": usage,
        }

    include_usage = bool((body.get("stream_options") or {}).get("include_usage"))

    def chunk(delta: dict, finish=None, usage_obj=None, choices=True) -> bytes:
        obj = {"id": cid, "object": "chat.completion.chunk", "created": created, "model": model,
               "choices": [{"index": 0, "delta": delta, "finish_reason": finish}] if choices
               else []}
        if include_usage:
            obj["usage"] = usage_obj
        return b"data: " + json.dumps(obj, separators=(",", ":")).encode() + b"\n\n"

    async def gen():
        await asyncio.sleep(TTFT)
        yield chunk({"role": "assistant", "content": ""})
        for i in range(n):
            if TOKEN_DELAY:
                await asyncio.sleep(TOKEN_DELAY)
            yield chunk({"content": WORDS[i % len(WORDS)] + " "})
        yield chunk({}, finish="length" if n < COMPLETION_TOKENS else "stop")
        if include_usage:
            yield chunk({}, usage_obj=usage, choices=False)
        yield b"data: [DONE]\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.post("/v1/embeddings")
async def embeddings(request: Request):
    body = await request.json()
    inp = body.get("input")
    items = inp if isinstance(inp, list) else [inp]
    tokens = sum(len(str(x).split()) for x in items)
    return {"object": "list", "model": body.get("model"),
            "data": [{"object": "embedding", "index": i, "embedding": [0.0, 0.1, 0.2]}
                     for i in range(len(items))],
            "usage": {"prompt_tokens": tokens, "total_tokens": tokens}}


@app.get("/v1/models")
async def models():
    ids = ["gpt-4o-mini", "gpt-4o", "text-embedding-3-small", "mock"]
    return {"object": "list", "data": [{"id": i, "object": "model", "owned_by": "mock"}
                                       for i in ids]}
