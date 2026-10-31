#!/usr/bin/env python3
"""Measure budget overrun under bursty concurrent traffic.

Creates two fresh tenants through the admin API and fires the same burst of
concurrent streaming requests at each:

  governed   - token_budget = B, enforced by the gateway's atomic reservations
  unlimited  - no budget; used to replay what a conventional
               "check used < budget, then charge after the response" limiter
               would have admitted given the exact same request timeline

and prints tokens spent vs budget for both.

  python scripts/budget_burst.py --url http://127.0.0.1:8000 --admin-key admin-secret \
      --budget 20000 --requests 400 --concurrency 100 --max-tokens 64
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
import uuid

import httpx


async def burst(client: httpx.AsyncClient, key: str, n: int, conc: int, max_tokens: int):
    sem = asyncio.Semaphore(conc)
    timeline = []  # (start, end, tokens, status)
    t0 = time.perf_counter()

    async def one(i: int):
        async with sem:
            start = time.perf_counter() - t0
            body = {"model": "gpt-4o-mini", "stream": True, "max_tokens": max_tokens,
                    "messages": [{"role": "user", "content": f"request {i}: tell me a story"}]}
            tokens = 0
            async with client.stream("POST", "/v1/chat/completions", json=body,
                                     headers={"authorization": f"Bearer {key}"}) as r:
                if r.status_code == 200:
                    async for line in r.aiter_lines():
                        if line.startswith("data: ") and '"usage":{' in line:
                            u = json.loads(line[6:])["usage"]
                            tokens = u["prompt_tokens"] + u["completion_tokens"]
                else:
                    await r.aread()
            timeline.append((start, time.perf_counter() - t0, tokens, r.status_code))

    await asyncio.gather(*(one(i) for i in range(n)))
    return timeline


def naive_replay(timeline, budget: int) -> int:
    """Check-then-charge: a request is admitted if the tokens of requests that
    had *completed* before it started are still under budget."""
    finished = sorted((end, tok) for _, end, tok, _ in timeline)
    spent = 0
    for start, _, tok, _ in sorted(timeline):
        charged = sum(t for end, t in finished if end <= start)
        if charged < budget:
            spent += tok
    return spent


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:8000")
    ap.add_argument("--admin-key", required=True)
    ap.add_argument("--budget", type=int, default=20_000)
    ap.add_argument("--requests", type=int, default=400)
    ap.add_argument("--concurrency", type=int, default=100)
    ap.add_argument("--max-tokens", type=int, default=64)
    a = ap.parse_args()

    admin = {"authorization": f"Bearer {a.admin_key}"}
    suffix = uuid.uuid4().hex[:6]
    async with httpx.AsyncClient(base_url=a.url, timeout=120,
                                 limits=httpx.Limits(max_connections=a.concurrency + 10)) as c:
        keys = {}
        for name, budget in (("governed", a.budget), ("unlimited", 0)):
            r = await c.post("/admin/tenants", headers=admin, json={
                "id": f"burst-{name}-{suffix}", "rpm": 0, "tpm": 0, "token_budget": budget})
            r.raise_for_status()
            keys[name] = r.json()["api_key"]

        gov = await burst(c, keys["governed"], a.requests, a.concurrency, a.max_tokens)
        free = await burst(c, keys["unlimited"], a.requests, a.concurrency, a.max_tokens)

        u = (await c.get(f"/admin/tenants/burst-governed-{suffix}/usage", headers=admin)).json()

    governed_spent = u["used"]
    naive_spent = naive_replay(free, a.budget)
    unlimited_spent = sum(x[2] for x in free)

    def over(x):
        return f"{(x - a.budget) / a.budget * 100:+.1f}%"

    print(f"budget                       {a.budget:>8} tokens")
    print(f"no limiter                   {unlimited_spent:>8} tokens  ({over(unlimited_spent)})")
    print(f"check-then-charge (replayed) {naive_spent:>8} tokens  ({over(naive_spent)})")
    print(f"gateway reservations         {governed_spent:>8} tokens  ({over(governed_spent)})"
          f"  admitted {sum(1 for x in gov if x[3] == 200)}/{a.requests},"
          f" reserved left {u['reserved_in_flight']}")


if __name__ == "__main__":
    asyncio.run(main())
