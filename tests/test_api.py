import json

import anyio
import pytest
import redis

from tests.conftest import auth, chat_body

pytestmark = pytest.mark.anyio


async def usage(client, tenant):
    r = await client.get(f"/admin/tenants/{tenant}/usage", headers=auth("admin-secret"))
    assert r.status_code == 200, r.text
    return r.json()


# ---------------------------------------------------------------- ops
async def test_health_and_ready(gw):
    client, _, _ = gw
    assert (await client.get("/health")).json()["status"] == "healthy"
    r = await client.get("/ready")
    assert r.status_code == 200 and r.json()["redis"] == "ok"


async def test_metrics_exposed(gw):
    client, _, _ = gw
    await client.post("/v1/chat/completions", headers=auth("weir-acme"), json=chat_body())
    text = (await client.get("/metrics")).text
    assert 'weir_requests_total{route="chat",status="200",tenant="acme"}' in text
    assert "weir_governance_seconds_bucket" in text


# ---------------------------------------------------------------- auth
@pytest.mark.parametrize("headers,status,code", [
    ({}, 401, "missing_api_key"),
    (auth("weir-nope"), 401, "invalid_api_key"),
    (auth("weir-off"), 403, "tenant_disabled"),
])
async def test_auth_failures(gw, headers, status, code):
    client, transport, _ = gw
    r = await client.post("/v1/chat/completions", headers=headers, json=chat_body())
    assert r.status_code == status
    assert r.json()["error"]["code"] == code
    assert transport.calls == []


async def test_x_api_key_header_accepted(gw):
    client, _, _ = gw
    r = await client.post("/v1/chat/completions", headers={"x-api-key": "weir-acme"},
                          json=chat_body())
    assert r.status_code == 200


async def test_tenant_key_never_reaches_upstream(gw):
    client, transport, _ = gw
    await client.post("/v1/chat/completions", headers=auth("weir-acme"), json=chat_body())
    sent = transport.calls[0]
    assert sent.headers["authorization"] == "Bearer sk-upstream-secret"


# ---------------------------------------------------------------- proxying
async def test_non_streaming_completion_is_billed_with_actual_usage(gw):
    client, _, _ = gw
    r = await client.post("/v1/chat/completions", headers=auth("weir-acme"),
                          json=chat_body(max_tokens=10))
    assert r.status_code == 200
    body = r.json()
    assert body["usage"]["completion_tokens"] == 10
    assert r.headers["x-weir-tenant"] == "acme"
    assert int(r.headers["x-ratelimit-remaining-requests"]) == 999
    u = await usage(client, "acme")
    assert u["used"] == body["usage"]["total_tokens"]
    assert u["reserved_in_flight"] == 0
    assert u["requests"] == 1


async def test_streaming_passthrough_and_usage_injection(gw):
    client, transport, _ = gw
    async with client.stream("POST", "/v1/chat/completions", headers=auth("weir-acme"),
                             json=chat_body(stream=True, max_tokens=5)) as r:
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/event-stream")
        raw = b"".join([c async for c in r.aiter_bytes()])
    events = [ln[6:] for ln in raw.decode().split("\n") if ln.startswith("data: ")]
    assert events[-1] == "[DONE]"
    final = json.loads(events[-2])
    assert final["usage"]["completion_tokens"] == 5
    sent = json.loads(transport.calls[0].content)
    assert sent["stream_options"] == {"include_usage": True}
    u = await usage(client, "acme")
    assert u["used"] == final["usage"]["total_tokens"]
    assert u["reserved_in_flight"] == 0


async def test_max_tokens_injected_when_missing(gw):
    client, transport, app = gw
    await client.post("/v1/chat/completions", headers=auth("weir-acme"), json=chat_body())
    sent = json.loads(transport.calls[0].content)
    assert sent["max_tokens"] == app.state.services.settings.default_max_tokens


async def test_model_allow_list(gw):
    client, transport, _ = gw
    r = await client.post("/v1/chat/completions", headers=auth("weir-restricted"),
                          json=chat_body(model="gpt-4o"))
    assert r.status_code == 403 and r.json()["error"]["code"] == "model_not_allowed"
    r = await client.get("/v1/models", headers=auth("weir-restricted"))
    assert [m["id"] for m in r.json()["data"]] == ["gpt-4o-mini"]


async def test_embeddings(gw):
    client, _, _ = gw
    r = await client.post("/v1/embeddings", headers=auth("weir-acme"),
                          json={"model": "text-embedding-3-small", "input": ["a b c", "d"]})
    assert r.status_code == 200 and len(r.json()["data"]) == 2
    u = await usage(client, "acme")
    assert u["prompt_tokens"] == 4 and u["completion_tokens"] == 0


@pytest.mark.parametrize("body,code", [
    ({"messages": []}, "missing_model"),
    ({"model": "gpt-4o-mini"}, "invalid_messages"),
])
async def test_bad_requests(gw, body, code):
    client, _, _ = gw
    r = await client.post("/v1/chat/completions", headers=auth("weir-acme"), json=body)
    assert r.status_code == 400 and r.json()["error"]["code"] == code


async def test_invalid_json(gw):
    client, _, _ = gw
    r = await client.post("/v1/chat/completions", headers=auth("weir-acme"), content=b"{nope")
    assert r.status_code == 400


# ---------------------------------------------------------------- upstream failures
async def test_upstream_error_passthrough_releases_reservation(gw):
    client, _, _ = gw
    r = await client.post("/v1/chat/completions", headers=auth("weir-acme"),
                          json=chat_body(model="mock-error-500"))
    assert r.status_code == 500
    assert r.json()["error"]["message"] == "mock upstream failure"
    u = await usage(client, "acme")
    assert u["reserved_in_flight"] == 0 and u["used"] == 0


async def test_upstream_unreachable_and_circuit_opens(make_client):
    client, _, _ = await make_client(use_mock=False, upstream_base_url="http://127.0.0.1:1/v1",
                                     upstream_breaker_threshold=2)
    for _ in range(2):
        r = await client.post("/v1/chat/completions", headers=auth("weir-acme"), json=chat_body())
        assert r.status_code == 502
        assert r.json()["error"]["code"] == "upstream_unreachable"
    r = await client.post("/v1/chat/completions", headers=auth("weir-acme"), json=chat_body())
    assert r.status_code == 503 and r.json()["error"]["code"] == "upstream_unavailable"
    assert (await usage(client, "acme"))["reserved_in_flight"] == 0


# ---------------------------------------------------------------- rate limits
async def test_rpm_limit_rejects_without_calling_upstream(gw):
    client, transport, _ = gw
    codes = []
    for _ in range(5):
        r = await client.post("/v1/chat/completions", headers=auth("weir-small"), json=chat_body())
        codes.append(r.status_code)
    assert codes == [200, 200, 200, 429, 429]
    assert len(transport.calls) == 3
    assert r.json()["error"]["code"] == "rate_limit_exceeded"
    assert int(r.headers["retry-after"]) >= 1


async def test_rpm_is_atomic_under_concurrency(gw):
    client, transport, _ = gw
    results = []

    async def one():
        r = await client.post("/v1/chat/completions", headers=auth("weir-small"), json=chat_body())
        results.append(r.status_code)

    async with anyio.create_task_group() as tg:
        for _ in range(40):
            tg.start_soon(one)
    assert results.count(200) == 3
    assert results.count(429) == 37


async def test_tpm_limit(gw):
    client, _, _ = gw
    await client.post("/admin/tenants", headers=auth("admin-secret"),
                      json={"id": "tpm", "rpm": 0, "tpm": 300, "token_budget": 0,
                            "issue_key": False})
    key = (await client.post("/admin/tenants/tpm/keys", headers=auth("admin-secret"))).json()
    h = auth(key["api_key"])
    r1 = await client.post("/v1/chat/completions", headers=h, json=chat_body(max_tokens=200))
    assert r1.status_code == 200
    # Reservation was ~210 but actual usage was ~70, so the unused part is refunded:
    r2 = await client.post("/v1/chat/completions", headers=h, json=chat_body(max_tokens=200))
    assert r2.status_code == 200
    r3 = await client.post("/v1/chat/completions", headers=h, json=chat_body(max_tokens=200))
    r4 = await client.post("/v1/chat/completions", headers=h, json=chat_body(max_tokens=200))
    assert 429 in (r3.status_code, r4.status_code)
    rej = r3 if r3.status_code == 429 else r4
    assert rej.json()["error"]["code"] == "rate_limit_exceeded"


# ---------------------------------------------------------------- budgets
async def _budget_tenant(client, budget, **extra):
    payload = {"id": "b", "rpm": 0, "tpm": 0, "token_budget": budget, **extra}
    r = await client.post("/admin/tenants", headers=auth("admin-secret"), json=payload)
    assert r.status_code == 201, r.text
    return auth(r.json()["api_key"])


async def test_budget_clamps_max_tokens_to_remaining(gw):
    client, transport, _ = gw
    h = await _budget_tenant(client, budget=40)
    r = await client.post("/v1/chat/completions", headers=h, json=chat_body(max_tokens=500))
    assert r.status_code == 200
    granted = int(r.headers["x-weir-max-tokens"])
    assert granted < 40
    assert json.loads(transport.calls[0].content)["max_tokens"] == granted
    assert r.json()["usage"]["completion_tokens"] <= granted


async def test_budget_exhausted(gw):
    client, _, _ = gw
    h = await _budget_tenant(client, budget=40)
    await client.post("/v1/chat/completions", headers=h, json=chat_body(max_tokens=500))
    r = await client.post("/v1/chat/completions", headers=h, json=chat_body(max_tokens=500))
    assert r.status_code == 429
    assert r.json()["error"]["code"] == "insufficient_quota"
    assert int(r.headers["retry-after"]) > 0


async def test_budget_never_overrun_under_concurrent_burst(gw):
    """100 concurrent requests that would each spend ~70 tokens against a 2,000
    token budget. A check-then-charge limiter would let all 100 through (~7,000
    tokens, 250% over budget). Reservations cap spend at the budget, minus
    nothing but prompt-estimate error."""
    client, transport, _ = gw
    budget = 2_000
    h = await _budget_tenant(client, budget=budget)
    statuses = []

    async def one():
        r = await client.post("/v1/chat/completions", headers=h, json=chat_body(max_tokens=64))
        statuses.append(r.status_code)

    async with anyio.create_task_group() as tg:
        for _ in range(100):
            tg.start_soon(one)
    u = await usage(client, "b")
    assert u["reserved_in_flight"] == 0
    assert u["used"] <= budget, u
    assert statuses.count(429) > 50
    assert len(transport.calls) == statuses.count(200)


# ---------------------------------------------------------------- admin
async def test_admin_lifecycle(gw):
    client, _, _ = gw
    a = auth("admin-secret")
    assert (await client.get("/admin/tenants", headers=auth("wrong"))).status_code == 401
    r = await client.post("/admin/tenants", headers=a, json={"id": "newco", "rpm": 10})
    assert r.status_code == 201
    key, key_id = r.json()["api_key"], r.json()["key_id"]
    assert key.startswith("weir-")
    assert (await client.post("/admin/tenants", headers=a, json={"id": "newco"})).status_code == 409
    assert (await client.post("/v1/chat/completions", headers=auth(key),
                              json=chat_body())).status_code == 200

    r = await client.patch("/admin/tenants/newco", headers=a, json={"allowed_models": ["x"]})
    assert r.json()["allowed_models"] == ["x"] and r.json()["rpm"] == 10
    assert (await client.post("/v1/chat/completions", headers=auth(key),
                              json=chat_body())).status_code == 403

    ids = {t["id"] for t in (await client.get("/admin/tenants", headers=a)).json()}
    assert {"newco", "acme", "small"} <= ids
    keys = (await client.get("/admin/tenants/newco/keys", headers=a)).json()
    assert keys[0]["key_id"] == key_id

    assert (await client.delete(f"/admin/tenants/newco/keys/{key_id}",
                                headers=a)).status_code == 204
    r = await client.post("/v1/chat/completions", headers=auth(key), json=chat_body())
    assert r.status_code == 401

    assert (await client.delete("/admin/tenants/newco", headers=a)).status_code == 204
    assert (await client.get("/admin/tenants/newco", headers=a)).status_code == 404


async def test_admin_disabled_without_key(make_client):
    client, _, _ = await make_client(admin_api_key="")
    assert (await client.get("/admin/tenants", headers=auth("x"))).status_code == 404


async def test_admin_edit_of_seed_tenant_survives_restart(make_client):
    client, _, _ = await make_client()
    await client.patch("/admin/tenants/acme", headers=auth("admin-secret"), json={"rpm": 7})
    client2, _, _ = await make_client()
    r = await client2.get("/admin/tenants/acme", headers=auth("admin-secret"))
    assert r.json()["rpm"] == 7


# ---------------------------------------------------------------- redis outage
async def test_fail_open_when_redis_down(make_client, redis_url):
    client, transport, app = await make_client(
        redis_url="redis://127.0.0.1:1/0", redis_breaker_threshold=2)
    # seed tenant still authenticates from the seed file; request still served
    r = await client.post("/v1/chat/completions", headers=auth("weir-acme"), json=chat_body())
    assert r.status_code == 200
    assert r.headers["x-weir-degraded"] == "1"
    assert app.state.services.redis.breaker.state.name == "OPEN"
    ready = await client.get("/ready")
    assert ready.status_code == 200 and ready.json()["status"] == "degraded"
    # ...but not unbounded: the local per-process RPM guard still applies
    codes = [(await client.post("/v1/chat/completions", headers=auth("weir-small"),
                                json=chat_body())).status_code for _ in range(5)]
    assert codes == [200, 200, 200, 429, 429]
    # unknown keys are still rejected
    r = await client.post("/v1/chat/completions", headers=auth("weir-unknown"), json=chat_body())
    assert r.status_code == 503


async def test_fail_closed_when_configured(make_client):
    client, transport, _ = await make_client(redis_url="redis://127.0.0.1:1/0", fail_open=False)
    r = await client.post("/v1/chat/completions", headers=auth("weir-acme"), json=chat_body())
    assert r.status_code == 503
    assert r.json()["error"]["code"] == "governance_unavailable"
    assert transport.calls == []


async def test_recovers_after_redis_returns(make_client, redis_url):
    client, _, app = await make_client(redis_breaker_threshold=1,
                                       redis_breaker_recovery_seconds=0.05)
    app.state.services.redis.breaker.force_open()
    r = await client.post("/v1/chat/completions", headers=auth("weir-acme"), json=chat_body())
    assert r.headers.get("x-weir-degraded") == "1"
    await anyio.sleep(0.06)
    r = await client.post("/v1/chat/completions", headers=auth("weir-acme"), json=chat_body())
    assert "x-weir-degraded" not in r.headers
    assert app.state.services.redis.breaker.state.name == "CLOSED"
    assert redis.Redis.from_url(redis_url).exists("weir:rl:{acme}:rpm")
