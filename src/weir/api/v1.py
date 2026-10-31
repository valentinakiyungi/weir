"""OpenAI-compatible data-plane routes."""

from __future__ import annotations

import json
import logging
import math
import time
import uuid
from collections.abc import AsyncIterator

import anyio
import httpx
from fastapi import APIRouter, Depends, Request, Response
from fastapi.responses import StreamingResponse

from weir.api.deps import Services, current_tenant, read_json, services
from weir.api.errors import GatewayError
from weir.core.logging import log_event
from weir.services.limiter import Decision
from weir.services.proxy import SSEUsageTap, UpstreamError, usage_from_json
from weir.services.tenants import Tenant
from weir.services.tokens import count_text, estimate_chat_prompt, estimate_embedding_input

router = APIRouter(prefix="/v1")
log = logging.getLogger("weir.access")

REJECT_MESSAGES = {
    "rpm_exceeded": ("Request rate limit exceeded for this tenant.", "rate_limit_exceeded"),
    "tpm_exceeded": ("Token-per-minute limit exceeded for this tenant.", "rate_limit_exceeded"),
    "budget_exhausted": ("Token budget for the current period is exhausted.", "insufficient_quota"),
}


def _request_id(request: Request) -> str:
    rid = request.headers.get("x-request-id", "")
    return rid if 0 < len(rid) <= 128 else uuid.uuid4().hex


def _governance_headers(tenant: Tenant, d: Decision, rid: str) -> dict[str, str]:
    h = {"x-request-id": rid, "x-weir-tenant": tenant.id}
    if tenant.rpm:
        h["x-ratelimit-limit-requests"] = str(tenant.rpm)
        if d.rpm_remaining is not None:
            h["x-ratelimit-remaining-requests"] = str(max(0, d.rpm_remaining))
    if tenant.tpm:
        h["x-ratelimit-limit-tokens"] = str(tenant.tpm)
        if d.tpm_remaining is not None:
            h["x-ratelimit-remaining-tokens"] = str(max(0, d.tpm_remaining))
    if d.budget_remaining is not None:
        h["x-weir-budget-remaining"] = str(max(0, d.budget_remaining))
    if d.granted_completion:
        h["x-weir-max-tokens"] = str(d.granted_completion)
    if d.degraded:
        h["x-weir-degraded"] = "1"
    return h


def _reject(tenant: Tenant, d: Decision, rid: str) -> GatewayError:
    msg, code = REJECT_MESSAGES.get(d.reason, ("Request rejected by gateway policy.", d.reason))
    headers = _governance_headers(tenant, d, rid)
    headers["retry-after"] = str(max(1, int(d.retry_after + 0.999)))
    return GatewayError(429, msg, "rate_limit_error" if code != "insufficient_quota" else
                        "insufficient_quota", code, headers)


def _requested_max(body: dict, default: int, field: str) -> tuple[str, int, bool]:
    for f in ("max_completion_tokens", "max_tokens"):
        v = body.get(f)
        if isinstance(v, int) and v > 0:
            return f, v, True
    return field, default, False


@router.post("/chat/completions")
async def chat_completions(request: Request, tenant: Tenant = Depends(current_tenant)):
    svc = services(request)
    s = svc.settings
    t0 = time.perf_counter()
    rid = _request_id(request)
    body = await read_json(request, s.max_request_bytes)

    model = body.get("model")
    if not isinstance(model, str) or not model:
        raise GatewayError(400, "'model' is required.", "invalid_request_error", "missing_model")
    if not tenant.model_allowed(model):
        raise GatewayError(403, f"Model '{model}' is not allowed for this tenant.",
                           "permission_error", "model_not_allowed")
    if not isinstance(body.get("messages"), list):
        raise GatewayError(400, "'messages' must be a list.", "invalid_request_error",
                           "invalid_messages")

    stream = bool(body.get("stream"))
    field, requested, explicit = _requested_max(body, s.default_max_tokens, s.max_tokens_field)
    if tenant.max_tokens_cap:
        requested = min(requested, tenant.max_tokens_cap)
    prompt_est = math.ceil(estimate_chat_prompt(body) * s.prompt_estimate_margin)

    decision = await svc.governor.admit(tenant, prompt_est, requested)
    if not decision.allowed:
        svc.metrics.requests.labels(tenant.id, "chat", "429").inc()
        raise _reject(tenant, decision, rid)

    if explicit or s.enforce_max_tokens or decision.granted_completion < requested:
        body[field] = decision.granted_completion
    if stream and s.inject_stream_usage:
        opts = body.get("stream_options") if isinstance(body.get("stream_options"), dict) else {}
        body["stream_options"] = {**opts, "include_usage": True}

    svc.metrics.governance_seconds.observe(time.perf_counter() - t0)
    return await _forward(svc, tenant, decision, rid, "chat/completions", body, stream, "chat", t0,
                          model)


@router.post("/embeddings")
async def embeddings(request: Request, tenant: Tenant = Depends(current_tenant)):
    svc = services(request)
    t0 = time.perf_counter()
    rid = _request_id(request)
    body = await read_json(request, svc.settings.max_request_bytes)
    model = body.get("model")
    if not isinstance(model, str) or not model:
        raise GatewayError(400, "'model' is required.", "invalid_request_error", "missing_model")
    if not tenant.model_allowed(model):
        raise GatewayError(403, f"Model '{model}' is not allowed for this tenant.",
                           "permission_error", "model_not_allowed")
    prompt_est = math.ceil(estimate_embedding_input(body) * svc.settings.prompt_estimate_margin)
    decision = await svc.governor.admit(tenant, prompt_est, 0)
    if not decision.allowed:
        svc.metrics.requests.labels(tenant.id, "embeddings", "429").inc()
        raise _reject(tenant, decision, rid)
    svc.metrics.governance_seconds.observe(time.perf_counter() - t0)
    return await _forward(svc, tenant, decision, rid, "embeddings", body, False, "embeddings", t0,
                          model)


@router.get("/models")
async def models(request: Request, tenant: Tenant = Depends(current_tenant)):
    svc = services(request)
    rid = _request_id(request)
    try:
        resp = await svc.upstream.send("GET", "models", rid)
    except UpstreamError as e:
        raise GatewayError(e.status, e.message, "api_error", e.code) from e
    content = await resp.aread()
    await resp.aclose()
    if resp.status_code == 200 and tenant.allowed_models:
        try:
            obj = json.loads(content)
            obj["data"] = [m for m in obj.get("data", []) if m.get("id") in tenant.allowed_models]
            content = json.dumps(obj).encode()
        except (ValueError, AttributeError):
            pass
    return Response(content, status_code=resp.status_code,
                    media_type=resp.headers.get("content-type", "application/json"),
                    headers={"x-request-id": rid})


async def _forward(svc: Services, tenant: Tenant, decision: Decision, rid: str, path: str,
                   body: dict, stream: bool, route: str, t0: float, model: str) -> Response:
    try:
        resp = await svc.upstream.send("POST", path, rid, json_body=body)
    except UpstreamError as e:
        await svc.governor.settle(tenant, decision, 0, 0)
        svc.metrics.requests.labels(tenant.id, route, str(e.status)).inc()
        raise GatewayError(e.status, e.message, "api_error", e.code,
                           _governance_headers(tenant, decision, rid)) from e

    headers = _governance_headers(tenant, decision, rid)
    ctype = resp.headers.get("content-type", "application/json")

    if stream and resp.status_code == 200 and "text/event-stream" in ctype:
        headers.update({"cache-control": "no-cache", "x-accel-buffering": "no"})
        return StreamingResponse(
            _passthrough(svc, tenant, decision, resp, rid, route, t0, model),
            status_code=200, media_type="text/event-stream", headers=headers,
        )

    # Non-streaming (or error) response: small, so read it fully.
    try:
        content = await resp.aread()
    finally:
        await resp.aclose()
    if resp.status_code >= 400:
        await svc.governor.settle(tenant, decision, 0, 0)
        prompt, completion = 0, 0
    else:
        obj, usage = usage_from_json(content)
        if usage is None:  # provider didn't report usage: fall back to estimates
            usage = (decision.prompt_estimate, _estimate_completion(obj))
        prompt, completion = usage
        await svc.governor.settle(tenant, decision, prompt, completion)
    svc.metrics.requests.labels(tenant.id, route, str(resp.status_code)).inc()
    _access_log(tenant, rid, route, model, resp.status_code, prompt, completion, t0, False,
                decision)
    return Response(content, status_code=resp.status_code, media_type=ctype, headers=headers)


async def _passthrough(svc: Services, tenant: Tenant, decision: Decision, resp: httpx.Response,
                       rid: str, route: str, t0: float, model: str) -> AsyncIterator[bytes]:
    """Forward upstream bytes as they arrive. Nothing is accumulated except the
    current partial SSE line inside the tap."""
    tap = SSEUsageTap()
    svc.metrics.active_streams.inc()
    status = "200"
    try:
        try:
            async for chunk in resp.aiter_raw():
                tap.feed(chunk)
                yield chunk
        except httpx.HTTPError as exc:
            status = "502"
            err = {"error": {"message": f"Upstream stream interrupted: {exc.__class__.__name__}",
                             "type": "api_error", "code": "upstream_stream_error"}}
            yield f"data: {json.dumps(err)}\n\n".encode()
    except BaseException:
        status = "499"  # client went away / cancelled
        raise
    finally:
        svc.metrics.active_streams.dec()
        # Shield: on client disconnect this scope is cancelled, but the upstream
        # connection must still be released and the usage still billed.
        with anyio.CancelScope(shield=True):
            await resp.aclose()
            tap.close()
            prompt, completion = tap.usage or (decision.prompt_estimate, tap.events)
            await svc.governor.settle(tenant, decision, prompt, completion)
            svc.metrics.requests.labels(tenant.id, route, status).inc()
            _access_log(tenant, rid, route, model, int(status), prompt, completion, t0, True,
                        decision, exact=tap.usage is not None)


def _estimate_completion(obj: dict | None) -> int:
    if not obj:
        return 0
    total = 0
    for ch in obj.get("choices") or []:
        msg = ch.get("message") or {}
        total += count_text(str(msg.get("content") or ch.get("text") or ""))
    return total


def _access_log(tenant: Tenant, rid: str, route: str, model: str, status: int, prompt: int,
                completion: int, t0: float, stream: bool, d: Decision, exact: bool = True) -> None:
    log_event(log, "request", tenant=tenant.id, request_id=rid, route=route, model=model,
              status=status, stream=stream, prompt_tokens=prompt, completion_tokens=completion,
              usage_exact=exact, reserved=d.reservation, degraded=d.degraded,
              duration_ms=round((time.perf_counter() - t0) * 1000, 1))
