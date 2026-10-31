from __future__ import annotations

import json
import secrets
from dataclasses import dataclass

from fastapi import Request

from weir.api.errors import GatewayError
from weir.core.config import Settings
from weir.core.redis import RedisGuard
from weir.services.limiter import Governor
from weir.services.metrics import Metrics
from weir.services.proxy import UpstreamProxy
from weir.services.tenants import Tenant, TenantRegistry


@dataclass
class Services:
    settings: Settings
    metrics: Metrics
    redis: RedisGuard
    tenants: TenantRegistry
    governor: Governor
    upstream: UpstreamProxy


def services(request: Request) -> Services:
    return request.app.state.services


def _extract_key(request: Request) -> str | None:
    auth = request.headers.get("authorization")
    if auth:
        scheme, _, value = auth.partition(" ")
        if scheme.lower() == "bearer" and value.strip():
            return value.strip()
    return request.headers.get("x-api-key") or None


async def current_tenant(request: Request) -> Tenant:
    key = _extract_key(request)
    if not key:
        raise GatewayError(401, "Missing API key. Send 'Authorization: Bearer <gateway key>'.",
                           "invalid_request_error", "missing_api_key")
    tenant = await services(request).tenants.authenticate(key)
    if tenant is None:
        raise GatewayError(401, "Invalid API key.", "invalid_request_error", "invalid_api_key")
    if not tenant.enabled:
        raise GatewayError(403, "Tenant is disabled.", "permission_error", "tenant_disabled")
    return tenant


async def require_admin(request: Request) -> None:
    expected = services(request).settings.admin_api_key
    if not expected:
        raise GatewayError(404, "Admin API is disabled (set ADMIN_API_KEY).",
                           "invalid_request_error", "admin_disabled")
    given = _extract_key(request) or ""
    if not secrets.compare_digest(given.encode(), expected.encode()):
        raise GatewayError(401, "Invalid admin key.", "invalid_request_error", "invalid_admin_key")


async def read_json(request: Request, max_bytes: int) -> dict:
    cl = request.headers.get("content-length")
    if cl and cl.isdigit() and int(cl) > max_bytes:
        raise GatewayError(413, "Request body too large.", "invalid_request_error", "too_large")
    raw = await request.body()
    if len(raw) > max_bytes:
        raise GatewayError(413, "Request body too large.", "invalid_request_error", "too_large")
    try:
        body = json.loads(raw)
    except ValueError as exc:
        raise GatewayError(400, "Body must be valid JSON.", "invalid_request_error",
                           "invalid_json") from exc
    if not isinstance(body, dict):
        raise GatewayError(400, "Body must be a JSON object.", "invalid_request_error",
                           "invalid_json")
    return body
