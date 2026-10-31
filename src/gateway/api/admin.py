"""Control-plane API for tenant and key management. Protected by ADMIN_API_KEY."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from gateway.api.deps import require_admin, services
from gateway.api.errors import GatewayError
from gateway.services.tenants import Tenant, TenantPolicy

router = APIRouter(prefix="/admin", dependencies=[Depends(require_admin)], tags=["admin"])


class TenantCreate(Tenant):
    issue_key: bool = True


class CreatedTenant(BaseModel):
    tenant: Tenant
    api_key: str | None = None
    key_id: str | None = None


def _not_found(tid: str) -> GatewayError:
    return GatewayError(404, f"Tenant '{tid}' not found.", "invalid_request_error",
                        "tenant_not_found")


async def _get(request: Request, tid: str) -> Tenant:
    t = await services(request).tenants.get(tid)
    if t is None:
        raise _not_found(tid)
    return t


@router.post("/tenants", status_code=201, response_model=CreatedTenant)
async def create_tenant(payload: TenantCreate, request: Request):
    reg = services(request).tenants
    tenant = Tenant(**payload.model_dump(exclude={"issue_key"}))
    if not await reg.create(tenant):
        raise GatewayError(409, f"Tenant '{tenant.id}' already exists.", "invalid_request_error",
                           "tenant_exists")
    out = CreatedTenant(tenant=tenant)
    if payload.issue_key:
        out.api_key, out.key_id = await reg.issue_key(tenant.id)
    return out


@router.get("/tenants", response_model=list[Tenant])
async def list_tenants(request: Request):
    return await services(request).tenants.list()


@router.get("/tenants/{tid}", response_model=Tenant)
async def get_tenant(tid: str, request: Request):
    return await _get(request, tid)


@router.patch("/tenants/{tid}", response_model=Tenant)
async def update_tenant(tid: str, patch: TenantPolicy, request: Request):
    t = await _get(request, tid)
    updated = t.model_copy(update=patch.model_dump(exclude_unset=True))
    updated = Tenant.model_validate(updated.model_dump())
    await services(request).tenants.save(updated)
    return updated


@router.delete("/tenants/{tid}", status_code=204)
async def delete_tenant(tid: str, request: Request):
    if not await services(request).tenants.delete(tid):
        raise _not_found(tid)


@router.post("/tenants/{tid}/keys", status_code=201)
async def issue_key(tid: str, request: Request):
    await _get(request, tid)
    key, key_id = await services(request).tenants.issue_key(tid)
    return {"api_key": key, "key_id": key_id}


@router.get("/tenants/{tid}/keys")
async def list_keys(tid: str, request: Request):
    await _get(request, tid)
    return await services(request).tenants.list_keys(tid)


@router.delete("/tenants/{tid}/keys/{key_id}", status_code=204)
async def revoke_key(tid: str, key_id: str, request: Request):
    if not await services(request).tenants.revoke_key(tid, key_id):
        raise GatewayError(404, "Key not found (or key_id prefix is ambiguous).",
                           "invalid_request_error", "key_not_found")


@router.get("/tenants/{tid}/usage")
async def usage(tid: str, request: Request, period: str | None = None):
    t = await _get(request, tid)
    return await services(request).governor.usage(t, period)


@router.post("/cache/invalidate", status_code=204)
async def invalidate(request: Request):
    services(request).tenants.invalidate()
