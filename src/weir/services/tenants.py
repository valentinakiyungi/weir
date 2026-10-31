"""Tenant registry: gateway-issued API keys -> tenant policy.

Keys are never stored in plaintext; only their SHA-256 digest is persisted.
Lookups are served from a short-TTL in-process cache, then Redis, then (if
Redis is down) the stale cache or the seed file, so authentication keeps
working for known tenants during a Redis outage.
"""

from __future__ import annotations

import hashlib
import json
import logging
import secrets
import time

import anyio
from pydantic import BaseModel, Field

from weir.core.config import Settings
from weir.core.logging import log_event
from weir.core.redis import RedisGuard, RedisUnavailable

log = logging.getLogger("weir.tenants")

TENANT_ID_PATTERN = r"^[a-z0-9][a-z0-9_-]{0,62}$"


class TenantPolicy(BaseModel):
    """Mutable policy fields (all optional for PATCH)."""

    name: str | None = None
    enabled: bool | None = None
    rpm: int | None = Field(default=None, ge=0, description="Requests per minute, 0 = unlimited")
    tpm: int | None = Field(default=None, ge=0, description="Tokens per minute, 0 = unlimited")
    token_budget: int | None = Field(
        default=None, ge=0, description="Tokens per budget period (day/month), 0 = unlimited"
    )
    max_tokens_cap: int | None = Field(default=None, ge=1)
    allowed_models: list[str] | None = None


class Tenant(BaseModel):
    id: str = Field(pattern=TENANT_ID_PATTERN)
    name: str = ""
    enabled: bool = True
    rpm: int = Field(default=60, ge=0)
    tpm: int = Field(default=100_000, ge=0)
    token_budget: int = Field(default=1_000_000, ge=0)
    max_tokens_cap: int | None = Field(default=None, ge=1)
    allowed_models: list[str] = Field(default_factory=list)

    def model_allowed(self, model: str) -> bool:
        return not self.allowed_models or model in self.allowed_models


def hash_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode()).hexdigest()


def generate_key() -> str:
    return "weir-" + secrets.token_urlsafe(32)


class TenantRegistry:
    NEGATIVE_TTL = 2.0
    MAX_CACHE = 50_000

    def __init__(self, settings: Settings, redis: RedisGuard) -> None:
        self.settings = settings
        self.redis = redis
        self.p = settings.key_prefix
        self._cache: dict[str, tuple[Tenant | None, float]] = {}
        self._seed_by_hash: dict[str, Tenant] = {}

    # ---------- key helpers ----------
    def _k_tenant(self, tid: str) -> str:
        return f"{self.p}:tenant:{tid}"

    def _k_key(self, h: str) -> str:
        return f"{self.p}:key:{h}"

    def _k_tenant_keys(self, tid: str) -> str:
        return f"{self.p}:tenant_keys:{tid}"

    @property
    def _k_index(self) -> str:
        return f"{self.p}:tenants"

    # ---------- seed ----------
    async def load_seed(self) -> int:
        path = self.settings.tenants_file
        if not path:
            return 0
        data = json.loads(await anyio.Path(path).read_text())
        count = 0
        for raw in data.get("tenants", []):
            keys = raw.pop("api_keys", [])
            tenant = Tenant(**raw)
            for k in keys:
                self._seed_by_hash[hash_key(k)] = tenant
            try:
                # NX: an admin edit made at runtime survives restarts.
                await self.redis.call("seed", lambda r, t=tenant: r.set(
                    self._k_tenant(t.id), t.model_dump_json(), nx=True))
                await self.redis.call("seed", lambda r, t=tenant: r.sadd(self._k_index, t.id))
                for k in keys:
                    await self._store_key(tenant.id, hash_key(k))
            except RedisUnavailable:
                log_event(log, "seed tenant not persisted (redis unavailable)", logging.WARNING,
                          tenant=tenant.id)
            count += 1
        log_event(log, "seed tenants loaded", count=count)
        return count

    # ---------- lookup ----------
    async def authenticate(self, api_key: str) -> Tenant | None:
        """Return the tenant for a key, None if unknown. Raises RedisUnavailable
        only if the key cannot be resolved from any source."""
        h = hash_key(api_key)
        now = time.monotonic()
        cached = self._cache.get(h)
        if cached and cached[1] > now:
            return cached[0]
        try:
            tenant = await self._fetch(h)
        except RedisUnavailable:
            if cached:  # stale but better than nothing
                return cached[0]
            if h in self._seed_by_hash:
                return self._seed_by_hash[h]
            raise
        ttl = self.settings.tenant_cache_ttl if tenant else self.NEGATIVE_TTL
        if len(self._cache) >= self.MAX_CACHE:
            self._cache.clear()
        self._cache[h] = (tenant, now + ttl)
        return tenant

    async def _fetch(self, h: str) -> Tenant | None:
        raw = await self.redis.call("key_lookup", lambda r: r.get(self._k_key(h)))
        if not raw:
            return None
        tid = json.loads(raw)["tenant_id"]
        return await self.get(tid)

    def invalidate(self) -> None:
        self._cache.clear()

    # ---------- admin CRUD ----------
    async def get(self, tenant_id: str) -> Tenant | None:
        raw = await self.redis.call("tenant_get", lambda r: r.get(self._k_tenant(tenant_id)))
        return Tenant.model_validate_json(raw) if raw else None

    async def list(self) -> list[Tenant]:
        ids = sorted(await self.redis.call("tenant_list", lambda r: r.smembers(self._k_index)))
        if not ids:
            return []
        raws = await self.redis.call(
            "tenant_list", lambda r: r.mget([self._k_tenant(i) for i in ids]))
        return [Tenant.model_validate_json(x) for x in raws if x]

    async def create(self, tenant: Tenant) -> bool:
        created = await self.redis.call("tenant_create", lambda r: r.set(
            self._k_tenant(tenant.id), tenant.model_dump_json(), nx=True))
        if created:
            await self.redis.call("tenant_create", lambda r: r.sadd(self._k_index, tenant.id))
        return bool(created)

    async def save(self, tenant: Tenant) -> None:
        await self.redis.call("tenant_save", lambda r: r.set(
            self._k_tenant(tenant.id), tenant.model_dump_json()))
        self.invalidate()

    async def delete(self, tenant_id: str) -> bool:
        hashes = await self.redis.call(
            "tenant_delete", lambda r: r.smembers(self._k_tenant_keys(tenant_id)))

        async def _del(r):
            pipe = r.pipeline(transaction=True)
            for h in hashes:
                pipe.delete(self._k_key(h))
            pipe.delete(self._k_tenant_keys(tenant_id))
            pipe.srem(self._k_index, tenant_id)
            pipe.delete(self._k_tenant(tenant_id))
            return await pipe.execute()

        res = await self.redis.call("tenant_delete", _del)
        self._seed_by_hash = {h: t for h, t in self._seed_by_hash.items() if t.id != tenant_id}
        self.invalidate()
        return bool(res[-1])

    async def _store_key(self, tenant_id: str, h: str) -> None:
        record = json.dumps(
            {"tenant_id": tenant_id, "key_id": h[:12], "created_at": int(time.time())})

        async def _do(r):
            pipe = r.pipeline(transaction=True)
            pipe.set(self._k_key(h), record)
            pipe.sadd(self._k_tenant_keys(tenant_id), h)
            return await pipe.execute()

        await self.redis.call("key_store", _do)

    async def issue_key(self, tenant_id: str) -> tuple[str, str]:
        key = generate_key()
        h = hash_key(key)
        await self._store_key(tenant_id, h)
        return key, h[:12]

    async def list_keys(self, tenant_id: str) -> list[dict]:
        hashes = await self.redis.call(
            "key_list", lambda r: r.smembers(self._k_tenant_keys(tenant_id)))
        if not hashes:
            return []
        raws = await self.redis.call("key_list", lambda r: r.mget([self._k_key(h) for h in hashes]))
        return sorted((json.loads(x) for x in raws if x), key=lambda d: d["created_at"])

    async def revoke_key(self, tenant_id: str, key_id: str) -> bool:
        hashes = await self.redis.call(
            "key_revoke", lambda r: r.smembers(self._k_tenant_keys(tenant_id)))
        matches = [h for h in hashes if h.startswith(key_id)]
        if len(matches) != 1:
            return False
        h = matches[0]

        async def _do(r):
            pipe = r.pipeline(transaction=True)
            pipe.delete(self._k_key(h))
            pipe.srem(self._k_tenant_keys(tenant_id), h)
            return await pipe.execute()

        await self.redis.call("key_revoke", _do)
        self._seed_by_hash.pop(h, None)
        self._cache.pop(h, None)
        self.invalidate()
        return True
