"""Atomic multi-tenant admission control and token accounting.

Every request goes through two Redis round trips, each a single Lua script
(so concurrent gateway instances can never race each other):

  admit()  - before calling upstream. Atomically checks the tenant's
             requests-per-minute bucket, tokens-per-minute bucket and the
             period token budget, and *reserves* the worst-case cost
             (estimated prompt tokens + max completion tokens). If the budget
             can't cover the requested max_tokens but can cover a smaller
             completion, the grant is reduced and the gateway rewrites
             max_tokens, so the upstream physically cannot overspend.

  settle() - after the response (or a disconnect). Releases the reservation,
             charges the actual usage reported by the provider, refunds the
             unused part of the TPM reservation, and records usage counters.

All keys for one tenant share a Redis Cluster hash tag ({tenant}) so the
scripts are cluster-safe. Time comes from Redis TIME so instances with skewed
clocks still agree.
"""

from __future__ import annotations

import calendar
import logging
import math
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime

from weir.core.config import Settings
from weir.core.logging import log_event
from weir.core.redis import RedisGuard, RedisUnavailable
from weir.services.metrics import Metrics
from weir.services.tenants import Tenant

log = logging.getLogger("weir.limiter")

ADMIT_LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local rpm = tonumber(ARGV[1])
local tpm = tonumber(ARGV[2])
local budget = tonumber(ARGV[3])
local prompt = tonumber(ARGV[4])
local completion = tonumber(ARGV[5])
local min_completion = tonumber(ARGV[6])
local ttl = tonumber(ARGV[7])

local function refill(key, cap)
  local d = redis.call('HMGET', key, 't', 'ts')
  local tokens = tonumber(d[1])
  local ts = tonumber(d[2])
  if tokens == nil or ts == nil then return cap end
  local elapsed = now - ts
  if elapsed < 0 then elapsed = 0 end
  local v = tokens + elapsed * cap / 60000.0
  if v > cap then v = cap end
  return v
end

-- 1) period budget: decides how many completion tokens can be granted
local b = redis.call('HMGET', KEYS[3], 'used', 'reserved')
local used = tonumber(b[1]) or 0
local reserved = tonumber(b[2]) or 0
local budget_remaining = -1
if budget > 0 then
  budget_remaining = budget - used - reserved
  if budget_remaining < prompt + min_completion then
    return {0, 'budget_exhausted', -1, 0, -1, -1, math.max(budget_remaining, 0)}
  end
  if prompt + completion > budget_remaining then
    completion = budget_remaining - prompt
  end
end
local cost = prompt + completion

-- 2) requests per minute (token bucket, capacity = rpm, refill rpm/min)
local rpm_tokens = -1
if rpm > 0 then
  rpm_tokens = refill(KEYS[1], rpm)
  if rpm_tokens < 1 then
    return {0, 'rpm_exceeded', math.ceil((1 - rpm_tokens) * 60000 / rpm), completion,
            0, -1, budget_remaining}
  end
end

-- 3) tokens per minute (a request bigger than the bucket is admitted only when
--    the bucket is full, then drives it negative = back-pressure)
local tpm_tokens = -1
if tpm > 0 then
  tpm_tokens = refill(KEYS[2], tpm)
  local need = math.min(cost, tpm)
  if tpm_tokens < need then
    return {0, 'tpm_exceeded', math.ceil((need - tpm_tokens) * 60000 / tpm), completion,
            math.floor(rpm_tokens), math.floor(tpm_tokens), budget_remaining}
  end
end

-- commit (only reached when every check passed)
if rpm > 0 then
  rpm_tokens = rpm_tokens - 1
  redis.call('HSET', KEYS[1], 't', rpm_tokens, 'ts', now)
  redis.call('PEXPIRE', KEYS[1], 120000)
end
if tpm > 0 then
  tpm_tokens = tpm_tokens - cost
  redis.call('HSET', KEYS[2], 't', tpm_tokens, 'ts', now)
  redis.call('PEXPIRE', KEYS[2], 120000)
end
redis.call('HINCRBY', KEYS[3], 'reserved', cost)
redis.call('EXPIRE', KEYS[3], ttl)
if budget_remaining >= 0 then budget_remaining = budget_remaining - cost end
return {1, 'ok', 0, completion, math.floor(rpm_tokens), math.floor(tpm_tokens), budget_remaining}
"""

SETTLE_LUA = """
local reserved = tonumber(ARGV[1])
local prompt = tonumber(ARGV[2])
local completion = tonumber(ARGV[3])
local tpm = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])
local actual = prompt + completion

local r = redis.call('HINCRBY', KEYS[2], 'reserved', -reserved)
if r < 0 then redis.call('HSET', KEYS[2], 'reserved', 0) end
redis.call('HINCRBY', KEYS[2], 'used', actual)
redis.call('HINCRBY', KEYS[2], 'requests', 1)
redis.call('HINCRBY', KEYS[2], 'prompt_tokens', prompt)
redis.call('HINCRBY', KEYS[2], 'completion_tokens', completion)
redis.call('EXPIRE', KEYS[2], ttl)

if tpm > 0 then
  local cur = tonumber(redis.call('HGET', KEYS[1], 't'))
  if cur ~= nil then
    local v = cur + (reserved - actual)
    if v > tpm then v = tpm end
    redis.call('HSET', KEYS[1], 't', v)
  end
end
return 1
"""


@dataclass
class Decision:
    allowed: bool
    reason: str = "ok"
    retry_after: float = 0.0
    granted_completion: int = 0
    prompt_estimate: int = 0
    reservation: int = 0
    rpm_remaining: int | None = None
    tpm_remaining: int | None = None
    budget_remaining: int | None = None
    period: str = ""
    degraded: bool = False
    settled: bool = field(default=False, repr=False)


class GovernanceUnavailable(Exception):
    pass


class _LocalBucket:
    """Per-process RPM guard used only while Redis is unavailable (fail-open
    is not fail-unbounded)."""

    def __init__(self) -> None:
        self._state: dict[str, tuple[float, float]] = {}

    def take(self, key: str, rpm: int) -> bool:
        if rpm <= 0:
            return True
        now = time.monotonic()
        tokens, ts = self._state.get(key, (float(rpm), now))
        tokens = min(float(rpm), tokens + (now - ts) * rpm / 60.0)
        if tokens < 1:
            self._state[key] = (tokens, now)
            return False
        self._state[key] = (tokens - 1, now)
        return True


class Governor:
    def __init__(self, settings: Settings, redis: RedisGuard, metrics: Metrics) -> None:
        self.settings = settings
        self.redis = redis
        self.metrics = metrics
        self.p = settings.key_prefix
        self._admit = redis.client.register_script(ADMIT_LUA)
        self._settle = redis.client.register_script(SETTLE_LUA)
        self._local = _LocalBucket()

    # ----- period helpers -----
    def period_id(self, now: datetime | None = None) -> str:
        now = now or datetime.now(UTC)
        return now.strftime("%Y%m%d" if self.settings.budget_period == "day" else "%Y%m")

    def seconds_until_reset(self, now: datetime | None = None) -> int:
        now = now or datetime.now(UTC)
        if self.settings.budget_period == "day":
            end = datetime(now.year, now.month, now.day, tzinfo=UTC).timestamp() + 86400
        else:
            days = calendar.monthrange(now.year, now.month)[1]
            end = datetime(now.year, now.month, 1, tzinfo=UTC).timestamp() + days * 86400
        return max(1, math.ceil(end - now.timestamp()))

    def _budget_ttl(self) -> int:
        keep = 7 * 86400 if self.settings.budget_period == "day" else 62 * 86400
        return self.seconds_until_reset() + keep

    def keys(self, tenant_id: str, period: str) -> tuple[str, str, str]:
        tag = "{" + tenant_id + "}"
        return (
            f"{self.p}:rl:{tag}:rpm",
            f"{self.p}:rl:{tag}:tpm",
            f"{self.p}:budget:{tag}:{period}",
        )

    # ----- admission -----
    async def admit(self, tenant: Tenant, prompt_estimate: int,
                    completion_request: int) -> Decision:
        period = self.period_id()
        rpm_key, tpm_key, budget_key = self.keys(tenant.id, period)
        args = [
            tenant.rpm,
            tenant.tpm,
            tenant.token_budget,
            prompt_estimate,
            completion_request,
            self.settings.min_completion_tokens,
            self._budget_ttl(),
        ]
        try:
            res = await self.redis.call(
                "admit",
                lambda r: self._admit(keys=[rpm_key, tpm_key, budget_key], args=args, client=r),
            )
        except RedisUnavailable as exc:
            return self._degraded(tenant, prompt_estimate, completion_request, period, exc)

        allowed, reason, retry_ms, granted, rpm_rem, tpm_rem, budget_rem = res
        reason = reason if isinstance(reason, str) else reason.decode()
        d = Decision(
            allowed=bool(allowed),
            reason=reason,
            granted_completion=int(granted),
            prompt_estimate=prompt_estimate,
            reservation=prompt_estimate + int(granted) if allowed else 0,
            rpm_remaining=None if rpm_rem < 0 else int(rpm_rem),
            tpm_remaining=None if tenant.tpm == 0 else int(tpm_rem),
            budget_remaining=None if budget_rem < 0 else int(budget_rem),
            period=period,
        )
        if reason == "budget_exhausted":
            d.retry_after = float(self.seconds_until_reset())
        else:
            d.retry_after = max(0.0, retry_ms / 1000.0)
        if not d.allowed:
            self.metrics.rejections.labels(tenant.id, reason).inc()
        elif d.granted_completion < completion_request:
            self.metrics.clamped.labels(tenant.id).inc()
        return d

    def _degraded(self, tenant: Tenant, prompt: int, completion: int, period: str,
                  exc: Exception) -> Decision:
        if not self.settings.fail_open:
            raise GovernanceUnavailable(str(exc)) from exc
        if not self._local.take(tenant.id, tenant.rpm):
            self.metrics.rejections.labels(tenant.id, "rpm_exceeded_local").inc()
            return Decision(allowed=False, reason="rpm_exceeded", retry_after=1.0,
                            prompt_estimate=prompt, period=period, degraded=True)
        self.metrics.degraded.inc()
        return Decision(allowed=True, reason="degraded", granted_completion=completion,
                        prompt_estimate=prompt, reservation=prompt + completion,
                        period=period, degraded=True)

    # ----- settlement -----
    async def settle(self, tenant: Tenant, decision: Decision, prompt_tokens: int,
                     completion_tokens: int) -> None:
        if decision.settled or not decision.allowed:
            return
        decision.settled = True
        self.metrics.tokens.labels(tenant.id, "prompt").inc(prompt_tokens)
        self.metrics.tokens.labels(tenant.id, "completion").inc(completion_tokens)
        if decision.degraded:
            return  # nothing was reserved in Redis
        _, tpm_key, budget_key = self.keys(tenant.id, decision.period)
        args = [decision.reservation, prompt_tokens, completion_tokens, tenant.tpm,
                self._budget_ttl()]
        try:
            await self.redis.call(
                "settle",
                lambda r: self._settle(keys=[tpm_key, budget_key], args=args, client=r),
            )
        except RedisUnavailable as exc:
            log_event(log, "settlement lost", logging.ERROR, tenant=tenant.id,
                      reservation=decision.reservation, prompt=prompt_tokens,
                      completion=completion_tokens, error=str(exc))

    # ----- reporting -----
    async def usage(self, tenant: Tenant, period: str | None = None) -> dict:
        period = period or self.period_id()
        _, _, budget_key = self.keys(tenant.id, period)
        raw = await self.redis.call("usage", lambda r: r.hgetall(budget_key))
        vals = {k: int(v) for k, v in raw.items()}
        used = vals.get("used", 0)
        reserved = vals.get("reserved", 0)
        return {
            "tenant": tenant.id,
            "period": period,
            "budget_period": self.settings.budget_period,
            "token_budget": tenant.token_budget,
            "used": used,
            "reserved_in_flight": reserved,
            "remaining": (max(0, tenant.token_budget - used - reserved)
                          if tenant.token_budget else None),
            "requests": vals.get("requests", 0),
            "prompt_tokens": vals.get("prompt_tokens", 0),
            "completion_tokens": vals.get("completion_tokens", 0),
            "resets_in_seconds": self.seconds_until_reset() if period == self.period_id() else None,
        }
