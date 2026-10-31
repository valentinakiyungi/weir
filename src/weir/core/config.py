from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All runtime configuration, read from environment variables (or a .env file)."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Upstream provider (any OpenAI-compatible API) ---
    upstream_base_url: str = "https://api.openai.com/v1"
    upstream_api_key: str = ""
    upstream_connect_timeout: float = 5.0
    upstream_read_timeout: float = 120.0
    upstream_max_connections: int = 4096
    # "fast": built-in lean HTTP/1.1 transport tuned for SSE (default).
    # "httpx": httpx's stock httpcore transport (HTTP/1.1, proxies, env config);
    #          its pool does O(n) work per request, so connections are spread
    #          over `upstream_pool_shards` independent clients.
    upstream_transport: Literal["fast", "httpx"] = "fast"
    upstream_pool_shards: int = 16
    upstream_breaker_threshold: int = 20
    upstream_breaker_recovery_seconds: float = 10.0

    # --- Redis / governance state ---
    redis_url: str = "redis://localhost:6379/0"
    redis_socket_timeout: float = 0.25
    redis_connect_timeout: float = 0.25
    redis_max_connections: int = 256
    redis_breaker_threshold: int = 5
    redis_breaker_recovery_seconds: float = 5.0
    key_prefix: str = "weir"

    # When Redis is unreachable: True = keep serving (fail-open, with a local
    # per-process RPM guard), False = reject with 503 (fail-closed).
    fail_open: bool = True

    # --- Token governance ---
    budget_period: Literal["day", "month"] = "day"
    default_max_tokens: int = 1024
    min_completion_tokens: int = 16
    # Prompt reservations are estimated locally; this multiplier keeps them on
    # the safe side of the provider's real tokenizer so budgets aren't overrun.
    prompt_estimate_margin: float = 1.2
    enforce_max_tokens: bool = True
    inject_stream_usage: bool = True
    max_tokens_field: Literal["max_tokens", "max_completion_tokens"] = "max_tokens"

    # --- Tenants & admin ---
    tenants_file: str | None = None
    admin_api_key: str = ""
    tenant_cache_ttl: float = 10.0

    # --- Misc ---
    log_level: str = "INFO"
    max_request_bytes: int = Field(default=10 * 1024 * 1024)


@lru_cache
def get_settings() -> Settings:
    return Settings()
