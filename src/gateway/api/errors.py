from __future__ import annotations

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from gateway.core.redis import RedisUnavailable
from gateway.services.limiter import GovernanceUnavailable


class GatewayError(Exception):
    """Rendered as an OpenAI-compatible error body so existing SDKs surface it."""

    def __init__(self, status: int, message: str, type_: str, code: str | None = None,
                 headers: dict[str, str] | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.message = message
        self.type = type_
        self.code = code
        self.headers = headers or {}


def error_body(message: str, type_: str, code: str | None) -> dict:
    return {"error": {"message": message, "type": type_, "param": None, "code": code}}


async def gateway_error_handler(_: Request, exc: GatewayError) -> JSONResponse:
    return JSONResponse(error_body(exc.message, exc.type, exc.code), status_code=exc.status,
                        headers=exc.headers)


async def redis_unavailable_handler(_: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(
        error_body("Governance store unavailable; try again shortly.", "service_unavailable",
                   "governance_unavailable"),
        status_code=503, headers={"retry-after": "5"},
    )


async def validation_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(error_body(str(exc.errors()), "invalid_request_error", "invalid_body"),
                        status_code=422)


def register(app) -> None:
    app.add_exception_handler(GatewayError, gateway_error_handler)
    app.add_exception_handler(RedisUnavailable, redis_unavailable_handler)
    app.add_exception_handler(GovernanceUnavailable, redis_unavailable_handler)
    app.add_exception_handler(RequestValidationError, validation_handler)
