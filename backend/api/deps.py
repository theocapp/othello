from fastapi import Header, Request

from core.auth import require_write_access


def require_write_access_dep(
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
):
    return require_write_access(request, x_api_key)
