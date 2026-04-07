import ipaddress
from threading import Lock

from fastapi import Header, HTTPException, Request

from core.config import ADMIN_API_KEY


def _request_is_internal(request: Request) -> bool:
    client_host = (request.client.host if request.client else "") or ""
    if not client_host:
        return False
    if client_host == "localhost":
        return True
    try:
        parsed = ipaddress.ip_address(client_host)
    except ValueError:
        return False
    return parsed.is_loopback


def require_write_access(
    request: Request, x_api_key: str | None = Header(default=None, alias="X-API-Key")
) -> None:
    if _request_is_internal(request):
        return
    if ADMIN_API_KEY and x_api_key == ADMIN_API_KEY:
        return
    detail = "Write access requires an internal client or a valid X-API-Key."
    if not ADMIN_API_KEY:
        detail = (
            f"{detail} Set OTHELLO_ADMIN_API_KEY to enable authenticated remote access."
        )
    raise HTTPException(status_code=403, detail=detail)


def run_exclusive(lock: Lock, label: str, fn):
    if not lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail=f"{label} already in progress.")
    try:
        return fn()
    finally:
        lock.release()


def run_exclusive_or_skip(lock: Lock, label: str, fn):
    if not lock.acquire(blocking=False):
        print(
            f"[{label}] Skipping scheduled run because another {label} is already in progress."
        )
        return {"status": "skipped", "reason": f"{label} already in progress"}
    try:
        return fn()
    finally:
        lock.release()
