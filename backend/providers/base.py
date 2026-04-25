import os
import time
import requests

_provider_cooldowns: dict[str, float] = {}
_provider_last_request_at: dict[str, float] = {}
_provider_failures: dict[str, int] = {}
_http = None


def get_http_session():
    global _http
    if _http is None:
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": "OthelloV2/1.0 (+local intelligence dashboard)",
                "Accept": "application/json,text/plain,*/*",
            }
        )
        _http = session
    return _http


def _cooldown_active(provider: str) -> bool:
    return _provider_cooldowns.get(provider, 0) > time.time()


def _set_provider_cooldown(provider: str, seconds: int) -> None:
    _provider_cooldowns[provider] = max(
        _provider_cooldowns.get(provider, 0), time.time() + max(seconds, 0)
    )


def _mark_provider_success(provider: str) -> None:
    _provider_failures.pop(provider, None)
    _provider_cooldowns.pop(provider, None)


def _mark_provider_failure(provider: str) -> int:
    failures = _provider_failures.get(provider, 0) + 1
    _provider_failures[provider] = failures
    return failures


def _respect_provider_min_interval(provider: str, default_seconds: float) -> None:
    env_name = f"OTHELLO_{provider.upper()}_MIN_INTERVAL_SECONDS"
    min_interval = float(os.getenv(env_name, str(default_seconds)))
    last_request_at = _provider_last_request_at.get(provider)
    if last_request_at is not None:
        wait_seconds = min_interval - (time.time() - last_request_at)
        if wait_seconds > 0:
            time.sleep(wait_seconds)
    _provider_last_request_at[provider] = time.time()


def _is_rate_limit_error(exc: Exception | str) -> bool:
    message = str(exc).lower()
    return any(
        token in message
        for token in [
            "rate limit",
            "ratelimit",
            "too many requests",
            "429",
            "please limit requests to one every 5 seconds",
        ]
    )


def _is_timeout_error(exc: Exception | str) -> bool:
    message = str(exc).lower()
    return any(
        token in message
        for token in ["timed out", "timeout", "connect timeout", "read timeout"]
    )
