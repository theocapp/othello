import logging
import os
import sys
import threading

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes.analytics import router as analytics_router
from api.routes.briefings import router as briefings_router
from api.routes.entities import router as entities_router
from api.routes.events import router as events_router
from api.routes.headlines import router as headlines_router
from api.routes.health import router as health_router
from api.routes.query import router as query_router
from bootstrap import initialize_runtime
from core.config import CORS_ORIGINS, INTERNAL_SCHEDULER_ENABLED
from core.scheduler import build_scheduler, schedule_initial_analytics_warm


def _log_runtime_environment() -> None:
    certifi_path = None
    requests_ca_path = None
    try:
        import certifi

        certifi_path = certifi.where()
    except Exception:
        certifi_path = None
    try:
        import requests

        requests_ca_path = requests.certs.where()
    except Exception:
        requests_ca_path = None

    logger = logging.getLogger("runtime.startup")
    message = (
        "Runtime environment | python=%s | certifi=%s | requests_ca=%s | "
        "REQUESTS_CA_BUNDLE=%s | SSL_CERT_FILE=%s | CURL_CA_BUNDLE=%s"
    )
    fields = (
        sys.executable,
        certifi_path,
        requests_ca_path,
        os.getenv("REQUESTS_CA_BUNDLE"),
        os.getenv("SSL_CERT_FILE"),
        os.getenv("CURL_CA_BUNDLE"),
    )
    logger.info(message, *fields)
    # Emit a plain line as well so startup diagnostics are visible even when logging is reconfigured.
    print(message % fields)


def _prewarm_models() -> None:
    # Intentionally not used — loading the sentence transformer in a background
    # thread crashes uvicorn on macOS due to tokenizer semaphore conflicts.
    # The model loads lazily on first use instead.
    pass


def create_app() -> FastAPI:
    app = FastAPI(title="Othello V2 API")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=CORS_ORIGINS,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    for router in (
        health_router,
        analytics_router,
        briefings_router,
        events_router,
        headlines_router,
        query_router,
        entities_router,
    ):
        app.include_router(router)

    scheduler = build_scheduler()

    @app.on_event("startup")
    def startup() -> None:
        import os
        os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
        _log_runtime_environment()
        initialize_runtime()
        threading.Thread(target=_prewarm_models, daemon=True).start()
        schedule_initial_analytics_warm(scheduler)
        if INTERNAL_SCHEDULER_ENABLED and not scheduler.running:
            scheduler.start()

    @app.on_event("shutdown")
    def shutdown() -> None:
        if scheduler.running:
            scheduler.shutdown()

    return app
