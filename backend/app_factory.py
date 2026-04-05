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
        initialize_runtime()
        schedule_initial_analytics_warm(scheduler)
        if INTERNAL_SCHEDULER_ENABLED and not scheduler.running:
            scheduler.start()

    @app.on_event("shutdown")
    def shutdown() -> None:
        if scheduler.running:
            scheduler.shutdown()

    return app
