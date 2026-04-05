from apscheduler.schedulers.background import BackgroundScheduler


def build_scheduler(include_ingestion: bool = True, include_translations: bool = True, include_analytics: bool = True) -> BackgroundScheduler:
    from main import build_scheduler as build_main_scheduler
    return build_main_scheduler(include_ingestion=include_ingestion, include_translations=include_translations, include_analytics=include_analytics)


def build_worker_scheduler() -> BackgroundScheduler:
    from main import build_worker_scheduler as build_main_worker_scheduler
    return build_main_worker_scheduler()
