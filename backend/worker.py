import signal
import sys
import time

from bootstrap import initialize_runtime
from bootstrap_jobs import build_bootstrap_jobs
from core.config import (
    WORKER_BOOTSTRAP_MODE,
    WORKER_ENABLE_INGESTION,
    WORKER_ENABLE_TRANSLATIONS,
)
from core.scheduler import build_worker_scheduler


def initialize_worker_state() -> None:
    initialize_runtime()


def main() -> int:
    initialize_worker_state()

    # Keep launch memory predictable: do the minimum needed to keep ingestion warm,
    # and let the scheduler handle the rest over time.
    bootstrap_jobs = build_bootstrap_jobs()

    for label, job in bootstrap_jobs:
        try:
            result = job()
            print(f"[worker] Startup job '{label}' completed: {result}")
        except Exception as exc:
            print(f"[worker] Startup job '{label}' failed: {exc}")

    scheduler = build_worker_scheduler()
    scheduler.start()
    print(
        "[worker] Othello worker started "
        f"(ingestion={WORKER_ENABLE_INGESTION}, translations={WORKER_ENABLE_TRANSLATIONS}, bootstrap={WORKER_BOOTSTRAP_MODE})"
    )

    def handle_shutdown(signum, frame):
        print(f"[worker] Shutting down on signal {signum}")
        if scheduler.running:
            scheduler.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    try:
        while True:
            time.sleep(30)
    except KeyboardInterrupt:
        handle_shutdown(signal.SIGINT, None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
