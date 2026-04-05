import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=True)


def split_csv_env(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


DEFAULT_CORS_ORIGINS = [
    "http://localhost:5173",
    "http://localhost:5174",
    "http://localhost:5175",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:5174",
    "http://127.0.0.1:5175",
    "http://127.0.0.1:5176",
]

CORS_ORIGINS = split_csv_env(os.getenv("OTHELLO_CORS_ORIGINS")) or DEFAULT_CORS_ORIGINS
TOPICS = ["geopolitics", "economics"]
BRIEFING_TOPICS = ["geopolitics", "economics", "conflict"]
