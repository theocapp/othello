import json
import os
import sqlite3
import time
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).with_name("othello_cache.db")
SQLITE_TIMEOUT_SECONDS = float(os.getenv("OTHELLO_SQLITE_TIMEOUT_SECONDS", "30"))
LOCK_TIMEOUT_SECONDS = float(os.getenv("OTHELLO_CACHE_LOCK_TIMEOUT_SECONDS", "60"))


def _connect():
    conn = sqlite3.connect(str(DB_PATH), timeout=SQLITE_TIMEOUT_SECONDS)
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    except sqlite3.OperationalError as exc:
        if "locked" not in str(exc).lower():
            raise
    return conn


def _ensure_initialized():
    conn = _connect()
    try:
        conn.execute("SELECT 1 FROM briefing_cache LIMIT 1")
        conn.execute("SELECT 1 FROM headlines_cache LIMIT 1")
    except sqlite3.OperationalError:
        init_db()
    finally:
        conn.close()


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row[1] == column for row in rows)


def init_db():
    conn = _connect()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS briefing_cache (
            topic TEXT PRIMARY KEY,
            briefing TEXT,
            sources TEXT,
            article_count INTEGER,
            generated_at REAL
        )
        """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS headlines_cache (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            stories TEXT,
            generated_at REAL
        )
        """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cache_locks (
            lock_key TEXT PRIMARY KEY,
            locked_at REAL,
            locked_by TEXT
        )
        """)

    if not _column_exists(conn, "briefing_cache", "events"):
        conn.execute("ALTER TABLE briefing_cache ADD COLUMN events TEXT DEFAULT '[]'")

    conn.commit()
    conn.close()


def acquire_lock(lock_key: str, lock_holder_id: str = "default") -> bool:
    """
    Attempt to acquire a distributed lock using SQLite.
    Returns True if lock acquired, False if already held by someone else.
    Locks expire after LOCK_TIMEOUT_SECONDS to prevent deadlocks.
    """
    _ensure_initialized()
    conn = _connect()
    try:
        now = time.time()

        # Check if lock exists and is still valid
        existing = conn.execute(
            "SELECT locked_at FROM cache_locks WHERE lock_key = ?",
            (lock_key,),
        ).fetchone()

        if existing:
            locked_at = existing[0]
            if now - locked_at < LOCK_TIMEOUT_SECONDS:
                # Lock is held by someone else and not expired
                conn.close()
                return False
            else:
                # Lock expired, remove it
                conn.execute("DELETE FROM cache_locks WHERE lock_key = ?", (lock_key,))

        # Try to insert our lock
        try:
            conn.execute(
                "INSERT INTO cache_locks (lock_key, locked_at, locked_by) VALUES (?, ?, ?)",
                (lock_key, now, lock_holder_id),
            )
            conn.commit()
            conn.close()
            logger.debug(f"[cache] Acquired lock '{lock_key}'")
            return True
        except sqlite3.IntegrityError:
            # Someone else inserted between our check and insert
            conn.close()
            return False
    except Exception as exc:
        logger.error(f"[cache] Error acquiring lock '{lock_key}': {exc}")
        conn.close()
        return False


def release_lock(lock_key: str) -> None:
    """Release a distributed lock."""
    _ensure_initialized()
    conn = _connect()
    try:
        conn.execute("DELETE FROM cache_locks WHERE lock_key = ?", (lock_key,))
        conn.commit()
        logger.debug(f"[cache] Released lock '{lock_key}'")
    except Exception as exc:
        logger.error(f"[cache] Error releasing lock '{lock_key}': {exc}")
    finally:
        conn.close()


def load_briefing(topic: str, ttl: int = 3600) -> dict | None:
    _ensure_initialized()
    conn = _connect()
    row = conn.execute(
        "SELECT briefing, sources, article_count, generated_at, events FROM briefing_cache WHERE topic = ?",
        (topic,),
    ).fetchone()
    conn.close()
    if not row:
        return None

    generated_at = row[3]
    if time.time() - generated_at > ttl:
        return None

    briefing_raw = row[0]
    try:
        briefing_val = json.loads(briefing_raw) if briefing_raw else briefing_raw
    except (json.JSONDecodeError, TypeError):
        briefing_val = briefing_raw

    return {
        "topic": topic,
        "briefing": briefing_val,
        "sources": json.loads(row[1]),
        "article_count": row[2],
        "generated_at": generated_at,
        "events": json.loads(row[4] or "[]"),
    }


def save_briefing(
    topic: str,
    briefing: str | dict,
    sources: list,
    article_count: int,
    events: list | None = None,
):
    _ensure_initialized()
    conn = _connect()
    briefing_str = json.dumps(briefing) if isinstance(briefing, dict) else briefing
    conn.execute(
        """
        INSERT OR REPLACE INTO briefing_cache (topic, briefing, sources, article_count, generated_at, events)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            topic,
            briefing_str,
            json.dumps(sources),
            article_count,
            time.time(),
            json.dumps(events or []),
        ),
    )
    conn.commit()
    conn.close()


def load_all_briefings(ttl: int = 3600) -> list[dict]:
    _ensure_initialized()
    conn = _connect()
    rows = conn.execute(
        "SELECT topic, briefing, sources, article_count, generated_at, events FROM briefing_cache"
    ).fetchall()
    conn.close()

    fresh = []
    now = time.time()
    for row in rows:
        if now - row[4] > ttl:
            continue
        fresh.append(
            {
                "topic": row[0],
                "briefing": row[1],
                "sources": json.loads(row[2]),
                "article_count": row[3],
                "generated_at": row[4],
                "events": json.loads(row[5] or "[]"),
            }
        )
    return fresh


def load_headlines(ttl: int = 3600) -> list | None:
    _ensure_initialized()
    conn = _connect()
    row = conn.execute(
        "SELECT stories, generated_at FROM headlines_cache WHERE id = 1"
    ).fetchone()
    conn.close()
    if not row:
        return None
    if time.time() - row[1] > ttl:
        return None
    return json.loads(row[0])


def save_headlines(stories: list):
    _ensure_initialized()
    conn = _connect()
    conn.execute(
        """
        INSERT OR REPLACE INTO headlines_cache (id, stories, generated_at)
        VALUES (1, ?, ?)
        """,
        (json.dumps(stories), time.time()),
    )
    conn.commit()
    conn.close()


def clear_headlines():
    _ensure_initialized()
    conn = _connect()
    conn.execute("DELETE FROM headlines_cache WHERE id = 1")
    conn.commit()
    conn.close()
