import sqlite3
import json
import time
import os

DB_PATH = "othello_cache.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
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
    conn.commit()
    conn.close()

def load_briefing(topic: str, ttl: int = 3600) -> dict | None:
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT briefing, sources, article_count, generated_at FROM briefing_cache WHERE topic = ?",
        (topic,)
    ).fetchone()
    conn.close()
    if not row:
        return None
    generated_at = row[3]
    if time.time() - generated_at > ttl:
        return None
    return {
        "topic": topic,
        "briefing": row[0],
        "sources": json.loads(row[1]),
        "article_count": row[2],
        "generated_at": generated_at,
    }

def save_briefing(topic: str, briefing: str, sources: list, article_count: int):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT OR REPLACE INTO briefing_cache (topic, briefing, sources, article_count, generated_at)
        VALUES (?, ?, ?, ?, ?)
    """, (topic, briefing, json.dumps(sources), article_count, time.time()))
    conn.commit()
    conn.close()

def load_headlines(ttl: int = 3600) -> list | None:
    conn = sqlite3.connect(DB_PATH)
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
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT OR REPLACE INTO headlines_cache (id, stories, generated_at)
        VALUES (1, ?, ?)
    """, (json.dumps(stories), time.time()))
    conn.commit()
    conn.close()