#!/usr/bin/env python3
"""SQLite migration helper: add canonical_events columns if missing.

Usage: python3 0001_add_canonical_events_sqlite.py /path/to/sqlite.db

This script is safe to re-run; it checks for existing columns via PRAGMA and
adds missing ones with `ALTER TABLE ... ADD COLUMN` (SQLite supports only that).
"""

import sqlite3
import sys


def has_column(conn, table, col):
    cur = conn.execute(f"PRAGMA table_info('{table}')")
    cols = [r[1] for r in cur.fetchall()]
    return col in cols


def add_column(conn, table, coldef):
    col_name = coldef.split()[0]
    if not has_column(conn, table, col_name):
        print(f"Adding column {col_name} to {table}...")
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {coldef}")
        conn.commit()
    else:
        print(f"Column {col_name} already exists on {table}; skipping.")


def main(db_path: str):
    conn = sqlite3.connect(db_path)
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='canonical_events'"
    )
    if not cur.fetchone():
        print("Error: canonical_events table not found in", db_path)
        return 1

    cols = [
        "neutral_summary TEXT",
        "neutral_confidence REAL",
        "neutral_generated_at TEXT",
        "linked_structured_event_ids TEXT DEFAULT '[]'",
        "article_urls TEXT DEFAULT '[]'",
        "first_seen_at TEXT",
        "computed_at TEXT DEFAULT (datetime('now'))",
        "payload TEXT DEFAULT '{}'",
    ]

    for c in cols:
        add_column(conn, "canonical_events", c)

    # Backfill safer defaults where possible
    try:
        conn.execute(
            "UPDATE canonical_events SET topic = 'uncategorized' WHERE topic IS NULL OR topic = ''"
        )
        conn.execute(
            "UPDATE canonical_events SET label = COALESCE(label, 'unnamed event') WHERE label IS NULL OR label = ''"
        )
        conn.commit()
    except Exception as e:
        print("Warning: backfill failed —", e)

    conn.close()
    print("SQLite canonical_events migration complete.")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 0001_add_canonical_events_sqlite.py /path/to/sqlite.db")
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
