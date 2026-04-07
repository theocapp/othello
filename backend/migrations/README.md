Migration notes
===============

This directory contains SQL and helper scripts to migrate the canonical
event schema. Use the Postgres script for PostgreSQL databases and the
SQLite helper script for local or fallback SQLite databases.

Postgres
--------
Run the Postgres migration using `psql` (or your DB migration tool):

```bash
psql $DATABASE_URL -f backend/migrations/0001_add_canonical_events_postgres.sql
```

SQLite
------
Run the SQLite helper script, passing the path to your SQLite file:

```bash
python3 backend/migrations/0001_add_canonical_events_sqlite.py /path/to/your.db
```

Notes
-----
- Always back up your DB before applying schema changes.
- The Postgres script attempts to add columns using `ADD COLUMN IF NOT EXISTS`
  and to set NOT NULL constraints after safe backfills. On very large tables,
  consider running index creation during a maintenance window.
- The SQLite helper uses `ALTER TABLE ... ADD COLUMN` per-column and will skip
  columns that already exist; it cannot add NOT NULL constraints post-hoc.
