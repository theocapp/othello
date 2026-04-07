from __future__ import annotations

import json
import time
from datetime import datetime, timezone, timedelta

from db.common import (
    _connect,
    _coerce_timestamptz,
    _row_to_article,
    _headline_corpus_sql_filter,
    _parse_article_timestamp,
    _canonical_url,
    _domain,
    _content_hash,
)
from db.sources_repo import (
    build_source_registry_lookup,
    resolve_registry_row_for_article,
)


def _normalize_article(
    article: dict,
    provider: str,
    *,
    registry_lookup: dict | None = None,
    default_analytic_tier: str | None = None,
) -> dict:
    url = (article.get("url") or "").strip()
    if not url:
        raise ValueError("article missing url")

    canonical = _canonical_url(url)
    title = (article.get("title") or "").strip()
    if not title:
        raise ValueError("article missing title")

    description = (article.get("description") or "").strip()

    source = (article.get("source") or article.get("source_name") or "").strip() or None
    source_domain = (
        article.get("source_domain") or _domain(canonical) or ""
    ).strip() or None

    registry_row = None
    if registry_lookup is not None:
        registry_row = resolve_registry_row_for_article(
            source or "", source_domain or "", registry_lookup
        )

    if registry_row:
        source = registry_row.get("source_name") or source
        source_domain = registry_row.get("source_domain") or source_domain

    parsed = _parse_article_timestamp(article.get("published_at"))
    if parsed:
        published_at = parsed.isoformat()
    else:
        published_at = (
            article.get("published_at") or datetime.now(timezone.utc).isoformat()
        ).strip()

    language = (
        article.get("language")
        or (registry_row.get("language") if registry_row else None)
        or ""
    ).strip() or None

    payload = article.get("payload") if isinstance(article.get("payload"), dict) else {}

    record = {
        "url": url,
        "canonical_url": canonical,
        "title": title,
        "description": description,
        "source": source or "",
        "source_domain": source_domain or "",
        "published_at": published_at,
        "language": language or "",
        "provider": provider,
        "payload": {**payload},
    }

    record["content_hash"] = article.get("content_hash") or _content_hash(record)
    return record


def _bulk_upsert_articles_pg(
    conn,
    records: list[dict],
    topics: list[str],
    now_iso: str,
) -> int:
    """
    Stage normalised article rows into a temp table, COPY them in, then
    merge into articles_v2 / article_topics_v2.  Postgres-only.

    Returns count of staged rows (best-effort).
    """
    if not records:
        return 0

    import io

    conn.execute("""
        CREATE TEMP TABLE _stg_articles_v2 (
            url TEXT PRIMARY KEY,
            canonical_url TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            source TEXT NOT NULL,
            source_domain TEXT,
            published_at TIMESTAMPTZ NOT NULL,
            language TEXT,
            provider TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            first_ingested_at TIMESTAMPTZ NOT NULL,
            last_ingested_at TIMESTAMPTZ NOT NULL,
            payload JSONB NOT NULL
        ) ON COMMIT DROP
        """)

    _ARTICLE_COLS = (
        "url",
        "canonical_url",
        "title",
        "description",
        "source",
        "source_domain",
        "published_at",
        "language",
        "provider",
        "content_hash",
        "first_ingested_at",
        "last_ingested_at",
        "payload",
    )

    buf = io.StringIO()
    for rec in records:
        pub = _coerce_timestamptz(rec["published_at"])
        vals = [
            rec["url"],
            rec["canonical_url"],
            rec["title"],
            rec.get("description") or "",
            rec["source"],
            rec.get("source_domain") or "",
            pub,
            rec.get("language") or "",
            rec["provider"],
            rec["content_hash"],
            now_iso,
            now_iso,
            json.dumps(rec["payload"]),
        ]
        line = "\t".join(
            v.replace("\\", "\\\\")
            .replace("\t", " ")
            .replace("\n", " ")
            .replace("\r", "")
            for v in vals
        )
        buf.write(line + "\n")

    buf.seek(0)
    col_list = ", ".join(_ARTICLE_COLS)
    with conn.cursor().copy(f"COPY _stg_articles_v2 ({col_list}) FROM STDIN") as copy:
        while chunk := buf.read(8192):
            copy.write(chunk.encode("utf-8"))

    conn.execute("""
        INSERT INTO articles_v2 (
            url, canonical_url, title, description, source, source_domain,
            published_at, language, provider, content_hash,
            first_ingested_at, last_ingested_at, payload
        )
        SELECT
            url, canonical_url, title, description, source, source_domain,
            published_at, language, provider, content_hash,
            first_ingested_at, last_ingested_at, payload
        FROM _stg_articles_v2
        ON CONFLICT (url) DO UPDATE SET
            canonical_url   = EXCLUDED.canonical_url,
            title           = EXCLUDED.title,
            description     = EXCLUDED.description,
            source          = EXCLUDED.source,
            source_domain   = EXCLUDED.source_domain,
            published_at    = EXCLUDED.published_at,
            language        = EXCLUDED.language,
            provider        = EXCLUDED.provider,
            content_hash    = EXCLUDED.content_hash,
            last_ingested_at = EXCLUDED.last_ingested_at,
            payload         = EXCLUDED.payload
        """)

    if topics:
        conn.execute("""
            CREATE TEMP TABLE _stg_article_topics_v2 (
                article_url TEXT NOT NULL,
                topic TEXT NOT NULL,
                assigned_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (article_url, topic)
            ) ON COMMIT DROP
            """)

        topic_buf = io.StringIO()
        for rec in records:
            for t in topics:
                vals = [rec["url"], t, now_iso]
                line = "\t".join(
                    v.replace("\\", "\\\\")
                    .replace("\t", " ")
                    .replace("\n", " ")
                    .replace("\r", "")
                    for v in vals
                )
                topic_buf.write(line + "\n")

        topic_buf.seek(0)
        with conn.cursor().copy(
            "COPY _stg_article_topics_v2 (article_url, topic, assigned_at) FROM STDIN"
        ) as copy:
            while chunk := topic_buf.read(8192):
                copy.write(chunk.encode("utf-8"))

        conn.execute("""
            INSERT INTO article_topics_v2 (article_url, topic, assigned_at)
            SELECT article_url, topic, assigned_at FROM _stg_article_topics_v2
            ON CONFLICT (article_url, topic) DO UPDATE SET assigned_at = EXCLUDED.assigned_at
            """)

    return len(records)


def upsert_articles(
    articles: list[dict],
    topic: str | list[str],
    provider: str,
    *,
    default_analytic_tier: str | None = None,
) -> int:
    if not articles:
        return 0

    from core.config import ARTICLES_V2_DUAL_WRITE

    now = time.time()
    inserted = 0
    topics = [topic] if isinstance(topic, str) else list(topic)
    registry_lookup = build_source_registry_lookup(active_only=True)
    v2_records: list[dict] = []

    with _connect() as conn:
        for article in articles:
            try:
                record = _normalize_article(
                    article,
                    provider=provider,
                    registry_lookup=registry_lookup,
                    default_analytic_tier=default_analytic_tier,
                )
            except ValueError:
                continue

            v2_records.append(record)
            result = conn.execute(
                """
                INSERT INTO articles (
                    url, canonical_url, title, description, source, source_domain, published_at,
                    language, provider, content_hash, first_ingested_at, last_ingested_at, payload
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (url) DO UPDATE SET
                    canonical_url = EXCLUDED.canonical_url,
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    source = EXCLUDED.source,
                    source_domain = EXCLUDED.source_domain,
                    published_at = EXCLUDED.published_at,
                    language = EXCLUDED.language,
                    provider = EXCLUDED.provider,
                    content_hash = EXCLUDED.content_hash,
                    last_ingested_at = EXCLUDED.last_ingested_at,
                    payload = EXCLUDED.payload
                WHERE articles.content_hash IS DISTINCT FROM EXCLUDED.content_hash
                RETURNING xmax = 0 AS is_new
                """,
                (
                    record["url"],
                    record["canonical_url"],
                    record["title"],
                    record["description"],
                    record["source"],
                    record["source_domain"],
                    record["published_at"],
                    record["language"],
                    record["provider"],
                    record["content_hash"],
                    now,
                    now,
                    json.dumps(record.get("payload") or {}),
                ),
            ).fetchone()

            if result and result.get("is_new"):
                inserted += 1

            for topic_name in topics:
                conn.execute(
                    """
                    INSERT INTO article_topics (article_url, topic, assigned_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (article_url, topic) DO UPDATE SET assigned_at = EXCLUDED.assigned_at
                    """,
                    (record["url"], topic_name, now),
                )

        try:
            now_iso = datetime.now(timezone.utc).isoformat()
            if ARTICLES_V2_DUAL_WRITE:
                _bulk_upsert_articles_pg(conn, v2_records, topics, now_iso)
        except Exception:
            import logging

            logging.getLogger(__name__).exception(
                "articles_v2 dual-write failed (non-fatal)"
            )

    return inserted


def upsert_article_summaries(
    articles: list[dict], topic: str | None = None, quality_scores: dict | None = None
) -> int:
    if not articles:
        return 0

    now = time.time()
    inserted = 0
    scores = quality_scores or {}

    with _connect() as conn:
        for article in articles:
            url = (article.get("url") or "").strip()
            title = (article.get("title") or "").strip()
            if not url or not title:
                continue

            score = scores.get(url, article.get("quality_score", 0))
            source = (article.get("source") or _domain(url) or "unknown").strip()
            source_domain = (article.get("source_domain") or _domain(url)).strip()
            published_at = (
                article.get("published_at") or datetime.now(timezone.utc).isoformat()
            ).strip()
            article_topic = topic or (article.get("topic") or "")

            cursor = conn.execute(
                """
                INSERT INTO article_summaries (url, title, source, source_domain, published_at, topic, quality_score, first_seen_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING
                """,
                (
                    url,
                    title,
                    source,
                    source_domain,
                    published_at,
                    article_topic,
                    score,
                    now,
                ),
            )
            if cursor.rowcount > 0:
                inserted += 1

    return inserted


def save_article_translation(
    article_url: str,
    source_language: str,
    translated_title: str,
    translated_description: str | None,
    translation_provider: str,
    target_language: str = "en",
) -> None:
    translated_at = time.time()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO article_translations (
                article_url, source_language, target_language, translated_title,
                translated_description, translation_provider, translated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (article_url) DO UPDATE SET
                source_language = EXCLUDED.source_language,
                target_language = EXCLUDED.target_language,
                translated_title = EXCLUDED.translated_title,
                translated_description = EXCLUDED.translated_description,
                translation_provider = EXCLUDED.translation_provider,
                translated_at = EXCLUDED.translated_at
            """,
            (
                article_url,
                source_language,
                target_language,
                translated_title,
                translated_description,
                translation_provider,
                translated_at,
            ),
        )


def get_articles_missing_translation(limit: int = 24, hours: int = 336) -> list[dict]:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT a.*
            FROM articles a
            LEFT JOIN article_translations t ON t.article_url = a.url
            WHERE a.published_at >= %s
              AND COALESCE(LOWER(a.language), 'en') NOT IN ('en', 'eng', 'english', 'en-us', 'en-gb')
              AND t.article_url IS NULL
            ORDER BY a.published_at DESC, a.last_ingested_at DESC
            LIMIT %s
            """,
            (cutoff, limit),
        ).fetchall()
    return [_row_to_article(row) for row in rows]


def get_recent_articles(
    topic: str | None = None,
    limit: int = 60,
    hours: int = 72,
    *,
    headline_corpus_only: bool = False,
) -> list[dict]:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    tier_clause = _headline_corpus_sql_filter("a") if headline_corpus_only else ""
    with _connect() as conn:
        if topic:
            rows = conn.execute(
                f"""
                SELECT a.*, tr.translated_title, tr.translated_description, tr.source_language AS translation_source_language,
                       tr.target_language AS translation_target_language, tr.translation_provider, tr.translated_at
                FROM articles a
                JOIN article_topics t ON t.article_url = a.url
                LEFT JOIN article_translations tr ON tr.article_url = a.url
                WHERE t.topic = %s AND a.published_at >= %s{tier_clause}
                ORDER BY a.published_at DESC, a.last_ingested_at DESC
                LIMIT %s
                """,
                (topic, cutoff, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                f"""
                SELECT a.*, tr.translated_title, tr.translated_description, tr.source_language AS translation_source_language,
                       tr.target_language AS translation_target_language, tr.translation_provider, tr.translated_at
                FROM articles a
                LEFT JOIN article_translations tr ON tr.article_url = a.url
                WHERE published_at >= %s{tier_clause}
                ORDER BY published_at DESC, last_ingested_at DESC
                LIMIT %s
                """,
                (cutoff, limit),
            ).fetchall()
    return [_row_to_article(row) for row in rows]


def get_articles_with_regions(hours: int = 72) -> list[dict]:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    active_clause = "TRUE"
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT
                a.source,
                a.source_domain,
                a.published_at,
                COALESCE(domain_registry.region, name_registry.region, 'global') AS region
            FROM articles a
            LEFT JOIN source_registry domain_registry
                ON domain_registry.source_domain = a.source_domain
               AND domain_registry.active = {active_clause}
            LEFT JOIN source_registry name_registry
                ON name_registry.source_name = a.source
               AND name_registry.active = {active_clause}
            WHERE a.published_at >= %s
            ORDER BY a.published_at DESC, a.last_ingested_at DESC
            """,
            (cutoff,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_articles_by_urls(urls: list[str], *, limit: int = 64) -> dict[str, dict]:
    cleaned = [str(u).strip() for u in urls if u and str(u).strip()]
    if not cleaned:
        return {}
    cap = max(1, min(limit, 120))
    cleaned = cleaned[:cap]
    placeholders = ", ".join(["%s"] * len(cleaned))
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT a.*, tr.translated_title, tr.translated_description, tr.source_language AS translation_source_language,
                   tr.target_language AS translation_target_language, tr.translation_provider, tr.translated_at
            FROM articles a
            LEFT JOIN article_translations tr ON tr.article_url = a.url
            WHERE a.url IN ({placeholders})
            """,
            cleaned,
        ).fetchall()
    return {str(row["url"]): _row_to_article(row) for row in rows}


def _published_values(topic: str | None = None) -> list[str]:
    params: list[object] = []
    join = ""
    where = ""
    if topic:
        join = "JOIN article_topics t ON t.article_url = a.url"
        where = "WHERE t.topic = %s"
        params.append(topic)
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT DISTINCT a.url, a.published_at
            FROM articles a
            {join}
            {where}
            """,
            params,
        ).fetchall()
    return [row["published_at"] for row in rows if row["published_at"]]


def _count_articles_since(cutoff: datetime, topic: str | None = None) -> int:
    count = 0
    for value in _published_values(topic=topic):
        parsed = _parse_article_timestamp(value)
        if parsed and parsed >= cutoff:
            count += 1
    return count


def get_article_count(topic: str | None = None, hours: int | None = None) -> int:
    if hours is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        return _count_articles_since(cutoff, topic=topic)

    clauses = []
    params: list[object] = []
    join = ""
    if topic:
        join = "JOIN article_topics t ON t.article_url = a.url"
        clauses.append("t.topic = %s")
        params.append(topic)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with _connect() as conn:
        row = conn.execute(
            f"""
            SELECT COUNT(DISTINCT a.url) AS count
            FROM articles a
            {join}
            {where}
            """,
            params,
        ).fetchone()
    return int((row["count"] if row else 0) or 0)


def _topic_time_bounds_python(topic: str | None = None) -> dict:
    parsed_values = [
        parsed
        for parsed in (
            _parse_article_timestamp(value) for value in _published_values(topic=topic)
        )
        if parsed is not None
    ]
    if not parsed_values:
        return {"earliest_published_at": None, "latest_published_at": None}
    earliest = (
        min(parsed_values).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    )
    latest = (
        max(parsed_values).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    )
    return {"earliest_published_at": earliest, "latest_published_at": latest}


def get_topic_time_bounds(topic: str | None = None) -> dict:
    return _topic_time_bounds_python(topic=topic)


def search_recent_articles_by_keywords(
    query: str, topic: str | None = None, limit: int = 12, hours: int = 168
) -> list[dict]:
    words = [
        word.strip().lower()
        for word in query.replace("?", " ").replace(",", " ").split()
        if len(word.strip()) >= 4
    ]
    if not words:
        return get_recent_articles(topic=topic, limit=limit, hours=hours)

    articles = get_recent_articles(topic=topic, limit=200, hours=hours)
    ranked = []
    for article in articles:
        haystack = " ".join(
            [
                article.get("title", ""),
                article.get("description", ""),
                article.get("source", ""),
            ]
        ).lower()
        score = sum(1 for word in words if word in haystack)
        if score:
            ranked.append((score, article.get("published_at", ""), article))

    ranked.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return [article for _, _, article in ranked[:limit]]
