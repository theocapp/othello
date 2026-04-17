import argparse
import json
import re
import time

from db.common import _connect


RETRYABLE_HTTP_CODES = {408, 425, 429, 500, 502, 503, 504}
RETRYABLE_TEXT_SNIPPETS = (
    "timed out",
    "timeout",
    "connection reset",
    "connection aborted",
    "connection refused",
    "temporary failure",
    "temporarily unavailable",
    "name or service not known",
    "failed to establish",
    "remote end closed connection",
    "proxyerror",
    "read timeout",
    "connect timeout",
    "tls",
    "ssl",
)


def _is_retryable_error(error_text: str) -> bool:
    text = (error_text or "").strip().lower()
    if not text:
        return False

    http_match = re.search(r"http\s+(\d{3})", text)
    if http_match:
        code = int(http_match.group(1))
        if code in RETRYABLE_HTTP_CODES:
            return True
        return False

    return any(snippet in text for snippet in RETRYABLE_TEXT_SNIPPETS)


def requeue_retryable_failures(apply_changes: bool, limit: int | None) -> dict:
    with _connect() as conn:
        with conn.cursor() as cur:
            query = (
                """
                SELECT url, COALESCE(payload->>'last_error', '') AS last_error
                FROM historical_url_queue
                WHERE fetch_status = 'failed'
                ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
                """
            )
            if limit is not None:
                query += " LIMIT %s"
                cur.execute(query, (max(1, limit),))
            else:
                cur.execute(query)
            rows = cur.fetchall()

        retryable_urls: list[str] = []
        non_retryable_count = 0
        for row in rows:
            if _is_retryable_error(row.get("last_error") or ""):
                retryable_urls.append(row["url"])
            else:
                non_retryable_count += 1

        if apply_changes and retryable_urls:
            now = time.time()
            with conn.cursor() as cur:
                for url in retryable_urls:
                    cur.execute(
                        """
                        UPDATE historical_url_queue
                        SET fetch_status = 'retry',
                            attempt_count = 0,
                            last_attempt_at = NULL,
                            updated_at = %s
                        WHERE url = %s
                        """,
                        (now, url),
                    )

    return {
        "inspected_failed": len(rows),
        "retryable_found": len(retryable_urls),
        "non_retryable_found": non_retryable_count,
        "requeued": len(retryable_urls) if apply_changes else 0,
        "mode": "apply" if apply_changes else "dry-run",
    }


def requeue_no_topic_items(apply_changes: bool, limit: int | None) -> dict:
    """Reset all fetch_status='no_topic' items back to 'retry' for re-classification."""
    with _connect() as conn:
        with conn.cursor() as cur:
            query = (
                "SELECT url FROM historical_url_queue "
                "WHERE fetch_status = 'no_topic' "
                "ORDER BY updated_at DESC NULLS LAST"
            )
            if limit is not None:
                query += " LIMIT %s"
                cur.execute(query, (max(1, limit),))
            else:
                cur.execute(query)
            rows = cur.fetchall()

        urls = [row["url"] for row in rows]
        if apply_changes and urls:
            now = time.time()
            with conn.cursor() as cur:
                for url in urls:
                    cur.execute(
                        """
                        UPDATE historical_url_queue
                        SET fetch_status = 'retry',
                            attempt_count = 0,
                            last_attempt_at = NULL,
                            updated_at = %s
                        WHERE url = %s
                        """,
                        (now, url),
                    )

    return {
        "no_topic_found": len(urls),
        "requeued": len(urls) if apply_changes else 0,
        "mode": "apply" if apply_changes else "dry-run",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Requeue only retryable historical queue failures "
            "(429/5xx/network-like errors)."
        )
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply updates. Without this flag, command runs as dry-run.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on number of failed rows to inspect.",
    )
    parser.add_argument(
        "--requeue-no-topic",
        action="store_true",
        help="Reset all 'no_topic' queue items to 'retry' for re-classification.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.requeue_no_topic:
        no_topic_result = requeue_no_topic_items(
            apply_changes=args.apply,
            limit=args.limit,
        )
        retryable_result = requeue_retryable_failures(
            apply_changes=args.apply,
            limit=args.limit,
        )
        print(
            json.dumps(
                {"no_topic": no_topic_result, "retryable_failures": retryable_result},
                indent=2,
            )
        )
    else:
        result = requeue_retryable_failures(
            apply_changes=args.apply,
            limit=args.limit,
        )
        print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())