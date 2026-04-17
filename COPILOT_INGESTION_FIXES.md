# Ingestion Pipeline Fix Instructions

This document describes targeted fixes for the article ingestion pipeline in priority order.
Each section includes the exact file, the problem, and what to change. Do not refactor
beyond what is described. Do not add docstrings or comments to unchanged code.

---

## Fix 1 — Stop permanently failing articles that have no topic

**Files:** `backend/fetch_historical_queue.py`

**Problem:** When a fetched article cannot be assigned a topic by `infer_article_topics()`,
the queue item is marked `fetch_status = "failed"` (lines ~360–368). This is permanent —
the item will never be retried, and `requeue_retryable_failures.py` ignores it because the
error text `"Could not infer a topic for fetched article"` does not match any retryable
pattern. Successfully fetched articles are silently lost.

**Fix:**

1. Change the failure path when no topic is found so it marks the item with
   `fetch_status = "no_topic"` instead of `"failed"`. Keep the `last_error` payload patch
   as-is so the reason is still recorded.

2. Store the fetched article in the corpus as tier-2 under the topic `"unclassified"` so
   the content is not lost. Use `upsert_articles()` with `topic="unclassified"` and
   `default_analytic_tier="volume"`. Only do this if `dry_run` is False.

3. In the summary dict, add a `"no_topic"` counter alongside `"retry"` and `"failed"`.
   Increment it whenever a `"no_topic"` status is assigned.

4. In `get_historical_url_queue_batch()` in `backend/db/sources_repo.py` (line ~250),
   add `"no_topic"` as a valid status that can be passed in the `statuses` list. Do NOT
   add it to the default batch — it should only be fetched when explicitly requested
   (e.g., from a manual requeue script).

5. In `backend/requeue_retryable_failures.py`, add `"no_topic"` as a separately handled
   case. When `--requeue-no-topic` flag is passed (add this new CLI flag), reset all
   `fetch_status = "no_topic"` items to `fetch_status = "retry"` with `attempt_count = 0`
   and `last_attempt_at = NULL`. Print a count of how many were requeued. This allows
   bulk re-classification after keyword updates.

---

## Fix 2 — Widen `TOPIC_KEYWORDS` and fix the score threshold

**File:** `backend/news.py`

**Problem:** `TOPIC_KEYWORDS` (lines 58–109) is too narrow. It relies heavily on named
entities and a small set of terms. Articles about coups, rebel advances, North Korea,
elections in developing countries, currency crises, or commodity shocks frequently match
zero keywords and are hard-failed. The `infer_article_topics()` function also requires a
minimum score of 2 (line ~1462), meaning a single strong keyword match is rejected.

**Fix — Part A: Expand the keyword sets**

Replace the existing `TOPIC_KEYWORDS` dict with this expanded version. Preserve the
existing set structure (not list):

```python
TOPIC_KEYWORDS = {
    "geopolitics": {
        # conflict
        "war", "conflict", "military", "ceasefire", "airstrike", "air strike",
        "missile", "strike", "bombing", "explosion", "attack", "assault",
        "invasion", "occupation", "offensive", "counteroffensive", "shelling",
        "artillery", "drone strike", "naval", "warship", "fighter jet",
        "special forces", "troops", "soldiers", "casualties", "killed", "wounded",
        "fatalities", "civilian", "displacement", "refugee", "evacuation",
        # political
        "sanctions", "diplomacy", "treaty", "agreement", "summit", "talks",
        "ceasefire", "peace deal", "nato", "un security council", "united nations",
        "elections", "election", "coup", "overthrow", "junta", "referendum",
        "parliament", "president", "prime minister", "government", "regime",
        "opposition", "protest", "uprising", "crackdown", "arrest", "detained",
        "extradition", "asylum", "expelled", "ambassador", "diplomat",
        "foreign minister", "secretary of state", "state department",
        "midterms", "election interference", "ballot",
        # entities (keep but don't rely on exclusively)
        "iran", "israel", "ukraine", "russia", "china", "taiwan", "north korea",
        "gaza", "west bank", "hamas", "hezbollah", "isis", "al-qaeda",
        "putin", "zelensky", "netanyahu", "xi jinping",
        "sudan", "myanmar", "ethiopia", "sahel", "mali", "niger", "burkina faso",
        "somalia", "yemen", "syria", "iraq", "afghanistan", "pakistan",
        "venezuela", "haiti", "colombia", "mexico cartel",
        # intelligence / security
        "intelligence", "espionage", "spy", "cyber attack", "cyberattack",
        "hack", "disinformation", "propaganda", "nuclear", "weapons",
        "ballistic", "hypersonic", "chemical weapons",
    },
    "economics": {
        # macro
        "inflation", "recession", "gdp", "growth", "economy", "economic",
        "unemployment", "jobs", "labor market", "wage", "wages",
        # monetary
        "interest rate", "rates", "federal reserve", "fed", "central bank",
        "monetary policy", "quantitative easing", "tightening", "rate hike",
        "rate cut", "yield", "yields", "bond", "treasury",
        # trade / fiscal
        "tariffs", "tariff", "trade war", "trade deal", "trade deficit",
        "sanctions", "embargo", "export", "import", "supply chain",
        "supply crunch", "shortage",
        # markets
        "market", "markets", "stocks", "equities", "index", "rally", "selloff",
        "volatility", "futures", "options", "hedge fund",
        # energy / commodities
        "oil", "crude", "opec", "gas", "energy", "commodity", "commodities",
        "gold", "copper", "wheat", "food prices", "gasoline", "fuel",
        # corporate / finance
        "buyout", "merger", "acquisition", "ipo", "bankruptcy", "default",
        "debt", "credit", "loan", "bailout", "austerity", "imf", "world bank",
        "rupee", "yuan", "ruble", "currency", "exchange rate", "devaluation",
        # tech/economic crossover
        "semiconductor", "chip", "supply chain", "reshoring", "nearshoring",
    },
}
```

**Fix — Part B: Lower the minimum score threshold for single strong matches**

In `infer_article_topics()` (line ~1456–1463), change the minimum score guard:

```python
# Before:
if best_score < 2 and not (best_topic == "economics" and "market" in haystack):
    return []

# After:
if best_score < 1:
    return []
```

A single keyword match on the expanded set is meaningful. The old guard was calibrated
for a small keyword set where false positives were common. With the expanded set, a
single match is sufficient signal.

---

## Fix 3 — Fix `LOW_SIGNAL_PATTERNS` to stop penalizing legitimate content

**File:** `backend/news.py` (lines 111–123)

**Problem:** Several patterns incorrectly penalize genuine intelligence reporting:
- `r"\blive updates?\b"` — live conflict dispatches are exactly the content this
  system wants to capture.
- `r"\breview\b"` — matches "UN Security Council review of resolution", "Pentagon review
  of strategy", etc.
- `r"\bopinion\b"` — matches "in the opinion of analysts", "experts opinion on..."
  appearing in article descriptions.
- `r"\bbest\b"` — too broad; matches "best path to ceasefire", "best estimates".

**Fix:**

Replace the `LOW_SIGNAL_PATTERNS` list with the following. Apply these patterns **only
against `title`** (not against the full `title + description + source` haystack). This
prevents false matches where the problematic word appears in a neutral description.

```python
LOW_SIGNAL_PATTERNS = [
    r"\bhow to\b",
    r"\bwhat is\b",
    r"\bwhat are\b",
    r"\bwhen asked\b",
    r"\btalking about\b",
    r"\btop \d+\b",
    r"\bquiz\b",
    r"\bbest \w+ to\b",          # "best way to", "best path to" — requires "to" after
    r"\bopinion:\b",             # "Opinion: ..." headline format only
    r"\breview:\b",              # "Review: ..." headline format only
]
```

In `article_quality_score()` (line ~1105), change the haystack used for
`LOW_SIGNAL_PATTERNS` from the combined `text` to just the article title:

```python
# Before: matches against combined title + description + source
for pattern in LOW_SIGNAL_PATTERNS:
    if re.search(pattern, haystack):
        score -= 4

# After: match only against title so descriptions don't cause false penalties
title_lower = title.lower()
for pattern in LOW_SIGNAL_PATTERNS:
    if re.search(pattern, title_lower):
        score -= 4
```

---

## Fix 4 — Don't retry non-retryable extraction failures

**File:** `backend/fetch_historical_queue.py` (lines ~319–330)

**Problem:** When HTML is successfully fetched (HTTP 200) but no title can be extracted,
the item is put into `"retry"` status up to `max_attempts` times. Re-fetching the same
URL will produce the same result. This wastes batch slots.

**Fix:**

Split extraction failures into retryable vs. non-retryable. An extraction failure on a
200 response is non-retryable. A network-level failure is retryable.

Introduce a constant for non-retryable extraction error substrings:

```python
_NON_RETRYABLE_EXTRACTION_ERRORS = (
    "could not extract a title",
    "unsupported content type",
)
```

In the extraction error block (the `if extraction_error or not article:` branch), check
whether the error matches a non-retryable pattern before deciding the status:

```python
if extraction_error or not article:
    err_lower = (extraction_error or "").lower()
    non_retryable = any(s in err_lower for s in _NON_RETRYABLE_EXTRACTION_ERRORS)
    fetch_status = (
        "failed"
        if non_retryable or attempts >= max_attempts
        else "retry"
    )
    update_historical_url_queue_status(
        url,
        fetch_status,
        attempt_count=attempts,
        payload_patch={
            "last_error": extraction_error or "Unknown extraction failure"
        },
    )
    summary["retry" if fetch_status == "retry" else "failed"] += 1
    continue
```

---

## Fix 5 — Don't store body text in the queue payload

**File:** `backend/fetch_historical_queue.py` (lines ~341–346)

**Problem:** `body_text` (up to several KB per article) is written into the `payload`
JSONB column of `historical_url_queue` even though the content is already written to the
`articles_v2` table. This bloats the queue table unnecessarily.

**Fix:**

Remove `"body_text"` from the payload dict built for the queue item. Keep it in the
`article` dict for the purposes of writing to the corpus, but do not include it in
`payload_patch` sent to `update_historical_url_queue_status`.

```python
# Before:
article["payload"] = {
    "historical_queue_url": url,
    "historical_discovered_via": row.get("discovered_via"),
    "historical_topic_guess": row.get("topic_guess"),
    "body_text": article["body_text"],
}

# After:
article["payload"] = {
    "historical_queue_url": url,
    "historical_discovered_via": row.get("discovered_via"),
    "historical_topic_guess": row.get("topic_guess"),
}
```

In the final `update_historical_url_queue_status` call (lines ~401–413), also remove
`"fetched_body_chars"` from the `payload_patch` if you want to keep the queue lean, or
keep just the char count (not the text). The char count is fine to keep as it is useful
for debugging.

---

## Fix 6 — Validate `topic_guess` against fetched content

**File:** `backend/fetch_historical_queue.py` (lines ~348–352)

**Problem:** The GDELT-provided `topic_guess` is trusted unconditionally. If GDELT
misclassified the article, it is stored under the wrong topic with no correction.

**Fix:**

After resolving topics from `topic_guess`, verify the guess has at least one keyword
match against the fetched article. If not, fall back to `infer_article_topics()` on the
actual content. If that also returns empty, keep the original `topic_guess` (better than
dropping the article):

```python
# Before:
topics = (
    [row["topic_guess"]]
    if row.get("topic_guess")
    else infer_article_topics(article)
)

# After:
if row.get("topic_guess"):
    # Verify the guess matches at least one keyword in the actual content
    inferred = infer_article_topics(article)
    if inferred:
        # Prefer content-derived topics; include topic_guess if it also matches
        topics = inferred
        if row["topic_guess"] not in topics:
            # topic_guess didn't survive content check — log it but don't block
            pass
    else:
        # Content inference failed; trust the GDELT guess rather than losing the article
        topics = [row["topic_guess"]]
else:
    topics = infer_article_topics(article)
```

---

## Fix 7 — Improve language detection with a fallback

**File:** `backend/fetch_historical_queue.py` (line ~339)

**Problem:** Language defaults to `"en"` when neither the queue row nor `og:locale` meta
tag provides a value. Many non-English articles are incorrectly marked English, causing
the translation pipeline to skip them.

**Fix:**

After setting `article["language"]`, add a lightweight content-based language check using
Python's `langdetect` library (already available, or add it). Only run this if the
resolved language is `"en"` and the title contains non-ASCII characters, which is a
strong signal of a non-English article:

```python
article["language"] = row.get("language") or article.get("language") or "en"

# Heuristic: if language resolved to "en" but title has non-ASCII chars,
# attempt detection on title + description
if article["language"] == "en":
    text_sample = " ".join(filter(None, [
        article.get("title", ""),
        article.get("description", ""),
    ]))
    if text_sample and not text_sample.isascii():
        try:
            from langdetect import detect
            detected = detect(text_sample)
            if detected and detected != "en":
                article["language"] = detected
        except Exception:
            pass  # langdetect failure is non-fatal; keep "en"
```

If `langdetect` is not already a dependency, add it to `requirements.txt`.

---

## Fix 8 — Fix duplicate paragraph extraction in `_ArticleTextParser`

**File:** `backend/fetch_historical_queue.py` (lines ~75–133)

**Problem:** `_capture_depth` is a simple counter incremented for every matching
`<div>`, `<section>`, `<p>`, or `<article>` tag. When a body-content div contains
nested divs that also match body markers, both the outer and inner divs emit their
accumulated text, producing duplicate or overlapping paragraphs in `body_text`.

**Fix:**

Track whether we are already inside a captured block and only capture the innermost
paragraph-level element. Use a set of depths where capture was triggered, and only emit
text when fully leaving a block that was the trigger (not a nested inner block):

Replace the `_capture_depth` integer approach with a stack-based approach:

```python
class _ArticleTextParser(HTMLParser):
    def __init__(self, domain: str = "") -> None:
        super().__init__()
        self.title = ""
        self.meta: dict[str, str] = {}
        self._in_title = False
        self._capture_stack: list[str] = []  # stack of tags that triggered capture
        self._chunks: list[str] = []
        self._paragraphs: list[str] = []
        self._seen_paragraphs: set[str] = set()  # dedup by content
        self._domain_rules = _domain_rule(domain)

    def handle_starttag(self, tag: str, attrs) -> None:
        attr_map = {key.lower(): value for key, value in attrs}
        if tag == "title":
            self._in_title = True
        if tag == "meta":
            key = (attr_map.get("property") or attr_map.get("name") or "").lower()
            content = attr_map.get("content") or ""
            if key and content:
                self.meta[key] = content.strip()
        if tag in {"p", "article", "div", "section"}:
            css = " ".join(
                filter(None, [attr_map.get("class"), attr_map.get("id")])
            ).lower()
            body_markers = [
                marker.lower() for marker in self._domain_rules.get("body_markers", [])
            ]
            is_capture = (
                tag == "p"
                or any(
                    marker in css
                    for marker in ("article", "story", "content", "body", "main")
                )
                or any(marker and marker in css for marker in body_markers)
            )
            if is_capture:
                self._capture_stack.append(tag)
            else:
                self._capture_stack.append("")  # placeholder to maintain depth

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self._in_title = False
        if tag in {"p", "article", "div", "section"}:
            if not self._capture_stack:
                return
            triggered = self._capture_stack.pop()
            # Only emit text when leaving a tag that triggered capture,
            # and only if this is the outermost active capture context
            if triggered and not any(self._capture_stack):
                text = _collapse_whitespace(" ".join(self._chunks))
                if len(text) >= 60 and text not in self._seen_paragraphs:
                    self._paragraphs.append(text)
                    self._seen_paragraphs.add(text)
                self._chunks = []

    def handle_data(self, data: str) -> None:
        text = _collapse_whitespace(data)
        if not text:
            return
        if self._in_title:
            self.title = f"{self.title} {text}".strip()
        if any(self._capture_stack):
            self._chunks.append(text)
```

Note the `_seen_paragraphs` set as an additional safety net to deduplicate identical
paragraphs even if the stack logic misses an edge case.

---

## Fix 9 — Separate retry and pending batches in the queue worker

**File:** `backend/fetch_historical_queue.py` function `fetch_historical_queue()` and
`backend/db/sources_repo.py` function `get_historical_url_queue_batch()`

**Problem:** `pending` and `retry` items compete for the same batch. Heavy retry backlogs
(from paywalled or unreachable domains) crowd out fresh `pending` articles.

**Fix:**

In `fetch_historical_queue()`, split the batch allocation: reserve 80% of `limit` for
`pending` items and 20% for `retry` items. Make these proportions configurable via
parameters with these defaults.

```python
def fetch_historical_queue(
    limit: int,
    batch_size: int,
    min_domain_interval_seconds: float,
    max_attempts: int,
    dry_run: bool,
    retry_share: float = 0.2,   # new parameter
) -> dict:
    pending_limit = max(1, int(limit * (1.0 - retry_share)))
    retry_limit = max(0, limit - pending_limit)

    pending_rows = get_historical_url_queue_batch(
        limit=pending_limit, statuses=["pending"]
    )
    retry_rows = (
        get_historical_url_queue_batch(limit=retry_limit, statuses=["retry"])
        if retry_limit > 0
        else []
    )
    queue_rows = pending_rows + retry_rows
    # rest of the function unchanged
```

Expose `--retry-share` as a CLI argument in `parse_args()` with default `0.2`.

---

## Fix 10 — Add `--requeue-no-topic` to `requeue_retryable_failures.py`

**File:** `backend/requeue_retryable_failures.py`

This is the companion fix to Fix 1. After keyword changes (Fix 2), previously
`no_topic`-failed articles should be re-processable without a manual SQL query.

Add a new CLI flag `--requeue-no-topic` and a corresponding function:

```python
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
```

In `parse_args()`, add:
```python
parser.add_argument(
    "--requeue-no-topic",
    action="store_true",
    help="Reset all 'no_topic' queue items to 'retry' for re-classification.",
)
```

In `main()`, if `args.requeue_no_topic` is True, call `requeue_no_topic_items()` instead
of (or in addition to) `requeue_retryable_failures()`, and print both results.

---

## Summary of files changed

| File | Fixes |
|------|-------|
| `backend/news.py` | Fix 2 (TOPIC_KEYWORDS expansion), Fix 3 (LOW_SIGNAL_PATTERNS) |
| `backend/fetch_historical_queue.py` | Fix 1 (no_topic status), Fix 4 (non-retryable extraction), Fix 5 (body_text in payload), Fix 6 (topic_guess validation), Fix 7 (language detection), Fix 8 (HTML parser), Fix 9 (batch split) |
| `backend/db/sources_repo.py` | Fix 1 (no_topic status support), Fix 9 (batch split) |
| `backend/requeue_retryable_failures.py` | Fix 1 + Fix 10 (--requeue-no-topic flag) |
| `requirements.txt` | Fix 7 (add `langdetect` if not present) |

## Implementation order

Apply in this order to avoid breakage:

1. Fix 2 (TOPIC_KEYWORDS) — foundation for all classification fixes
2. Fix 3 (LOW_SIGNAL_PATTERNS) — affects the same scoring path
3. Fix 1 (no_topic status) — needs Fix 2 done first so fewer articles hit this path
4. Fix 10 (requeue-no-topic) — companion to Fix 1
5. Fix 4 (non-retryable extraction) — independent, safe at any point
6. Fix 5 (body_text payload) — independent, safe at any point
7. Fix 6 (topic_guess validation) — depends on Fix 2 for infer_article_topics accuracy
8. Fix 7 (language detection) — independent, safe at any point
9. Fix 8 (HTML parser) — independent, safe at any point
10. Fix 9 (batch split) — last, as it changes the scheduler-facing interface
