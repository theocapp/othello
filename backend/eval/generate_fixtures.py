"""Generate labeled clustering fixtures from real corpus articles using an LLM.

Pulls articles from the DB, scores every pair with the existing relatedness
function, samples pairs from three buckets (likely-same, ambiguous,
likely-different), then asks the configured model to label each pair
SAME or DIFFERENT.
Saves results to eval/fixtures/clustering_generated.json.

Usage (from backend/):
    .venv/bin/python -m eval.generate_fixtures [--count N] [--append] [--provider anthropic|groq]

Options:
    --count N    Number of pairs to label (default 50)
    --append     Append to existing generated fixtures instead of overwriting
    --provider   Label provider: anthropic or groq (default anthropic)
    --model      Override model name for selected provider
"""

import argparse
import json
import os
import random
import sys
import time
from urllib import request as urllib_request
from urllib import error as urllib_error
from itertools import combinations
from pathlib import Path
from unittest.mock import patch

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "clustering_generated.json"
DEFAULT_ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"
DEFAULT_GROQ_MODEL = "llama-3.3-70b-versatile"


def _fetch_articles() -> list[dict]:
    from db.common import _connect
    with _connect() as conn:
        rows = conn.execute("""
            SELECT url, title, description, source, source_domain, published_at, language
            FROM articles
            WHERE language = 'en'
              AND description IS NOT NULL
              AND description != ''
              AND description != title
              AND title IS NOT NULL
              AND title != ''
            ORDER BY published_at DESC
            LIMIT 500
        """).fetchall()
    return [dict(r) for r in rows]


def _score_all_pairs(articles: list[dict]) -> list[tuple[int, int, float]]:
    """Return (i, j, relatedness_score) for all pairs, sorted by score desc.

    Batches all embeddings in a single model.encode() call so this is fast
    regardless of corpus size.
    """
    with (
        patch("clustering.get_source_registry", return_value=[]),
        patch("clustering.load_latest_source_reliability", return_value={}),
    ):
        import clustering
        from sklearn.metrics.pairwise import cosine_similarity
        import numpy as np

        clustering._source_registry_cache = None
        sigs = clustering.build_article_signatures(articles)

    model = clustering.get_semantic_model()

    # Batch encode all articles at once
    texts = [sig.get("text", "") or "" for sig in sigs]
    embeddings = model.encode(texts, convert_to_numpy=True, show_progress_bar=True)

    # Compute full similarity matrix at once
    sim_matrix = cosine_similarity(embeddings)

    scored = []
    for i, sig_i in enumerate(sigs):
        for j in range(i + 1, len(sigs)):
            sig_j = sigs[j]
            raw_sim = float(sim_matrix[i][j])

            # Apply the same temporal and geo penalties as relatedness_score
            hours_apart = clustering._time_distance_hours(
                sig_i.get("published_dt"), sig_j.get("published_dt")
            )
            time_weight = clustering._temporal_weight(hours_apart)

            left_gpes = {e for e in sig_i.get("entities", set()) if clustering._is_likely_location(e)}
            right_gpes = {e for e in sig_j.get("entities", set()) if clustering._is_likely_location(e)}
            geo_penalty = 0.70 if (left_gpes and right_gpes and not (left_gpes & right_gpes)) else 1.0

            score = raw_sim * time_weight * geo_penalty
            scored.append((i, j, score))

    scored.sort(key=lambda x: -x[2])
    return scored


def _sample_pairs(scored: list[tuple], n: int) -> list[tuple[int, int, float]]:
    """Sample n pairs across three buckets: likely-same, ambiguous, likely-different."""
    # Bucket thresholds based on RELATEDNESS_THRESHOLD = 0.40 (cosine similarity)
    high   = [(i, j, s) for i, j, s in scored if s >= 0.40]          # system says same
    medium = [(i, j, s) for i, j, s in scored if 0.20 <= s < 0.40]   # ambiguous
    low    = [(i, j, s) for i, j, s in scored if s < 0.20]            # system says different

    # Allocate: 40% high, 35% medium, 25% low
    n_high   = max(1, int(n * 0.40))
    n_medium = max(1, int(n * 0.35))
    n_low    = max(1, n - n_high - n_medium)

    sampled = (
        random.sample(high,   min(n_high,   len(high)))
        + random.sample(medium, min(n_medium, len(medium)))
        + random.sample(low,    min(n_low,    len(low)))
    )
    random.shuffle(sampled)
    return sampled[:n]


def _format_article_for_prompt(article: dict) -> str:
    date = str(article.get("published_at", ""))[:10]
    return (
        f"Date: {date}\n"
        f"Source: {article.get('source', 'unknown')}\n"
        f"Title: {article.get('title', '')}\n"
        f"Description: {str(article.get('description', ''))[:300]}"
    )


def _build_prompt(article_a: dict, article_b: dict) -> str:
    return f"""Two news articles are shown below. Decide if they are reporting on the SAME specific real-world event (same incident, same actors, same location) or DIFFERENT events.

A SAME label requires concrete incident continuity: the reports must describe the same specific occurrence.
Use DIFFERENT by default when uncertain.

A sequential update about the SAME SPECIFIC INCIDENT is still the SAME event
(e.g. a strike on day 1, and casualty updates from that same strike on day 3).
Two articles about the SAME CONFLICT but DIFFERENT INCIDENTS are DIFFERENT events
(e.g. a specific airstrike vs. an oil market analysis caused by the war).

Label as DIFFERENT if ANY of these are true:
- One article is macro analysis/economic impact/opinion and the other is a specific battlefield incident.
- The incidents occur in different locations without explicit evidence they are the same occurrence.
- The actors/actions differ (for example, diplomatic statement vs airstrike vs market reaction).

Label as SAME only if there is explicit evidence of same-incident linkage:
- Same place + same actors + same action type, or
- Follow-up casualty/damage/update tied to that exact earlier incident.

Article 1:
{_format_article_for_prompt(article_a)}

Article 2:
{_format_article_for_prompt(article_b)}

Reply with exactly: SAME or DIFFERENT, then a comma, then one sentence explaining why."""


def _parse_label_response(text: str) -> tuple[str, str]:
    cleaned = (text or "").strip()
    if "," in cleaned:
        label_part, reasoning = cleaned.split(",", 1)
    else:
        label_part, reasoning = cleaned, ""
    label = "SAME" if "SAME" in label_part.upper() else "DIFFERENT"
    return label, reasoning.strip()


def _label_pair_anthropic(client, model: str, article_a: dict, article_b: dict) -> tuple[str, str]:
    prompt = _build_prompt(article_a, article_b)

    try:
        response = client.messages.create(
            model=model,
            max_tokens=120,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()
        return _parse_label_response(text)
    except Exception as exc:
        return "ERROR", str(exc)


def _label_pair_groq(api_key: str, model: str, article_a: dict, article_b: dict) -> tuple[str, str]:
    prompt = _build_prompt(article_a, article_b)
    payload = {
        "model": model,
        "temperature": 0,
        "max_tokens": 140,
        "messages": [{"role": "user", "content": prompt}],
    }
    for attempt in range(3):
        req = urllib_request.Request(
            url="https://api.groq.com/openai/v1/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
                "User-Agent": "othello-v2-fixture-generator/1.0",
            },
            method="POST",
        )
        try:
            with urllib_request.urlopen(req, timeout=30) as response:
                body = json.loads(response.read().decode("utf-8"))
            text = (
                body.get("choices", [{}])[0]
                .get("message", {})
                .get("content", "")
                .strip()
            )
            if not text:
                return "ERROR", f"Empty Groq response: {body}"
            return _parse_label_response(text)
        except urllib_error.HTTPError as exc:
            response_body = exc.read().decode("utf-8", errors="ignore")
            # Retry transient gateway/rate errors and occasional edge blocks.
            if exc.code in {403, 429, 500, 502, 503, 504} and attempt < 2:
                time.sleep(0.8 * (attempt + 1))
                continue
            return "ERROR", f"HTTP {exc.code}: {response_body[:280]}"
        except Exception as exc:
            if attempt < 2:
                time.sleep(0.8 * (attempt + 1))
                continue
            return "ERROR", str(exc)


def _build_fixture_case(
    idx: int,
    article_a: dict,
    article_b: dict,
    label: str,
    reasoning: str,
    relatedness_score: float,
    provider_name: str,
) -> dict:
    same = label == "SAME"
    return {
        "id": f"generated_{idx:04d}",
        "description": reasoning or f"Generated pair (relatedness={round(relatedness_score, 2)})",
        "articles": [
            {k: v for k, v in article_a.items() if k in ("url", "title", "description", "published_at", "source", "source_domain")},
            {k: v for k, v in article_b.items() if k in ("url", "title", "description", "published_at", "source", "source_domain")},
        ],
        "expected_clusters": [[0, 1]] if same else [[0], [1]],
        "expected_behavior": "pass",
        "notes": f"{provider_name} label: {label}. System relatedness_score={round(relatedness_score, 2)}. {reasoning}",
    }


def run(
    count: int = 50,
    append: bool = False,
    provider: str = "anthropic",
    model: str | None = None,
    allow_url_reuse: bool = False,
) -> None:
    provider = provider.strip().lower()
    if provider not in {"anthropic", "groq"}:
        print("ERROR: --provider must be one of: anthropic, groq", file=sys.stderr)
        sys.exit(1)

    label_fn = None
    provider_name = ""
    model_name = model

    if provider == "anthropic":
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            print("ERROR: ANTHROPIC_API_KEY not set", file=sys.stderr)
            sys.exit(1)
        try:
            from anthropic import Anthropic
        except ImportError:
            print("ERROR: anthropic package not installed. Run: pip install anthropic", file=sys.stderr)
            sys.exit(1)
        client = Anthropic(api_key=api_key)
        model_name = model_name or DEFAULT_ANTHROPIC_MODEL
        label_fn = lambda a, b: _label_pair_anthropic(client, model_name, a, b)
        provider_name = "Anthropic"
    else:
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            print("ERROR: GROQ_API_KEY not set", file=sys.stderr)
            sys.exit(1)
        model_name = model_name or DEFAULT_GROQ_MODEL
        label_fn = lambda a, b: _label_pair_groq(api_key, model_name, a, b)
        provider_name = "Groq"

    print(f"Using label provider: {provider_name} ({model_name})")

    print(f"Fetching articles from corpus...", flush=True)
    articles = _fetch_articles()
    if len(articles) < 2:
        print("ERROR: Not enough articles in corpus (need at least 2)", file=sys.stderr)
        sys.exit(1)
    print(f"  {len(articles)} articles found")

    print("Scoring all pairs...", flush=True)
    scored_pairs = _score_all_pairs(articles)
    print(f"  {len(scored_pairs)} pairs scored")

    sampled = _sample_pairs(scored_pairs, count)
    print(f"  Sampled {len(sampled)} pairs across score buckets")

    # Deduplicate URLs — don't label a pair if either article URL already appears in existing fixtures
    existing_urls: set[str] = set()
    if append and FIXTURE_PATH.exists() and not allow_url_reuse:
        with open(FIXTURE_PATH) as f:
            existing = json.load(f)
        for case in existing.get("cases", []):
            for art in case.get("articles", []):
                existing_urls.add(art.get("url", ""))
        print(f"  {len(existing.get('cases', []))} existing generated cases found")
    elif append and FIXTURE_PATH.exists() and allow_url_reuse:
        with open(FIXTURE_PATH) as f:
            existing = json.load(f)
        print(
            f"  {len(existing.get('cases', []))} existing generated cases found (URL reuse allowed)"
        )

    cases = []
    skipped = 0
    for seq, (i, j, score) in enumerate(sampled):
        a, b = articles[i], articles[j]
        if not allow_url_reuse and (a["url"] in existing_urls or b["url"] in existing_urls):
            skipped += 1
            continue

        label, reasoning = label_fn(a, b)
        if label == "ERROR":
            print(f"  [{seq+1}/{len(sampled)}] ERROR: {reasoning}", flush=True)
            continue

        case = _build_fixture_case(seq, a, b, label, reasoning, score, provider_name)
        cases.append(case)

        indicator = "=" if label == "SAME" else "≠"
        print(f"  [{seq+1}/{len(sampled)}] {indicator} score={round(score,2):6.2f}  {a['title'][:45]!r}  vs  {b['title'][:45]!r}", flush=True)

        # Small delay to avoid hitting rate limits
        time.sleep(0.1)

    if skipped:
        print(f"  Skipped {skipped} pairs with URLs already in existing fixtures")

    # Merge with existing if appending
    if append and FIXTURE_PATH.exists():
        with open(FIXTURE_PATH) as f:
            existing_data = json.load(f)
        all_cases = existing_data.get("cases", []) + cases
    else:
        all_cases = cases

    # Re-sequence IDs
    for idx, case in enumerate(all_cases):
        case["id"] = f"generated_{idx:04d}"

    output = {
        "_comment": (
            "Auto-generated fixtures. Do not edit by hand — re-run generate_fixtures.py instead. "
            f"Label source: {provider_name} {model_name}."
        ),
        "cases": all_cases,
    }

    FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(FIXTURE_PATH, "w") as f:
        json.dump(output, f, indent=2)

    same_count = sum(1 for c in cases if c["expected_clusters"] == [[0, 1]])
    diff_count = len(cases) - same_count
    print(f"\nWrote {len(cases)} new cases ({same_count} SAME, {diff_count} DIFFERENT) to {FIXTURE_PATH}")
    print(f"Total cases in file: {len(all_cases)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate labeled clustering fixtures from corpus articles")
    parser.add_argument("--count", type=int, default=50, help="Number of pairs to label (default 50)")
    parser.add_argument("--append", action="store_true", help="Append to existing generated fixtures")
    parser.add_argument(
        "--provider",
        choices=["anthropic", "groq"],
        default="anthropic",
        help="Label provider to use (default: anthropic)",
    )
    parser.add_argument(
        "--model",
        default=None,
        help="Optional model override for selected provider",
    )
    parser.add_argument(
        "--allow-url-reuse",
        action="store_true",
        help="Allow appending new cases that reuse article URLs from existing generated fixtures",
    )
    args = parser.parse_args()
    run(
        count=args.count,
        append=args.append,
        provider=args.provider,
        model=args.model,
        allow_url_reuse=args.allow_url_reuse,
    )
