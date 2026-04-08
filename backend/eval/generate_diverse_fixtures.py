"""
Pull diverse articles from NewsAPI across multiple topic queries,
generate labeled clustering fixtures using Groq, and report
per-topic pass rates against the clustering logic.

Usage (from backend/):
    .venv/bin/python -m eval.generate_diverse_fixtures
"""

import json
import os
import random
import time
from pathlib import Path
from unittest.mock import patch

from groq import Groq

TOPICS = {
    "sudan_conflict": "Sudan RSF SAF war 2026",
    "taiwan_strait": "Taiwan China strait military 2026",
    "kenya_politics": "Kenya election politics 2026",
    "fed_economy": "Federal Reserve interest rates inflation 2026",
    "climate_disaster": "earthquake flood hurricane disaster 2026",
    "eu_politics": "European Union France Germany election 2026",
}

OUTPUT_PATH = Path(__file__).parent / "fixtures" / "clustering_diverse.json"
PAIRS_PER_TOPIC = 20


def fetch_newsapi_articles(query: str, api_key: str, page_size: int = 30) -> list[dict]:
    import urllib.parse
    import urllib.request

    params = urllib.parse.urlencode(
        {
            "q": query,
            "language": "en",
            "pageSize": page_size,
            "sortBy": "publishedAt",
            "apiKey": api_key,
        }
    )
    url = f"https://newsapi.org/v2/everything?{params}"
    with urllib.request.urlopen(url, timeout=10) as resp:
        data = json.loads(resp.read())
    articles = []
    for a in data.get("articles", []):
        if not a.get("title") or not a.get("description"):
            continue
        articles.append(
            {
                "url": a.get("url", ""),
                "title": a.get("title", ""),
                "description": a.get("description", ""),
                "published_at": a.get("publishedAt", ""),
                "source": a.get("source", {}).get("name", ""),
                "source_domain": "",
                "language": "en",
            }
        )
    return articles


def score_pairs(articles: list[dict]) -> list[tuple[int, int, float]]:
    with (
        patch("clustering.get_source_registry", return_value=[]),
        patch("clustering.load_latest_source_reliability", return_value={}),
    ):
        import clustering
        from sklearn.metrics.pairwise import cosine_similarity

        clustering._source_registry_cache = None
        sigs = clustering.build_article_signatures(articles)

    model = clustering.get_semantic_model()
    texts = [s.get("text", "") or "" for s in sigs]
    embeddings = model.encode(texts, convert_to_numpy=True, show_progress_bar=False)
    sim_matrix = cosine_similarity(embeddings)

    scored = []
    for i, sig_i in enumerate(sigs):
        for j in range(i + 1, len(sigs)):
            sig_j = sigs[j]
            raw = float(sim_matrix[i][j])
            hours = clustering._time_distance_hours(
                sig_i.get("published_dt"), sig_j.get("published_dt")
            )
            tw = clustering._temporal_weight(hours)
            lg = {
                e
                for e in sig_i.get("entities", set())
                if clustering._is_likely_location(e)
            }
            rg = {
                e
                for e in sig_j.get("entities", set())
                if clustering._is_likely_location(e)
            }
            geo = 0.70 if (lg and rg and not (lg & rg)) else 1.0
            scored.append((i, j, raw * tw * geo))

    return sorted(scored, key=lambda x: -x[2])


def label_pair(client: Groq, a: dict, b: dict) -> tuple[str, str]:
    def fmt(x):
        return (
            f"Date: {str(x.get('published_at', ''))[:10]}\n"
            f"Source: {x.get('source', '')}\n"
            f"Title: {x.get('title', '')}\n"
            f"Description: {str(x.get('description', ''))[:250]}"
        )

    prompt = f"""Two news articles are shown. Are they reporting on the SAME specific real-world event (same incident, same actors, same location) or DIFFERENT events?

A sequential update about the SAME SPECIFIC INCIDENT is SAME.
Two articles about the same conflict but different incidents are DIFFERENT.

Article 1:
{fmt(a)}

Article 2:
{fmt(b)}

Reply: SAME or DIFFERENT, then a comma, then one sentence why."""

    try:
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=100,
            temperature=0.0,
        )
        text = resp.choices[0].message.content.strip()
        label = "SAME" if text.upper().startswith("SAME") else "DIFFERENT"
        reason = text.split(",", 1)[1].strip() if "," in text else ""
        return label, reason
    except Exception as e:
        return "ERROR", str(e)


def run():
    news_key = os.getenv("NEWS_API_KEY")
    groq_key = os.getenv("GROQ_API_KEY")
    if not news_key:
        raise RuntimeError("NEWS_API_KEY not set")
    if not groq_key:
        raise RuntimeError("GROQ_API_KEY not set")

    client = Groq(api_key=groq_key)
    all_cases = []
    per_topic_results = {}

    for topic_id, query in TOPICS.items():
        print(f"\n[{topic_id}] Fetching: {query!r}")
        try:
            articles = fetch_newsapi_articles(query, news_key)
        except Exception as e:
            print(f"  ERROR fetching: {e}")
            continue

        if len(articles) < 4:
            print(f"  Only {len(articles)} articles, skipping")
            continue

        print(f"  {len(articles)} articles fetched")
        scored = score_pairs(articles)

        high = [(i, j, s) for i, j, s in scored if s >= 0.38]
        medium = [(i, j, s) for i, j, s in scored if 0.20 <= s < 0.38]
        low = [(i, j, s) for i, j, s in scored if s < 0.20]

        n = PAIRS_PER_TOPIC
        sampled = (
            random.sample(high, min(int(n * 0.40), len(high)))
            + random.sample(medium, min(int(n * 0.35), len(medium)))
            + random.sample(low, min(int(n * 0.25), len(low)))
        )[:n]

        topic_cases = []
        for idx, (i, j, score) in enumerate(sampled):
            a, b = articles[i], articles[j]
            label, reason = label_pair(client, a, b)
            if label == "ERROR":
                print(f"  [{idx+1}] ERROR: {reason}")
                continue

            same = label == "SAME"
            case = {
                "id": f"{topic_id}_{idx:03d}",
                "description": reason,
                "topic_bucket": topic_id,
                "articles": [{k: v for k, v in a.items()}, {k: v for k, v in b.items()}],
                "expected_clusters": [[0, 1]] if same else [[0], [1]],
                "expected_behavior": "pass",
                "notes": f"Groq label: {label}. score={round(score,3)}. {reason}",
            }
            topic_cases.append(case)
            print(
                f"  [{idx+1}] {'=' if same else '!='} {round(score,2):5.2f}  {a['title'][:50]!r}"
            )
            time.sleep(0.05)

        with (
            patch("clustering.get_source_registry", return_value=[]),
            patch("clustering.load_latest_source_reliability", return_value={}),
        ):
            import clustering

            clustering._source_registry_cache = None

        passed = 0
        for case in topic_cases:
            arts = case["articles"]
            events = clustering.cluster_articles(arts)
            predicted_urls = set()
            for ev in events:
                urls = [a["url"] for a in ev.get("articles", [])]
                if len(urls) > 1:
                    for u in urls:
                        predicted_urls.add(u)

            expected_same = case["expected_clusters"] == [[0, 1]]
            system_same = (
                arts[0]["url"] in predicted_urls and arts[1]["url"] in predicted_urls
            )
            if expected_same == system_same:
                passed += 1

        rate = passed / len(topic_cases) if topic_cases else 0
        per_topic_results[topic_id] = {
            "cases": len(topic_cases),
            "passed": passed,
            "rate": round(rate, 3),
        }
        all_cases.extend(topic_cases)
        print(f"  Pass rate: {passed}/{len(topic_cases)} ({rate:.0%})")

    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(
            {
                "_comment": "Diverse fixture set generated via NewsAPI + Groq labeling.",
                "cases": all_cases,
            },
            f,
            indent=2,
        )

    print("\n=== PER-TOPIC PASS RATES ===")
    for topic, res in per_topic_results.items():
        print(f"  {topic:30s} {res['passed']:3d}/{res['cases']:3d}  ({res['rate']:.0%})")
    print(f"\nTotal cases written: {len(all_cases)}")
    print(f"Output: {OUTPUT_PATH}")


if __name__ == "__main__":
    run()
