"""Evaluate clustering.cluster_articles against labeled fixtures.

Metric: pairwise precision, recall, F1.
  - A "pair" is two articles that should (or should not) be in the same cluster.
  - Precision: of pairs the system grouped together, what fraction should be together?
  - Recall: of pairs that should be together, what fraction did the system group?

Run from the backend/ directory:
    python -m eval.eval_clustering
"""

import json
import sys
from itertools import combinations
from pathlib import Path
from unittest.mock import patch

FIXTURE_PATHS = [
    Path(__file__).parent / "fixtures" / "clustering.json",
    Path(__file__).parent / "fixtures" / "clustering_generated.json",
]


def _load_fixtures():
    cases = []
    for path in FIXTURE_PATHS:
        if path.exists():
            with open(path) as f:
                cases.extend(json.load(f)["cases"])
    return cases


def _predicted_pairs(events: list[dict]) -> set[frozenset]:
    """Extract all (i, j) pairs that landed in the same cluster, by article URL."""
    pairs = set()
    for event in events:
        urls = [a["url"] for a in event.get("articles", [])]
        for u, v in combinations(urls, 2):
            pairs.add(frozenset({u, v}))
    return pairs


def _expected_pairs(articles: list[dict], expected_clusters: list[list[int]]) -> set[frozenset]:
    pairs = set()
    for cluster in expected_clusters:
        for i, j in combinations(cluster, 2):
            pairs.add(frozenset({articles[i]["url"], articles[j]["url"]}))
    return pairs


def _pairwise_metrics(predicted: set, expected: set) -> dict:
    tp = len(predicted & expected)
    precision = tp / len(predicted) if predicted else 1.0
    recall = tp / len(expected) if expected else 1.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0
    return {"precision": round(precision, 3), "recall": round(recall, 3), "f1": round(f1, 3)}


def run(verbose: bool = True) -> list[dict]:
    # Patch out the DB calls so the eval is self-contained.
    # source registry and reliability affect source quality weights but not clustering logic.
    with (
        patch("clustering.get_source_registry", return_value=[]),
        patch("clustering.load_latest_source_reliability", return_value={}),
    ):
        import clustering  # noqa: PLC0415 — import after patch setup
        clustering._source_registry_cache = None

        cases = _load_fixtures()
        results = []

        for case in cases:
            articles = case["articles"]
            expected_clusters = case["expected_clusters"]
            expected_behavior = case.get("expected_behavior", "pass")

            try:
                events = clustering.cluster_articles(articles)
            except Exception as exc:
                results.append({
                    "id": case["id"],
                    "error": str(exc),
                    "passed": False,
                })
                continue

            predicted = _predicted_pairs(events)
            expected = _expected_pairs(articles, expected_clusters)
            metrics = _pairwise_metrics(predicted, expected)

            # A case "passes" if F1 == 1.0 (perfect pairwise match).
            system_correct = metrics["f1"] == 1.0
            # expected_behavior tells us whether the system is expected to pass or fail.
            # A case marked "fail" documents a known bug — we want to see it fail
            # so we know when a fix lands.
            if expected_behavior == "pass":
                passed = system_correct
            else:
                # "fail" cases: the system is expected to get it wrong.
                # We report "documented bug still present" (good) vs "unexpectedly fixed" (also good).
                passed = not system_correct  # still failing as expected

            predicted_cluster_sizes = [len(e.get("articles", [])) for e in events]

            results.append({
                "id": case["id"],
                "description": case["description"],
                "expected_behavior": expected_behavior,
                "system_correct": system_correct,
                "passed": passed,
                "metrics": metrics,
                "predicted_cluster_sizes": predicted_cluster_sizes,
                "notes": case.get("notes", ""),
            })

        if verbose:
            _print_results(results)

        return results


def _print_results(results: list[dict]) -> None:
    print("\n=== CLUSTERING EVAL ===\n")
    all_pass = True
    for r in results:
        if "error" in r:
            status = "ERROR"
            all_pass = False
        elif r["passed"]:
            if r["expected_behavior"] == "fail":
                status = "OK  (known bug still present)"
            else:
                status = "PASS"
        else:
            if r["expected_behavior"] == "fail":
                status = "FIXED (known bug no longer triggers — update fixture!)"
            else:
                status = "FAIL"
                all_pass = False

        m = r.get("metrics", {})
        sizes = r.get("predicted_cluster_sizes", [])
        print(f"  [{status}] {r['id']}")
        if "error" in r:
            print(f"         error: {r['error']}")
        else:
            print(f"         P={m.get('precision')} R={m.get('recall')} F1={m.get('f1')}  clusters={sizes}")
            if not r["passed"] and r["expected_behavior"] == "pass":
                print(f"         note: {r.get('notes', '')}")

    total = len(results)
    passed = sum(1 for r in results if r.get("passed"))
    print(f"\n  {passed}/{total} cases as expected")
    if all_pass:
        print("  All pass-expected cases correct.\n")


if __name__ == "__main__":
    results = run()
    failures = [r for r in results if not r.get("passed") and r.get("expected_behavior") == "pass"]
    sys.exit(1 if failures else 0)
