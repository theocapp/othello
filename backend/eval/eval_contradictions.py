"""Evaluate contradiction adjudication on labeled fixtures.

Runs contradiction adjudication logic directly (not full pipeline)
and checks most_credible_source against expected values.

Run from backend/:
    python -m eval.eval_contradictions
"""

import json
import sys
from pathlib import Path
from unittest.mock import patch

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "contradictions.json"


def _load_fixtures():
    with open(FIXTURE_PATH) as f:
        return json.load(f)["cases"]


def _registry_rows():
    return [
        {
            "source_name": "Reuters",
            "source_domain": "reuters.com",
            "source_type": "article",
            "trust_tier": "tier_1",
            "region": "global",
        },
        {
            "source_name": "Associated Press",
            "source_domain": "apnews.com",
            "source_type": "article",
            "trust_tier": "tier_1",
            "region": "global",
        },
        {
            "source_name": "Metro Times",
            "source_domain": "metrotimes.example",
            "source_type": "article",
            "trust_tier": "tier_2",
            "region": "global",
        },
        {
            "source_name": "Global Ledger",
            "source_domain": "globalledger.example",
            "source_type": "article",
            "trust_tier": "tier_2",
            "region": "global",
        },
        {
            "source_name": "Local War Blog",
            "source_domain": "blog.example",
            "source_type": "article",
            "trust_tier": "tier_3",
            "region": "global",
        },
    ]


def run(verbose: bool = True) -> list[dict]:
    with (
        patch("contradictions.get_source_registry", return_value=_registry_rows()),
        patch("contradictions.load_latest_source_reliability", return_value={}),
    ):
        import contradictions  # noqa: PLC0415

        contradictions._source_registry_cache = None
        contradictions._source_reliability_cache = {}
        contradictions._source_reliability_cache_time = {}

        cases = _load_fixtures()
        results = []

        for case in cases:
            expected = case["expected_most_credible_source"]
            event = case["event"]
            articles = event.get("articles", [])

            try:
                if len(articles) < 2:
                    raise ValueError("fixture requires two articles")
                actual, _ = contradictions._adjudicate_most_credible_source(
                    articles[0], articles[1]
                )
                actual = actual or "unresolved"
                details = "adjudication result"
            except Exception as exc:
                results.append({"id": case["id"], "passed": False, "error": str(exc)})
                continue

            passed = actual == expected
            results.append(
                {
                    "id": case["id"],
                    "description": case.get("description", ""),
                    "expected": expected,
                    "actual": actual,
                    "passed": passed,
                    "details": details,
                }
            )

    if verbose:
        _print_results(results)

    return results


def _print_results(results: list[dict]) -> None:
    print("\n=== CONTRADICTIONS EVAL ===\n")
    for row in results:
        if "error" in row:
            print(f"  [ERROR] {row['id']}: {row['error']}")
            continue
        status = "PASS" if row["passed"] else "FAIL"
        print(f"  [{status}] {row['id']}")
        print(f"         expected={row['expected']}  got={row['actual']}")
        if not row["passed"]:
            print(f"         note: {row.get('details', '')}")

    total = len(results)
    passed = sum(1 for row in results if row.get("passed"))
    print(f"\n  {passed}/{total} passed\n")


if __name__ == "__main__":
    rows = run()
    sys.exit(0 if all(row.get("passed") for row in rows) else 1)
