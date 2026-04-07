"""Evaluate importance scoring via pairwise ranking tests.

Calls story_materialization._build_importance_scoring_artifacts directly.
Patches source registry so tier-1 bonuses can be tested with/without registry.

For each fixture pair (A, B), checks that the expected_higher event scores higher.
Also prints the score breakdown so you can see which components drove the result.

Run from the backend/ directory:
    python -m eval.eval_importance
"""

import json
import sys
from pathlib import Path
from unittest.mock import patch

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "importance.json"


def _load_fixtures():
    with open(FIXTURE_PATH) as f:
        return json.load(f)["cases"]


def _score_event(fn, event_data: dict, registry_by_domain: dict) -> tuple[float, list[str], dict]:
    """Call _build_importance_scoring_artifacts with the fixture event data."""
    return fn(
        event_data,
        linked_structured_ids=event_data.get("linked_structured_ids", []),
        reliability_by_source={},
        registry_by_domain=registry_by_domain,
        latest_observation=event_data.get("latest_observation"),
        structured_meta_by_id=event_data.get("structured_meta", {}),
    )


def run(verbose: bool = True) -> list[dict]:
    import story_materialization as sm

    fn = sm._build_importance_scoring_artifacts
    if True:
        cases = _load_fixtures()
        results = []

        for case in cases:
            event_a = case["event_a"]
            event_b = case["event_b"]
            expected_higher = case["expected_higher"]  # "a" or "b"
            
            # Allow test cases to provide a source registry
            registry_by_domain = case.get("registry_by_domain", {})

            try:
                score_a, reasons_a, breakdown_a = _score_event(fn, event_a, registry_by_domain)
                score_b, reasons_b, breakdown_b = _score_event(fn, event_b, registry_by_domain)
            except Exception as exc:
                results.append({"id": case["id"], "error": str(exc), "passed": False})
                continue

            actual_higher = "a" if score_a > score_b else ("b" if score_b > score_a else "tie")
            passed = actual_higher == expected_higher

            results.append({
                "id": case["id"],
                "description": case["description"],
                "passed": passed,
                "expected_higher": expected_higher,
                "actual_higher": actual_higher,
                "score_a": round(score_a, 2),
                "score_b": round(score_b, 2),
                "margin": round(abs(score_a - score_b), 2),
                "reasons_a": reasons_a,
                "reasons_b": reasons_b,
                "notes": case.get("notes", ""),
            })

        if verbose:
            _print_results(results)

        return results


def _print_results(results: list[dict]) -> None:
    print("\n=== IMPORTANCE EVAL ===\n")
    for r in results:
        if "error" in r:
            print(f"  [ERROR] {r['id']}: {r['error']}")
            continue

        status = "PASS" if r["passed"] else "FAIL"
        print(f"  [{status}] {r['id']}")
        print(f"         A={r['score_a']}  B={r['score_b']}  margin={r['margin']}  expected={r['expected_higher'].upper()} higher  got={r['actual_higher'].upper()}")
        print(f"         A reasons: {r['reasons_a']}")
        print(f"         B reasons: {r['reasons_b']}")
        if not r["passed"]:
            print(f"         note: {r.get('notes', '')}")

    total = len(results)
    passed = sum(1 for r in results if r.get("passed"))
    print(f"\n  {passed}/{total} passed\n")


if __name__ == "__main__":
    results = run()
    sys.exit(0 if all(r.get("passed") for r in results) else 1)
