"""Evaluate event_identity.resolve_canonical_event_id against labeled fixtures.

No DB dependencies — the scoring functions are pure.

For each case we check:
  - Whether the correct candidate was matched (or correctly rejected)
  - Whether the score fell in the expected range

Run from the backend/ directory:
    python -m eval.eval_identity
"""

import json
import sys
from pathlib import Path

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "identity.json"


def _load_fixtures():
    with open(FIXTURE_PATH) as f:
        return json.load(f)["cases"]


def run(verbose: bool = True) -> list[dict]:
    from event_identity import resolve_canonical_event_id, score_observation_against_candidate

    cases = _load_fixtures()
    results = []

    for case in cases:
        observation = case["observation"]
        candidates = case["candidates"]
        expected_match_index = case.get("expected_match_index")  # None = expect no match
        expected_action = case.get("expected_action")
        expected_min_score = case.get("expected_min_score")
        expected_max_score = case.get("expected_max_score")

        try:
            event_id, decision = resolve_canonical_event_id(
                observation_key=observation["observation_key"],
                observation=observation,
                candidates=candidates,
            )
        except Exception as exc:
            results.append({"id": case["id"], "error": str(exc), "passed": False})
            continue

        action = decision.get("action")
        matched_id = decision.get("matched_event_id")
        confidence = decision.get("confidence")

        # Determine which candidate index was matched (if any)
        matched_index = None
        if matched_id:
            for i, c in enumerate(candidates):
                if c.get("event_id") == matched_id:
                    matched_index = i
                    break

        # Score the best candidate individually so we can check score bounds
        best_score = None
        best_score_reasons = None
        if candidates:
            scored = [
                score_observation_against_candidate(observation, c)
                for c in candidates
            ]
            best_score, best_score_reasons = max(scored, key=lambda x: x[0])

        checks = []

        # Check: correct match / no-match
        if expected_match_index is None:
            match_correct = action == "created_new"
            checks.append(("no_match", match_correct, f"expected created_new, got {action}"))
        else:
            match_correct = matched_index == expected_match_index
            checks.append((
                "correct_candidate",
                match_correct,
                f"expected candidate[{expected_match_index}], got candidate[{matched_index}] (action={action})",
            ))

        # Check: expected action
        if expected_action:
            action_correct = action == expected_action
            checks.append(("action", action_correct, f"expected {expected_action}, got {action}"))

        # Check: score bounds
        if expected_min_score is not None and best_score is not None:
            score_ok = best_score >= expected_min_score
            checks.append((
                "min_score",
                score_ok,
                f"expected score >= {expected_min_score}, got {round(best_score, 4)}",
            ))

        if expected_max_score is not None and best_score is not None:
            score_ok = best_score <= expected_max_score
            checks.append((
                "max_score",
                score_ok,
                f"expected score <= {expected_max_score}, got {round(best_score, 4)}",
            ))

        passed = all(ok for _, ok, _ in checks)
        failed_checks = [(name, msg) for name, ok, msg in checks if not ok]

        results.append({
            "id": case["id"],
            "description": case["description"],
            "passed": passed,
            "action": action,
            "matched_index": matched_index,
            "best_score": round(best_score, 4) if best_score is not None else None,
            "confidence": confidence,
            "failed_checks": failed_checks,
            "notes": case.get("notes", ""),
        })

    if verbose:
        _print_results(results)

    return results


def _print_results(results: list[dict]) -> None:
    print("\n=== IDENTITY EVAL ===\n")
    for r in results:
        if "error" in r:
            print(f"  [ERROR] {r['id']}: {r['error']}")
            continue

        status = "PASS" if r["passed"] else "FAIL"
        score_str = f"best_score={r['best_score']}" if r["best_score"] is not None else "no_candidates"
        print(f"  [{status}] {r['id']}  action={r['action']}  {score_str}")
        for name, msg in r.get("failed_checks", []):
            print(f"         FAIL [{name}]: {msg}")

    total = len(results)
    passed = sum(1 for r in results if r.get("passed"))
    print(f"\n  {passed}/{total} passed\n")


if __name__ == "__main__":
    results = run()
    sys.exit(0 if all(r.get("passed") for r in results) else 1)
