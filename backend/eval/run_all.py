"""Run all eval modules and print a summary.

Usage (from backend/ directory):
    python -m eval.run_all

Exit code 0 if all pass-expected cases pass, 1 otherwise.

What "passing" means per module:
    - clustering:  F1=1.0 on all ground-truth cases.
                                 Only cases explicitly marked with
                                 expected_behavior=fail and fail_reason=known_architectural_limit
                                 are excluded from hard-failure counts.
  - identity:    All score/match assertions pass.
  - importance:  Correct pairwise ranking on all cases.
                 Cases that reveal a known scoring limitation are still
                 counted as failures — they should be fixed.
"""

import sys

from eval.eval_clustering import run as run_clustering
from eval.eval_identity import run as run_identity
from eval.eval_importance import run as run_importance


def main():
    clustering_results = run_clustering(verbose=True)
    identity_results = run_identity(verbose=True)
    importance_results = run_importance(verbose=True)

    # Hard failures are any clustering cases that did not pass.
    clustering_failures = [r for r in clustering_results if not r.get("passed")]
    identity_failures = [r for r in identity_results if not r.get("passed")]
    importance_failures = [r for r in importance_results if not r.get("passed")]

    # Exempt known-limit cases that are unexpectedly fixed.
    known_bugs = [
        r for r in clustering_results
        if r.get("exempt_known_limit") and r.get("system_correct")
    ]

    print("=== SUMMARY ===\n")

    total_hard = len(clustering_failures) + len(identity_failures) + len(importance_failures)
    if total_hard == 0:
        print("  All pass-expected cases: OK")
    else:
        if clustering_failures:
            print(f"  Clustering failures ({len(clustering_failures)}):")
            for r in clustering_failures:
                print(f"    - {r['id']}: P={r['metrics']['precision']} R={r['metrics']['recall']} F1={r['metrics']['f1']}")
        if identity_failures:
            print(f"  Identity failures ({len(identity_failures)}):")
            for r in identity_failures:
                for name, msg in r.get("failed_checks", []):
                    print(f"    - {r['id']} [{name}]: {msg}")
        if importance_failures:
            print(f"  Importance failures ({len(importance_failures)}):")
            for r in importance_failures:
                print(f"    - {r['id']}: expected {r['expected_higher'].upper()} higher, got {r['actual_higher'].upper()} (A={r['score_a']} B={r['score_b']})")

    if known_bugs:
        print(f"\n  Known limits now fixed ({len(known_bugs)}) — update fixtures to expected_behavior=pass:")
        for r in known_bugs:
            print(f"    - {r['id']}")

    known_bug_count = len([
        r for r in clustering_results
        if r.get("exempt_known_limit") and not r.get("system_correct")
    ])
    if known_bug_count:
        print(f"\n  Known limits still present: {known_bug_count} (see clustering eval above)")

    print()
    return 1 if total_hard > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
