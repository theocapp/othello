"""Auto-tune clustering parameters by iterative edit-and-eval search.

This script edits tunable constants in backend/clustering.py, runs clustering eval,
and keeps only parameter sets that improve hard failure count (then mean F1).

Usage (from backend/):
    .venv/bin/python -m eval.auto_tune_clustering --iterations 30
"""

from __future__ import annotations

import argparse
import importlib
import random
import re
from dataclasses import dataclass
from pathlib import Path

CLUSTERING_PATH = Path(__file__).resolve().parents[1] / "clustering.py"


@dataclass
class ParamSpec:
    min_value: float
    max_value: float
    step: float


TUNABLE_PARAMS: dict[str, ParamSpec] = {
    "RELATEDNESS_THRESHOLD": ParamSpec(0.30, 0.50, 0.01),
    "GEO_MISMATCH_PENALTY": ParamSpec(0.45, 0.95, 0.02),
    "TOPICAL_BASE_PENALTY": ParamSpec(0.60, 0.95, 0.02),
    "CONSEQUENCE_IRAN_ONLY_PENALTY": ParamSpec(0.40, 0.95, 0.02),
    "LABOR_MILITARY_PENALTY": ParamSpec(0.40, 0.95, 0.02),
    "ACTOR_STRIKE_DIFF_GPE_PENALTY": ParamSpec(0.40, 0.95, 0.02),
    "BASE_CONTEXT_BOOST": ParamSpec(0.05, 0.35, 0.01),
    "SHORT_ACTOR_CONTINUITY_BOOST": ParamSpec(0.05, 0.35, 0.01),
    "LONG_ACTOR_ANCHOR_BOOST": ParamSpec(0.05, 0.35, 0.01),
}


def _read_text() -> str:
    return CLUSTERING_PATH.read_text()


def _extract_params(text: str) -> dict[str, float]:
    params: dict[str, float] = {}
    for name in TUNABLE_PARAMS:
        match = re.search(rf"^{name}\s*=\s*([0-9]*\.?[0-9]+)", text, flags=re.M)
        if not match:
            raise RuntimeError(f"Could not find tunable param in clustering.py: {name}")
        params[name] = float(match.group(1))
    return params


def _render_text(template: str, params: dict[str, float]) -> str:
    updated = template
    for name, value in params.items():
        updated = re.sub(
            rf"^{name}\s*=\s*[0-9]*\.?[0-9]+",
            f"{name} = {value:.3f}",
            updated,
            flags=re.M,
        )
    return updated


def _evaluate_current() -> tuple[int, float]:
    # Import lazily to avoid module state issues while editing clustering.py.
    import clustering
    from eval import eval_clustering

    importlib.reload(clustering)
    results = eval_clustering.run(verbose=False)
    hard_failures = [r for r in results if not r.get("passed")]
    f1_values = [r.get("metrics", {}).get("f1", 0.0) for r in results if "metrics" in r]
    mean_f1 = sum(f1_values) / len(f1_values) if f1_values else 0.0
    return len(hard_failures), mean_f1


def _clamp(value: float, spec: ParamSpec) -> float:
    return max(spec.min_value, min(spec.max_value, value))


def _mutate(params: dict[str, float], rng: random.Random) -> dict[str, float]:
    candidate = dict(params)
    target = rng.choice(list(TUNABLE_PARAMS.keys()))
    spec = TUNABLE_PARAMS[target]

    direction = rng.choice([-1.0, 1.0])
    magnitude = spec.step * rng.choice([1, 1, 2])
    candidate[target] = _clamp(round(candidate[target] + (direction * magnitude), 3), spec)

    # Sometimes co-adjust threshold with boost/penalty to escape local minima.
    if rng.random() < 0.25:
        second = rng.choice(list(TUNABLE_PARAMS.keys()))
        if second != target:
            s2 = TUNABLE_PARAMS[second]
            d2 = rng.choice([-1.0, 1.0]) * s2.step
            candidate[second] = _clamp(round(candidate[second] + d2, 3), s2)

    return candidate


def run(iterations: int, seed: int) -> None:
    rng = random.Random(seed)

    original_text = _read_text()
    template_text = original_text
    best_params = _extract_params(template_text)

    try:
        CLUSTERING_PATH.write_text(_render_text(template_text, best_params))
        best_failures, best_f1 = _evaluate_current()
        print(f"baseline: failures={best_failures} mean_f1={best_f1:.4f}")

        for idx in range(1, iterations + 1):
            candidate = _mutate(best_params, rng)
            CLUSTERING_PATH.write_text(_render_text(template_text, candidate))
            fail_count, mean_f1 = _evaluate_current()

            improved = (fail_count < best_failures) or (
                fail_count == best_failures and mean_f1 > best_f1
            )

            status = "ACCEPT" if improved else "reject"
            print(
                f"iter={idx:03d} {status} failures={fail_count} mean_f1={mean_f1:.4f}"
            )

            if improved:
                best_params = candidate
                best_failures = fail_count
                best_f1 = mean_f1

        # Persist best params.
        CLUSTERING_PATH.write_text(_render_text(template_text, best_params))
        print("best params:")
        for name in sorted(best_params.keys()):
            print(f"  {name}={best_params[name]:.3f}")
        print(f"best score: failures={best_failures} mean_f1={best_f1:.4f}")

    except Exception:
        # Restore original on unexpected failure.
        CLUSTERING_PATH.write_text(original_text)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Auto-tune clustering constants by eval feedback")
    parser.add_argument("--iterations", type=int, default=30, help="Search iterations (default: 30)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    args = parser.parse_args()
    run(iterations=args.iterations, seed=args.seed)
