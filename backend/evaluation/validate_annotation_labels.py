"""Validate JSONL annotation labels against draft schema rules."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))

from evaluation.labels import validate_annotation_record  # noqa: E402


def _validate_jsonl(path: Path) -> list[str]:
    errors: list[str] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                record = json.loads(text)
            except json.JSONDecodeError as exc:
                errors.append(f"line {line_number}: invalid json ({exc.msg})")
                continue

            if not isinstance(record, dict):
                errors.append(f"line {line_number}: record must be a JSON object")
                continue

            record_errors = validate_annotation_record(record)
            for err in record_errors:
                errors.append(f"line {line_number}: {err}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate annotation JSONL labels")
    parser.add_argument("input", help="Path to annotation JSONL file")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"error: file not found: {input_path}")
        return 2

    errors = _validate_jsonl(input_path)
    if errors:
        print(f"validation failed with {len(errors)} error(s)")
        for err in errors:
            print(f"- {err}")
        return 1

    print(f"validation passed: {input_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
