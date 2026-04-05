import argparse
import json

from analyst import LOCAL_TRANSLATION_MODEL_MAP, warm_local_translation_models


DEFAULT_LANGUAGES = [
    "zh",
    "ar",
    "uk",
    "tr",
    "el",
    "he",
    "sq",
    "id",
    "cs",
    "hr",
    "ko",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Download and cache local translation models for Othello V2.")
    parser.add_argument(
        "--languages",
        nargs="*",
        default=DEFAULT_LANGUAGES,
        help="Language codes to warm. Defaults to the highest-volume remaining non-English corpus languages.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Warm every language currently configured in analyst.LOCAL_TRANSLATION_MODEL_MAP.",
    )
    args = parser.parse_args()

    languages = sorted(LOCAL_TRANSLATION_MODEL_MAP) if args.all else args.languages
    results = warm_local_translation_models(languages)
    print(json.dumps({"languages": languages, "results": results}, indent=2))


if __name__ == "__main__":
    main()
