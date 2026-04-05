import argparse
import importlib.util
import json

from spacy.cli import download as spacy_download


DEFAULT_MODELS = [
    "xx_ent_wiki_sm",
    "fr_core_news_md",
    "es_core_news_md",
    "de_core_news_md",
    "pt_core_news_md",
    "it_core_news_md",
    "zh_core_web_md",
]


def model_installed(name: str) -> bool:
    return bool(importlib.util.find_spec(name))


def warm_models(models: list[str]) -> dict[str, str]:
    results: dict[str, str] = {}
    for model in models:
        if model_installed(model):
            results[model] = "already-installed"
            continue
        try:
            spacy_download(model)
            results[model] = "ok"
        except Exception as exc:
            results[model] = f"error: {exc}"
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Download and cache spaCy models for multilingual entity extraction.")
    parser.add_argument("--models", nargs="*", default=DEFAULT_MODELS, help="spaCy model package names to warm.")
    args = parser.parse_args()
    print(json.dumps({"models": args.models, "results": warm_models(args.models)}, indent=2))


if __name__ == "__main__":
    main()
