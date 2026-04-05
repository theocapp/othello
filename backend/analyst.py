import json
import os
from datetime import date

from groq import Groq

MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
TRANSLATION_PROVIDER = os.getenv("OTHELLO_TRANSLATION_PROVIDER", "local").lower()
ALLOW_GROQ_TRANSLATION_FALLBACK = os.getenv("OTHELLO_ALLOW_GROQ_TRANSLATION_FALLBACK", "true").lower() == "true"
LOCAL_TRANSLATION_FILES_ONLY = os.getenv("OTHELLO_TRANSLATION_LOCAL_FILES_ONLY", "true").lower() == "true"

LOCAL_TRANSLATION_MODEL_MAP = {
    "fr": "Helsinki-NLP/opus-mt-ROMANCE-en",
    "es": "Helsinki-NLP/opus-mt-ROMANCE-en",
    "de": "Helsinki-NLP/opus-mt-de-en",
    "el": "Helsinki-NLP/opus-mt-tc-big-el-en",
    "uk": "Helsinki-NLP/opus-mt-uk-en",
    "tr": "Helsinki-NLP/opus-mt-tr-en",
    "he": "Helsinki-NLP/opus-mt-tc-big-he-en",
    "ar": "Helsinki-NLP/opus-mt-ar-en",
    "it": "Helsinki-NLP/opus-mt-ROMANCE-en",
    "nl": "Helsinki-NLP/opus-mt-nl-en",
    "pt": "Helsinki-NLP/opus-mt-ROMANCE-en",
    "ca": "Helsinki-NLP/opus-mt-ROMANCE-en",
    "ro": "Helsinki-NLP/opus-mt-ROMANCE-en",
    "zh": "Helsinki-NLP/opus-mt-zh-en",
    "sq": "Helsinki-NLP/opus-mt-sq-en",
    "id": "Helsinki-NLP/opus-mt-id-en",
    "cs": "Helsinki-NLP/opus-mt-cs-en",
    "ko": "Helsinki-NLP/opus-mt-ko-en",
}

_client = None
_translation_pipelines = {}  # language_key -> {"tokenizer": ..., "model": ...}
_translation_pipeline_order = []  # LRU order — oldest first
_TRANSLATION_PIPELINE_MAX = 2  # max language models kept in RAM at once (~300MB each)


def get_client():
    global _client
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY is missing. Add it to the backend environment before starting Othello V2.")
    if _client is None:
        _client = Groq(api_key=api_key)
    return _client


def _normalize_language_code(value: str | None) -> str:
    text = (value or "").strip().lower()
    aliases = {
        "english": "en",
        "en-us": "en",
        "en-gb": "en",
        "french": "fr",
        "spanish": "es",
        "german": "de",
        "greek": "el",
        "ukrainian": "uk",
        "turkish": "tr",
        "hebrew": "he",
        "arabic": "ar",
        "italian": "it",
        "dutch": "nl",
        "portuguese": "pt",
        "catalan": "ca",
        "romanian": "ro",
    }
    if text in aliases:
        return aliases[text]
    return text.split("-")[0].split("_")[0]


def _evict_translation_pipeline(language_key: str) -> None:
    """Remove a translation model from cache and free its memory."""
    import gc
    bundle = _translation_pipelines.pop(language_key, None)
    if bundle:
        del bundle["tokenizer"]
        del bundle["model"]
        del bundle
        gc.collect()
    if language_key in _translation_pipeline_order:
        _translation_pipeline_order.remove(language_key)
    print(f"[translation] Evicted model for '{language_key}' to free RAM")


def _load_local_translation_pipeline(source_language: str, allow_download: bool = False):
    language_key = _normalize_language_code(source_language)
    if language_key == "en":
        return None

    if language_key in _translation_pipelines:
        # Move to end (most recently used)
        if language_key in _translation_pipeline_order:
            _translation_pipeline_order.remove(language_key)
        _translation_pipeline_order.append(language_key)
        return _translation_pipelines[language_key]

    model_name = LOCAL_TRANSLATION_MODEL_MAP.get(language_key)
    if not model_name:
        raise RuntimeError(f"No local translation model configured for language '{source_language}'.")

    # Evict least recently used model if at capacity
    while len(_translation_pipelines) >= _TRANSLATION_PIPELINE_MAX and _translation_pipeline_order:
        oldest = _translation_pipeline_order[0]
        _evict_translation_pipeline(oldest)

    try:
        from transformers import AutoModelForSeq2SeqLM, AutoTokenizer
    except Exception as exc:
        raise RuntimeError("transformers is not available for local translation.") from exc

    local_files_only = False if allow_download else LOCAL_TRANSLATION_FILES_ONLY
    try:
        tokenizer = AutoTokenizer.from_pretrained(model_name, local_files_only=local_files_only)
        model = AutoModelForSeq2SeqLM.from_pretrained(model_name, local_files_only=local_files_only)
    except Exception as exc:
        if allow_download:
            raise
        raise RuntimeError(f"Local translation model '{model_name}' is unavailable: {exc}") from exc

    bundle = {"tokenizer": tokenizer, "model": model}
    _translation_pipelines[language_key] = bundle
    _translation_pipeline_order.append(language_key)
    return bundle


def _translate_locally(text: str, source_language: str) -> str:
    if not text:
        return ""
    bundle = _load_local_translation_pipeline(source_language)
    if bundle is None:
        return text
    tokenizer = bundle["tokenizer"]
    model = bundle["model"]
    encoded = tokenizer(
        text,
        return_tensors="pt",
        truncation=True,
        max_length=min(256, max(64, len(text) + 24)),
    )
    generated = model.generate(**encoded, max_length=256)
    decoded = tokenizer.batch_decode(generated, skip_special_tokens=True)
    if not decoded:
        raise RuntimeError("Local translation returned no output.")
    return (decoded[0] or text).strip()


def warm_local_translation_models(languages: list[str] | None = None) -> dict[str, str]:
    requested = languages or sorted(LOCAL_TRANSLATION_MODEL_MAP)
    results: dict[str, str] = {}
    for language in requested:
        language_key = _normalize_language_code(language)
        try:
            _load_local_translation_pipeline(language_key, allow_download=True)
            results[language_key] = "ok"
        except Exception as exc:
            results[language_key] = f"error: {exc}"
    return results


SYSTEM_PROMPT = """You are an intelligence analyst providing briefings in the style of a geopolitical research firm.

When given a set of news articles, provide a structured intelligence briefing. You MUST use
exactly these section headers, in plain text with no markdown symbols like # or ##:

SITUATION REPORT:
[content]

KEY DEVELOPMENTS:
[content]

CRITICAL ACTORS:
[content]

SIGNAL vs NOISE:
[content]

PREDICTIONS:
[content]

DEEPER CONTEXT:
[content]

WHAT TO WATCH:
[content]

SOURCE CONTRADICTIONS:
[If contradiction data is provided, highlight the most significant contradictions and what
they reveal about narrative bias. If no contradiction data is provided, omit this section.]

Rules:
- Use exactly the section headers above, nothing else
- No markdown headers (#, ##, ###)
- Bullet points with - are fine within sections
- Be direct, analytical, specific
- Distinguish well-supported reporting from inference
- Always cite which source a claim comes from"""


def _chat(messages: list[dict], max_tokens: int = 1200) -> str:
    response = get_client().chat.completions.create(
        model=MODEL,
        max_tokens=max_tokens,
        messages=messages,
    )
    return response.choices[0].message.content


def _translate_article_groq(article: dict, target_language: str = "English") -> dict:
    source_language = article.get("language") or "unknown"
    prompt = f"""Translate this news article metadata into concise, natural {target_language}.

Return ONLY valid JSON:
{{
  "translated_title": "English title",
  "translated_description": "English summary"
}}

Rules:
- Preserve names, places, organizations, and numbers accurately
- Keep the translation faithful, not interpretive
- If the source text is already in English, return it cleanly

Source language: {source_language}
Source: {article.get('source', 'Unknown source')}
Original title: {article.get('original_title') or article.get('title') or ''}
Original summary: {article.get('original_description') or article.get('description') or ''}"""

    text = _chat([{"role": "user", "content": prompt}], max_tokens=300)
    cleaned = text.strip().replace("```json", "").replace("```", "").strip()
    payload = json.loads(cleaned)
    return {
        "translated_title": (payload.get("translated_title") or article.get("title") or "").strip(),
        "translated_description": (payload.get("translated_description") or article.get("description") or "").strip(),
        "target_language": target_language.lower()[:8],
        "provider": "groq",
    }


def _article_dump(articles: list[dict]) -> str:
    lines = []
    for index, article in enumerate(articles, 1):
        lines.append(
            f"""Article {index} — {article['source']} ({article['published_at'][:10]})
Title: {article['title']}
Summary: {article.get('description') or 'No summary'}
URL: {article['url']}"""
        )
    return "\n\n".join(lines)


def generate_briefing(
    topic: str,
    articles: list[dict],
    signals: str = "",
    contradictions: str = "",
    event_brief: str = "",
) -> str:
    context_blocks = [_article_dump(articles)]
    if signals:
        context_blocks.append(signals)
    if event_brief:
        context_blocks.append(event_brief)
    if contradictions:
        context_blocks.append(contradictions)
    joined_context = "\n\n".join(block for block in context_blocks if block)

    prompt = f"""Generate an intelligence briefing on {topic} based on these recent news articles:

{joined_context}

Today's date is {date.today().strftime('%B %d, %Y')}."""

    return _chat(
        [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        max_tokens=1500,
    )


def answer_query(question: str, context_articles: list[dict] | None = None, topic: str | None = None) -> str:
    context = ""
    if context_articles:
        context = "\n\nRelevant archived and live reporting:\n" + _article_dump(context_articles)

    scope_line = f"Topic constraint: {topic}.\n" if topic else ""
    prompt = f"""Question: {question}

{scope_line}{context}

Today's date is {date.today().strftime('%B %d, %Y')}.

Answer with the depth and precision of a senior intelligence analyst.
Be explicit about what is directly supported by reporting versus your inference.
If the articles are relevant, cite them by source name.
Acknowledge uncertainty with probability estimates where relevant."""

    return _chat(
        [
            {
                "role": "system",
                "content": """You are an elite intelligence analyst with deep expertise in geopolitics,
US politics, economics, and strategic forecasting. Prioritize precision over drama.
When live or archived articles are provided, use them first and cite the source names.""",
            },
            {"role": "user", "content": prompt},
        ],
        max_tokens=1000,
    )


def build_headlines_from_events(events: list[dict]) -> list[dict]:
    if not events:
        return []

    event_payload = [
        {
            "event_id": event["event_id"],
            "topic": event.get("topic"),
            "label": event["label"],
            "summary": event["summary"],
            "entity_focus": event.get("entity_focus", [])[:5],
            "source_count": event.get("source_count", 0),
            "article_count": event.get("article_count", 0),
            "contradiction_count": event.get("contradiction_count", 0),
        }
        for event in events[:8]
    ]

    prompt = f"""You are the front-page editor for a personal geopolitical intelligence dashboard.

Choose the 3 most important events from this structured event list and rewrite them as sharp story cards.
Write in neutral, cross-source language.
Do not mirror any single outlet's framing, blame language, or sensational wording.
Use the middle ground supported by the cluster.
If sources diverge on details, keep only the facts that are stable across the cluster.

Events:
{json.dumps(event_payload, indent=2)}

Return ONLY valid JSON:
{{
  "stories": [
    {{
      "event_id": "economics-1",
      "headline": "Neutral headline under 12 words",
      "summary": "One sentence on the cross-source consensus, max 28 words",
      "topic": "economics",
      "why_signal": "One sentence describing the unusual signal"
    }}
  ]
}}"""

    text = _chat([{"role": "user", "content": prompt}], max_tokens=700)
    cleaned = text.strip().replace("```json", "").replace("```", "").strip()
    payload = json.loads(cleaned)
    return payload.get("stories", [])


def build_timeline(query: str, articles: list[dict]) -> dict:
    prompt = f"""You are an intelligence analyst building a timeline for: "{query}"

{_article_dump(articles)}

Respond ONLY as valid JSON:
{{
  "title": "Timeline: [topic]",
  "summary": "One sentence overview",
  "events": [
    {{
      "date": "YYYY-MM-DD",
      "headline": "Short event headline max 10 words",
      "description": "2-3 sentences on what happened and why it matters",
      "significance": "HIGH" | "MEDIUM" | "LOW",
      "source": "Source name"
    }}
  ]
}}

Chronological, oldest first. 6-10 events max."""

    text = _chat([{"role": "user", "content": prompt}], max_tokens=1200)
    cleaned = text.strip().replace("```json", "").replace("```", "").strip()
    return json.loads(cleaned)


def translate_article(article: dict, target_language: str = "English", allow_remote_fallback: bool = True) -> dict:
    source_language = _normalize_language_code(article.get("language"))
    original_title = article.get("original_title") or article.get("title") or ""
    original_description = article.get("original_description") or article.get("description") or ""

    if source_language == "en":
        return {
            "translated_title": original_title.strip(),
            "translated_description": original_description.strip(),
            "target_language": target_language.lower()[:8],
            "provider": "identity",
        }

    local_error = None
    if TRANSLATION_PROVIDER in {"local", "auto"}:
        try:
            return {
                "translated_title": _translate_locally(original_title, source_language),
                "translated_description": _translate_locally(original_description, source_language),
                "target_language": target_language.lower()[:8],
                "provider": "local-helsinki",
            }
        except Exception as exc:
            local_error = exc

    if allow_remote_fallback and ALLOW_GROQ_TRANSLATION_FALLBACK and os.getenv("GROQ_API_KEY"):
        try:
            return _translate_article_groq(article, target_language=target_language)
        except Exception as exc:
            if local_error:
                raise RuntimeError(f"Local translation unavailable ({local_error}); Groq fallback failed ({exc}).") from exc
            raise

    if local_error:
        raise RuntimeError(f"Local translation unavailable: {local_error}")
    raise RuntimeError("No translation provider is available for this article.")
