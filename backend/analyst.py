import json
import os
import time
import logging
import functools
from collections import OrderedDict
import re
from datetime import date

from groq import Groq
from core.config import REQUEST_ENABLE_LLM_RESPONSES

logger = logging.getLogger(__name__)

MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
TRANSLATION_PROVIDER = os.getenv("OTHELLO_TRANSLATION_PROVIDER", "local").lower()
ALLOW_GROQ_TRANSLATION_FALLBACK = (
    os.getenv("OTHELLO_ALLOW_GROQ_TRANSLATION_FALLBACK", "true").lower() == "true"
)
LOCAL_TRANSLATION_FILES_ONLY = (
    os.getenv("OTHELLO_TRANSLATION_LOCAL_FILES_ONLY", "true").lower() == "true"
)

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


# Consolidated LRU cache for translation pipelines (authoritative)
# Use OrderedDict so we can insert pre-populated bundles and evict deterministically.
_translation_pipelines = OrderedDict()  # language_key -> bundle (tokenizer+model)
_translation_pipeline_order = []  # deprecated: keep list view for tests (oldest first)
_TRANSLATION_PIPELINE_MAX = 2  # max language models kept in RAM at once (~300MB each)


def _get_translation_pipeline_cached(language_key: str) -> dict:
    """Load and return a translation model bundle using a single authoritative LRU.

    This function both loads on demand (respecting LOCAL_TRANSLATION_FILES_ONLY)
    and updates the shared `_translation_pipelines` OrderedDict. Tests and legacy
    code may still inspect `_translation_pipelines` and `_translation_pipeline_order`.
    """
    if language_key == "en":
        return None

    model_name = LOCAL_TRANSLATION_MODEL_MAP.get(language_key)
    if not model_name:
        raise RuntimeError(
            f"No local translation model configured for language '{language_key}'."
        )

    try:
        from transformers import AutoModelForSeq2SeqLM, AutoTokenizer
    except Exception as exc:
        raise RuntimeError(
            "transformers is not available for local translation."
        ) from exc

    local_files_only = LOCAL_TRANSLATION_FILES_ONLY
    try:
        tokenizer = AutoTokenizer.from_pretrained(
            model_name, local_files_only=local_files_only
        )
        model = AutoModelForSeq2SeqLM.from_pretrained(
            model_name, local_files_only=local_files_only
        )
    except Exception as exc:
        raise RuntimeError(
            f"Local translation model '{model_name}' is unavailable: {exc}"
        ) from exc

    bundle = {"tokenizer": tokenizer, "model": model}

    # Insert / move to most-recent position in OrderedDict
    if language_key in _translation_pipelines:
        # remove then re-insert to move to end
        _translation_pipelines.pop(language_key, None)
    _translation_pipelines[language_key] = bundle

    # Keep deprecated order list in sync (oldest first)
    if language_key in _translation_pipeline_order:
        _translation_pipeline_order.remove(language_key)
    _translation_pipeline_order.append(language_key)

    # Enforce size limit with eviction
    while len(_translation_pipeline_order) > _TRANSLATION_PIPELINE_MAX:
        oldest = _translation_pipeline_order.pop(0)
        _evict_translation_pipeline(oldest)

    logger.info(f"[translation] Loaded model for '{language_key}' ({model_name})")
    return bundle


_client = None


def get_client():
    global _client
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError(
            "GROQ_API_KEY is missing. Add it to the backend environment before starting Othello V2."
        )
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
    """Remove a translation model from cache and free its memory (deprecated - lru_cache handles this)."""
    import gc
    bundle = _translation_pipelines.pop(language_key, None)
    if bundle:
        del bundle["tokenizer"]
        del bundle["model"]
        del bundle
        gc.collect()
    if language_key in _translation_pipeline_order:
        _translation_pipeline_order.remove(language_key)
    logger.debug(f"[translation] Evicted model for '{language_key}' to free RAM")


def _load_local_translation_pipeline(
    source_language: str, allow_download: bool = False
):
    """Load a translation pipeline with automatic LRU eviction."""
    language_key = _normalize_language_code(source_language)
    if language_key == "en":
        return None

    # Prefer any explicitly populated (deprecated) caches so unit tests that
    # inject fake bundles into `_translation_pipelines` behave as expected.
    if not allow_download:
        if language_key in _translation_pipelines:
            # Accessing the existing bundle moves it to most-recent position
            bundle = _translation_pipelines.pop(language_key)
            _translation_pipelines[language_key] = bundle
            if language_key in _translation_pipeline_order:
                _translation_pipeline_order.remove(language_key)
            _translation_pipeline_order.append(language_key)
            # Enforce max cache size
            while len(_translation_pipeline_order) > _TRANSLATION_PIPELINE_MAX:
                _evict_translation_pipeline(_translation_pipeline_order.pop(0))
            return bundle

        # Use consolidated loader which will insert into `_translation_pipelines`
        return _get_translation_pipeline_cached(language_key)

    # For allow_download=True (used in warm_local_translation_models),
    # we need to load without the local_files_only restriction
    model_name = LOCAL_TRANSLATION_MODEL_MAP.get(language_key)
    if not model_name:
        raise RuntimeError(
            f"No local translation model configured for language '{source_language}'."
        )

    try:
        from transformers import AutoModelForSeq2SeqLM, AutoTokenizer
    except Exception as exc:
        raise RuntimeError(
            "transformers is not available for local translation."
        ) from exc

    try:
        tokenizer = AutoTokenizer.from_pretrained(model_name, local_files_only=False)
        model = AutoModelForSeq2SeqLM.from_pretrained(
            model_name, local_files_only=False
        )
        bundle = {"tokenizer": tokenizer, "model": model}
        # Insert into consolidated cache and enforce limits
        if language_key in _translation_pipelines:
            _translation_pipelines.pop(language_key, None)
        _translation_pipelines[language_key] = bundle
        if language_key in _translation_pipeline_order:
            _translation_pipeline_order.remove(language_key)
        _translation_pipeline_order.append(language_key)
        while len(_translation_pipeline_order) > _TRANSLATION_PIPELINE_MAX:
            _evict_translation_pipeline(_translation_pipeline_order.pop(0))
        logger.info(
            f"[translation] Downloaded and loaded model for '{language_key}' ({model_name})"
        )
        return bundle
    except Exception as exc:
        raise RuntimeError(
            f"Local translation model '{model_name}' download failed: {exc}"
        ) from exc


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


def _chat(messages: list[dict], max_tokens: int = 1200, max_retries: int = 3) -> str:
    """Call Groq API with retry logic and graceful fallback."""
    last_error = None

    for attempt in range(max_retries):
        try:
            response = get_client().chat.completions.create(
                model=MODEL,
                max_tokens=max_tokens,
                messages=messages,
                timeout=30,
            )

            if not response.choices or not response.choices[0].message:
                raise RuntimeError("API returned empty response")

            # Extract content and detect common refusal/assistant-safety responses
            content = response.choices[0].message.content
            try:
                # Log a truncated preview of the raw response for debugging
                logger.debug(f"[groq] Raw response preview: {str(content)[:1000]}")
            except Exception:
                pass

            if isinstance(content, str):
                # Common refusal / assistant-safety patterns to treat as non-answer
                if re.search(
                    r"(?i)i\s*(?:'|’)?m sorry|i cannot assist|i can(?:'|’)?t assist|cannot help with that|unable to assist|i can'?t help with that",
                    content,
                ):
                    logger.warning(
                        "[groq] Detected refusal text in model response; treating as failure to trigger fallback"
                    )
                    raise RuntimeError("API returned refusal")

            return content

        except Exception as exc:
            last_error = exc
            error_name = type(exc).__name__

            # Rate limit: exponential backoff
            if "429" in str(exc) or "rate_limit" in str(exc).lower():
                wait_time = min(2**attempt, 8)  # max 8 seconds
                logger.warning(
                    f"[groq] Rate limited (attempt {attempt+1}/{max_retries}), waiting {wait_time}s: {error_name}"
                )
                time.sleep(wait_time)
                continue

            # Timeout: retry immediately
            elif "timeout" in str(exc).lower() or "timed out" in str(exc).lower():
                logger.warning(
                    f"[groq] Timeout (attempt {attempt+1}/{max_retries}): {error_name}"
                )
                if attempt < max_retries - 1:
                    time.sleep(0.5)  # brief pause before retry
                continue

            # Auth errors: don't retry
            elif "401" in str(exc) or "unauthorized" in str(exc).lower():
                logger.error(f"[groq] Auth failed: {error_name} — {exc}")
                raise

            # All other errors: log and retry
            else:
                logger.warning(
                    f"[groq] Error (attempt {attempt+1}/{max_retries}): {error_name} — {exc}"
                )
                if attempt < max_retries - 1:
                    time.sleep(0.5)
                continue

    # All retries exhausted: return graceful fallback
    logger.error(
        f"[groq] All {max_retries} attempts failed. Last error: {last_error}. Returning fallback response."
    )

    # Attempt to provide a sensible fallback based on the user's request
    user_msgs = [m for m in messages if m.get("role") == "user"]
    if user_msgs:
        content = user_msgs[-1].get("content", "")
        if "briefing" in content.lower():
            return (
                "SITUATION REPORT:\n[Unable to generate briefing — Groq API unavailable. "
                "Please review source articles directly.]\n\n"
                "KEY DEVELOPMENTS:\n[Data unavailable]\n\n"
                "SIGNAL vs NOISE:\n[Cannot assess]\n\n"
                "WHAT TO WATCH:\n[Service temporarily offline]"
            )
        elif "timeline" in content.lower():
            return json.dumps(
                {
                    "title": "Timeline: Unavailable",
                    "summary": "Timeline generation unavailable (API offline)",
                    "events": [],
                }
            )
        elif "question" in content.lower().split()[0:3]:
            return "[Unable to answer question — Groq API is temporarily unavailable. Please try again in a moment.]"

    return "[Query processing temporarily unavailable due to API issues. Please try again shortly.]"


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

    try:
        text = _chat([{"role": "user", "content": prompt}], max_tokens=300)
        cleaned = text.strip().replace("```json", "").replace("```", "").strip()
        payload = json.loads(cleaned)
        return {
            "translated_title": (
                payload.get("translated_title") or article.get("title") or ""
            ).strip(),
            "translated_description": (
                payload.get("translated_description")
                or article.get("description")
                or ""
            ).strip(),
            "target_language": target_language.lower()[:8],
            "provider": "groq",
        }
    except json.JSONDecodeError as exc:
        logger.warning(
            f"[translation] JSON parse failed for article {article.get('title', 'unknown')}: {exc}. Using fallback."
        )
        return {
            "translated_title": article.get("title", ""),
            "translated_description": article.get("description", ""),
            "target_language": target_language.lower()[:8],
            "provider": "fallback",
        }
    except Exception as exc:
        logger.error(
            f"[translation] Unexpected error translating article: {exc}. Using fallback."
        )
        return {
            "translated_title": article.get("title", ""),
            "translated_description": article.get("description", ""),
            "target_language": target_language.lower()[:8],
            "provider": "fallback",
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


def build_deterministic_briefing(
    articles: list[dict],
    topic: str | None = None,
    events: list[dict] | None = None,
) -> dict:
    """Build a structured briefing from raw data without any LLM dependency.

    Returns a dict with fixed keys that the frontend can render directly.
    Each field is populated from deterministic logic — sorted lists, counts,
    entity extraction — so this always returns useful output.
    """
    from news import article_quality_score, infer_article_topics, diversify_articles

    topic_articles = [
        a for a in (articles or []) if not topic or topic in (infer_article_topics(a) or [])
    ]
    top_articles = sorted(
        topic_articles,
        key=lambda a: -article_quality_score(a, [topic] if topic else None),
    )[:12]

    key_developments = [
        {
            "headline": a.get("title", ""),
            "source": a.get("source") or a.get("source_domain") or "Unknown",
            "url": a.get("url", ""),
            "published_at": a.get("published_at", ""),
        }
        for a in top_articles[:5]
    ]

    entity_counts: dict[str, int] = {}
    for a in top_articles:
        for entity in (a.get("entities") or []):
            name = entity if isinstance(entity, str) else entity.get("entity", "")
            if name:
                entity_counts[name] = entity_counts.get(name, 0) + 1
    critical_actors = sorted(entity_counts.items(), key=lambda x: -x[1])[:6]

    sources = sorted(
        {a.get("source") or a.get("source_domain") or "Unknown" for a in top_articles}
    )

    event_summary = []
    for event in (events or [])[:5]:
        event_summary.append(
            {
                "location": event.get("location") or event.get("country") or "Unknown",
                "event_type": event.get("event_type", ""),
                "fatalities": event.get("fatalities") or 0,
                "date": str(event.get("event_date") or ""),
            }
        )

    return {
        "topic": topic or "general",
        "article_count": len(topic_articles),
        "key_developments": key_developments,
        "critical_actors": [
            {"entity": name, "mentions": count} for name, count in critical_actors
        ],
        "sources": sources,
        "event_summary": event_summary,
        "situation_summary": "",
        "signal_vs_noise": "",
        "llm_enriched": False,
    }


def generate_briefing(
    articles: list[dict],
    topic: str | None = None,
    events: list[dict] | None = None,
    use_llm: bool = True,
) -> dict:
    briefing = build_deterministic_briefing(articles, topic=topic, events=events)

    if not use_llm or not REQUEST_ENABLE_LLM_RESPONSES:
        return briefing

    try:
        top_headlines = "\n".join(
            f"- {d['headline']} ({d['source']})" for d in briefing["key_developments"]
        )
        actors = ", ".join(d["entity"] for d in briefing["critical_actors"])
        enrichment_prompt = (
            f"Topic: {topic or 'general intelligence'}\n\n"
            f"Top developments:\n{top_headlines}\n\n"
            f"Key actors: {actors}\n\n"
            "Write two short paragraphs:\n"
            "1. SITUATION SUMMARY (3-4 sentences): What is the core situation?\n"
            "2. SIGNAL VS NOISE (2-3 sentences): What is genuinely significant "
            "versus routine reporting?\n\n"
            "Be specific and factual. Do not introduce actors or events not listed above."
        )
        llm_response = _chat([{"role": "user", "content": enrichment_prompt}], max_tokens=500)
        if llm_response:
            lines = llm_response.strip().split("\n")
            situation_lines = []
            noise_lines = []
            current = None
            for line in lines:
                low = line.lower()
                if "situation summary" in low:
                    current = "situation"
                elif "signal vs noise" in low or "signal versus noise" in low:
                    current = "noise"
                elif current == "situation" and line.strip():
                    situation_lines.append(line.strip())
                elif current == "noise" and line.strip():
                    noise_lines.append(line.strip())
            if situation_lines:
                briefing["situation_summary"] = " ".join(situation_lines)
            if noise_lines:
                briefing["signal_vs_noise"] = " ".join(noise_lines)
            briefing["llm_enriched"] = True
    except Exception as exc:
        print(f"[briefing] LLM enrichment failed, returning scaffold: {exc}")

    return briefing


def answer_query(
    question: str, context_articles: list[dict] | None = None, topic: str | None = None
) -> str:
    context = ""
    if context_articles:
        context = "\n\nRelevant archived and live reporting:\n" + _article_dump(
            context_articles
        )

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

    try:
        text = _chat([{"role": "user", "content": prompt}], max_tokens=700)
        cleaned = text.strip().replace("```json", "").replace("```", "").strip()
        payload = json.loads(cleaned)
        return payload.get("stories", [])
    except json.JSONDecodeError as exc:
        logger.warning(
            f"[headlines] JSON parse failed: {exc}. Returning fallback headlines."
        )
        return [
            {
                "event_id": event.get("event_id", ""),
                "headline": event.get("label", "Event")[:50],
                "summary": event.get("summary", "")[:100],
                "topic": event.get("topic", ""),
                "why_signal": "Signal processing unavailable",
            }
            for event in events[:3]
        ]
    except Exception as exc:
        logger.error(
            f"[headlines] Unexpected error building headlines: {exc}. Returning fallback."
        )
        return []


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

    try:
        text = _chat([{"role": "user", "content": prompt}], max_tokens=1200)
        cleaned = text.strip().replace("```json", "").replace("```", "").strip()
        return json.loads(cleaned)
    except json.JSONDecodeError as exc:
        logger.warning(
            f"[timeline] JSON parse failed: {exc}. Returning fallback timeline."
        )
        return {
            "title": f"Timeline: {query}",
            "summary": "Timeline generation unavailable",
            "events": [
                {
                    "date": article.get("published_at", "")[:10],
                    "headline": article.get("title", "Event")[:50],
                    "description": article.get("description", "")[:200],
                    "significance": "MEDIUM",
                    "source": article.get("source", "Unknown"),
                }
                for article in articles[:6]
            ],
        }
    except Exception as exc:
        logger.error(
            f"[timeline] Unexpected error building timeline: {exc}. Returning empty timeline."
        )
        return {
            "title": f"Timeline: {query}",
            "summary": "Timeline generation temporarily unavailable",
            "events": [],
        }


def translate_article(
    article: dict, target_language: str = "English", allow_remote_fallback: bool = True
) -> dict:
    source_language = _normalize_language_code(article.get("language"))
    original_title = article.get("original_title") or article.get("title") or ""
    original_description = (
        article.get("original_description") or article.get("description") or ""
    )

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
                "translated_description": _translate_locally(
                    original_description, source_language
                ),
                "target_language": target_language.lower()[:8],
                "provider": "local-helsinki",
            }
        except Exception as exc:
            local_error = exc

    if (
        allow_remote_fallback
        and ALLOW_GROQ_TRANSLATION_FALLBACK
        and os.getenv("GROQ_API_KEY")
    ):
        try:
            return _translate_article_groq(article, target_language=target_language)
        except Exception as exc:
            if local_error:
                raise RuntimeError(
                    f"Local translation unavailable ({local_error}); Groq fallback failed ({exc})."
                ) from exc
            raise

    if local_error:
        raise RuntimeError(f"Local translation unavailable: {local_error}")
    raise RuntimeError("No translation provider is available for this article.")
