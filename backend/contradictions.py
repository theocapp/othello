import spacy
from anthropic import Anthropic
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

nlp = spacy.load("en_core_web_sm")
client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

def group_articles_by_event(articles: list[dict]) -> list[list[dict]]:
    """
    Group articles that are likely covering the same event
    using spaCy entity overlap as a similarity signal.
    """
    def get_entities(text: str) -> set:
        doc = nlp(text[:500])
        return {ent.text.lower() for ent in doc.ents 
                if ent.label_ in {"GPE", "PERSON", "ORG", "NORP", "EVENT"}}

    # Get entities for each article
    article_entities = []
    for article in articles:
        text = f"{article['title']}. {article.get('description', '')}"
        entities = get_entities(text)
        article_entities.append(entities)

    # Group articles with significant entity overlap
    groups = []
    used = set()

    for i, article in enumerate(articles):
        if i in used:
            continue
        group = [article]
        used.add(i)

        for j, other in enumerate(articles):
            if j in used or i == j:
                continue
            # Calculate entity overlap
            overlap = article_entities[i] & article_entities[j]
            if len(overlap) >= 2:  # At least 2 shared entities = likely same event
                group.append(other)
                used.add(j)

        if len(group) >= 2:  # Only keep groups with multiple sources
            groups.append(group)

    return groups


def detect_contradictions(articles: list[dict], topic: str) -> str:
    """
    Find contradictions across articles covering the same events.
    Returns a formatted string to inject into the briefing prompt.
    """
    if len(articles) < 2:
        return ""

    # Group articles by event
    groups = group_articles_by_event(articles)

    if not groups:
        return ""

    # Format grouped articles for Claude
    groups_text = ""
    for i, group in enumerate(groups, 1):
        groups_text += f"\nEVENT GROUP {i} ({len(group)} sources covering same event):\n"
        for article in group:
            groups_text += f"""
  Source: {article['source']}
  Title: {article['title']}
  Summary: {article.get('description', 'No description')}
"""

    prompt = f"""Analyze these groups of articles covering the same events from different sources.

{groups_text}

Your task:
1. Identify direct factual contradictions between sources covering the same event
2. For each contradiction explain WHY it likely exists (bias, different access, spin, genuine uncertainty)
3. Assess which account is more credible and why
4. Flag any cases where a source appears to be deliberately misleading

Format your response as:

CONTRADICTIONS DETECTED:
[For each contradiction:]
▸ [Topic of contradiction] — [Source A]: "[claim]" vs [Source B]: "[claim]"
  Why this contradiction exists: [your analysis]
  More credible account: [assessment]

If no meaningful contradictions exist, respond with: NO SIGNIFICANT CONTRADICTIONS DETECTED

Be direct and specific. Name the sources. Don't hedge excessively."""

    try:
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=800,
            messages=[{"role": "user", "content": prompt}]
        )
        result = message.content[0].text

        if "NO SIGNIFICANT CONTRADICTIONS" in result:
            return ""

        return result

    except Exception as e:
        print(f"[contradictions] Error: {e}")
        return ""