import anthropic
import os
from datetime import date

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

SYSTEM_PROMPT = """You are an elite intelligence analyst providing briefings in the style of a 
senior analyst at a top-tier geopolitical research firm.

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
- No horizontal rules (---)
- Bullet points with - are fine within sections
- Bold with ** is fine within sections
- Be direct, analytical, specific. Use names and data from the articles.
- Always cite which source a claim comes from."""


def generate_briefing(topic: str, articles: list[dict], signals: str = "", contradictions: str = "") -> str:
    articles_text = ""
    for i, article in enumerate(articles, 1):
        articles_text += f"""
Article {i} — {article['source']} ({article['published_at'][:10]})
Title: {article['title']}
Summary: {article['description']}
URL: {article['url']}
"""

    # Build extra context
    extra_context = ""
    if signals:
        extra_context += f"\n\n{signals}"
    if contradictions:
        extra_context += f"\n\n{contradictions}"

    prompt = f"""Generate an intelligence briefing on {topic} based on these recent news articles:

{articles_text}{extra_context}

Today's date is {date.today().strftime('%B %d, %Y')}.
"""

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1500,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}]
    )

    return message.content[0].text


def answer_query(question: str, context_articles: list[dict] = None) -> str:
    context = ""
    if context_articles:
        context = "\n\nRelevant live news articles for context:\n"
        for i, article in enumerate(context_articles, 1):
            context += f"""
Article {i} — {article['source']} ({article['published_at'][:10]})
Title: {article['title']}
Summary: {article['description']}
URL: {article['url']}
"""

    prompt = f"""Question: {question}

{context}

Today's date is {date.today().strftime('%B %d, %Y')}.

Answer with the depth and precision of a senior intelligence analyst. Be direct and specific.
If the news articles above are relevant, cite them by source name.
Acknowledge uncertainty with probability estimates where relevant."""

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1000,
        system="""You are an elite intelligence analyst with deep expertise in geopolitics,
US politics, and economics. Answer questions with precision and depth. Be direct, cite
specific actors and data, use probability estimates for uncertain claims. Never be vague.
When live news articles are provided, prioritize that information and cite sources.""",
        messages=[{"role": "user", "content": prompt}]
    )

    return message.content[0].text