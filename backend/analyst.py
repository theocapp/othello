from groq import Groq
import os
from datetime import date

client = Groq(api_key=os.getenv("GROQ_API_KEY"))
MODEL = "llama-3.3-70b-versatile"  # best free model on Groq

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
they reveal about narrative bias. Quote directly and give examples. If no contradiction data is provided, omit this section.]

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

    extra_context = ""
    if signals:
        extra_context += f"\n\n{signals}"
    if contradictions:
        extra_context += f"\n\n{contradictions}"

    prompt = f"""Generate an intelligence briefing on {topic} based on these recent news articles:

{articles_text}{extra_context}

Today's date is {date.today().strftime('%B %d, %Y')}.
"""

    response = client.chat.completions.create(
        model=MODEL,
        max_tokens=1500,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt}
        ]
    )

    return response.choices[0].message.content


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

    response = client.chat.completions.create(
        model=MODEL,
        max_tokens=1000,
        messages=[
            {"role": "system", "content": """You are an elite intelligence analyst with deep expertise in geopolitics,
US politics, and economics. Answer questions with precision and depth. Be direct, cite
specific actors and data, use probability estimates for uncertain claims. Never be vague.
When live news articles are provided, prioritize that information and cite sources."""},
            {"role": "user", "content": prompt}
        ]
    )

    return response.choices[0].message.content