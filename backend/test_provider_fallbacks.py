import importlib
import os


def test_runtime_flags_and_source_status(monkeypatch):
    # Ensure environment is clean for the test
    # Prevent dotenv from populating keys by setting them to empty strings
    monkeypatch.setenv("GROQ_API_KEY", "")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "")
    # Import runtime and patch heavy dependencies to keep test fast
    from core import runtime as runtime_mod

    monkeypatch.setattr(runtime_mod, "get_ingestion_summary", lambda: {"total_articles": 0})
    monkeypatch.setattr(runtime_mod, "get_entity_model_capabilities", lambda: {})
    monkeypatch.setattr(runtime_mod, "source_status", lambda: {"gdelt": {"enabled": True}, "directfeeds": {"enabled": False}})

    status = runtime_mod.runtime_status()
    assert status["llm_ready"] is False
    assert status["contradiction_ready"] is False

    # Toggle keys and expect flags to flip
    monkeypatch.setenv("GROQ_API_KEY", "x")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "y")
    status2 = runtime_mod.runtime_status()
    assert status2["llm_ready"] is True
    assert status2["contradiction_ready"] is True


def test_news_provider_enabled_reload(monkeypatch):
    # Re-import news with different env vars to exercise module-level fallbacks
    monkeypatch.setenv("OTHELLO_ENABLE_NEWSAPI_FALLBACK", "true")
    # Ensure NEWS_API_KEY is an empty string (dotenv may populate it from .env)
    monkeypatch.setenv("NEWS_API_KEY", "")
    import news as news_mod
    importlib.reload(news_mod)

    status = news_mod.source_status()
    # Empty NEWS_API_KEY => available False, enabled False even if fallback enabled
    assert status["newsapi"]["available"] is False
    assert status["newsapi"]["enabled"] is False

    # Provide key and reload
    monkeypatch.setenv("NEWS_API_KEY", "abc")
    importlib.reload(news_mod)
    status2 = news_mod.source_status()
    assert status2["newsapi"]["available"] is True
    assert status2["newsapi"]["enabled"] is True
