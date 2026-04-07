"""Intel domain package.

High-level analytics and inference modules live here (correlation,
causal, contradictions, story rollups).
"""

try:
    from . import entities  # type: ignore
    from .. import correlation_engine  # type: ignore
    from .. import causal  # type: ignore
    from .. import contradictions  # type: ignore
    from .. import story_materialization  # type: ignore
    __all__ = [
        "correlation_engine",
        "causal",
        "contradictions",
        "story_materialization",
        "entities",
    ]
except Exception:
    import correlation_engine  # type: ignore
    import causal  # type: ignore
    import contradictions  # type: ignore
    import story_materialization  # type: ignore
    try:
        import entities  # type: ignore
        __all__ = [
            "correlation_engine",
            "causal",
            "contradictions",
            "story_materialization",
            "entities",
        ]
    except Exception:
        __all__ = [
            "correlation_engine",
            "causal",
            "contradictions",
            "story_materialization",
        ]
