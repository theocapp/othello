import os
import sys
# Ensure backend folder is on sys.path when pytest runs from a different CWD
sys.path.insert(0, os.path.dirname(__file__))
from contradictions import _article_status_markers, _article_framing_labels, _token_keywords


def test_article_status_markers_detects_positive_and_negative():
    text_pos = "A ceasefire has been agreed and will be observed by both sides."
    markers = _article_status_markers(text_pos)
    assert markers.get("ceasefire") == "positive"

    text_neg = "The ceasefire collapsed and fighting resumed overnight."
    markers2 = _article_status_markers(text_neg)
    assert markers2.get("ceasefire") == "negative"


def test_framing_labels_and_token_keywords():
    text = "Rebels claimed they captured the town while government forces deny the claim."
    labels = _article_framing_labels(text)
    # Expect framing label 'rebel' or similar to be detected
    assert any(lbl in labels for lbl in ("rebel", "insurgent", "militant"))

    tokens = _token_keywords("Market yields jumped due to inflation and central bank actions")
    assert "market" in tokens or "yields" in tokens
