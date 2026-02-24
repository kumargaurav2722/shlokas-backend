"""Semantic search using cosine similarity over embeddings."""

import json
import logging
import math
from typing import List, Any

logger = logging.getLogger(__name__)


def cosine(a: List[float], b: List[float]) -> float:
    """Cosine similarity with zero-norm protection."""
    if not a or not b or len(a) != len(b):
        return 0.0

    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))

    if norm_a == 0 or norm_b == 0:
        return 0.0

    return dot / (norm_a * norm_b)


def _parse_embedding(raw) -> List[float]:
    """Parse embedding from various formats (list, JSON string, etc)."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return []
    # numpy array or similar
    try:
        return list(raw)
    except (TypeError, ValueError):
        return []


def semantic_search(query: str, rows: list, top_k: int = 5) -> list:
    """
    Find the top-K most relevant rows by cosine similarity.

    Args:
        query: the user's question text
        rows: objects with an `.embedding` attribute
        top_k: how many results to return

    Returns:
        List of the top-K rows sorted by relevance
    """
    from app.embeddings.embedder import generate_embedding

    q_emb = generate_embedding(query)
    if not q_emb:
        logger.warning("Failed to generate query embedding")
        return rows[:top_k]

    scored = []
    for r in rows:
        emb = _parse_embedding(r.embedding)
        if not emb:
            continue
        try:
            score = cosine(q_emb, emb)
            scored.append((score, r))
        except Exception as exc:
            logger.debug("Cosine failed for row: %s", exc)
            continue

    scored.sort(reverse=True, key=lambda x: x[0])
    return [r for _, r in scored[:top_k]]
