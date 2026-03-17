import hashlib
import math
import os
from typing import List, Optional, Any

_MODEL_NAME = "all-MiniLM-L6-v2"
_model: Optional[Any] = None
_model_error: Optional[Exception] = None

def _get_model() -> Optional[Any]:
    global _model, _model_error
    if os.getenv("EMBEDDINGS_FORCE_FALLBACK") == "1":
        return None
    if _model is not None or _model_error is not None:
        return _model
    try:
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer(_MODEL_NAME)
    except Exception as exc:
        _model_error = exc
    return _model


def _fallback_embedding(text: str, dim: int = 384) -> List[float]:
    tokens = [t for t in text.lower().split() if t]
    if not tokens:
        return [0.0] * dim
    vec = [0.0] * dim
    for token in tokens:
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        for i in range(0, len(digest) - 1, 2):
            idx = digest[i] % dim
            val = (digest[i + 1] - 128) / 128.0
            vec[idx] += val
    norm = math.sqrt(sum(v * v for v in vec))
    if norm > 0:
        vec = [v / norm for v in vec]
    return vec


def generate_embedding(text: str):
    model = _get_model()
    if model is None:
        return _fallback_embedding(text)
    return model.encode(text).tolist()
