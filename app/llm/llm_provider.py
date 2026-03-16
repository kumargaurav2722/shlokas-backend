"""
Multi-provider LLM client with automatic failover.

Order of preference:
  1. Groq  (free tier — needs GROQ_API_KEY env var)
  2. Ollama (local — needs ollama running)
  3. Keyword fallback (no LLM — returns top verses only)
"""

import json
import logging
import os
import urllib.request
import urllib.error
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Provider implementations
# ---------------------------------------------------------------------------

def _groq_generate(prompt: str, system_prompt: str = "") -> Optional[str]:
    """Call Groq's free chat completion API."""
    api_key = os.getenv("GROQ_API_KEY", "").strip()
    if not api_key:
        return None

    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    body = json.dumps({
        "model": os.getenv("GROQ_MODEL", "llama-3.1-8b-instant"),
        "messages": messages,
        "temperature": 0.4,
        "max_tokens": 1024,
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://api.groq.com/openai/v1/chat/completions",
        data=body,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data["choices"][0]["message"]["content"]
    except Exception as exc:
        logger.warning("Groq API failed: %s", exc)
        return None


def _ollama_generate(prompt: str, system_prompt: str = "") -> Optional[str]:
    """Call local Ollama instance."""
    base_url = os.getenv("OLLAMA_URL", "http://localhost:11434")
    model = os.getenv("OLLAMA_MODEL", "mistral:7b-instruct")

    full_prompt = f"{system_prompt}\n\n{prompt}" if system_prompt else prompt

    body = json.dumps({
        "model": model,
        "prompt": full_prompt,
        "stream": False,
    }).encode("utf-8")

    req = urllib.request.Request(
        f"{base_url}/api/generate",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            text = data.get("response", "").strip()
            return text if text else None
    except Exception as exc:
        logger.warning("Ollama failed: %s", exc)
        return None


def _keyword_fallback(context: str, question: str) -> str:
    """No-LLM fallback: return verses that best match the question."""
    return (
        f"Based on the scriptures, here are the most relevant verses "
        f"for your question:\n\n{context}\n\n"
        f"(Note: An AI-powered answer is unavailable right now. "
        f"The verses above are selected by semantic similarity to your question.)"
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a wise and respectful guide to the Bhagavad Gita and Hindu scriptures.
Answer ONLY using the scripture verses provided as context.
Always cite the chapter and verse numbers in your answer.
If the provided verses don't address the question, say so honestly.
Keep your answer clear, concise, and grounded in the scripture."""


def generate_answer(
    question: str,
    context: str,
    system_prompt: str = SYSTEM_PROMPT,
) -> Tuple[str, str]:
    """
    Generate an answer using the best available LLM provider.

    Returns:
        (answer_text, provider_name)
    """
    user_prompt = f"""Context (Scripture Verses):
{context}

Question:
{question}

Answer using ONLY the verses provided above. Cite chapter:verse numbers."""

    # Try Groq first
    answer = _groq_generate(user_prompt, system_prompt)
    if answer:
        return answer, "groq"

    # Try Ollama
    answer = _ollama_generate(user_prompt, system_prompt)
    if answer:
        return answer, "ollama"

    # Keyword fallback
    return _keyword_fallback(context, question), "fallback"


def translate_text(sanskrit: str, language: str) -> Tuple[str, str]:
    """Translate Sanskrit text to the given language using best available LLM."""
    prompt = f"""Translate the following Sanskrit shloka into {language}.
Then provide a brief explanation of its meaning.

Rules:
- Do NOT change the meaning
- Do NOT add new verses
- Keep explanation clear and concise
- Format: Translation first, then "Explanation:" followed by the explanation

Shloka:
{sanskrit}"""

    system = "You are a Sanskrit scholar. Translate accurately and explain simply."

    # Try Groq
    text = _groq_generate(prompt, system)
    if text:
        return _split_translation(text)

    # Try Ollama
    text = _ollama_generate(prompt, system)
    if text:
        return _split_translation(text)

    return f"[Translation unavailable for {language}]", ""


def _split_translation(text: str) -> Tuple[str, str]:
    """Split LLM output into translation + commentary."""
    parts = text.split("Explanation:")
    translation = parts[0].strip()
    commentary = parts[1].strip() if len(parts) > 1 else ""
    return translation, commentary
