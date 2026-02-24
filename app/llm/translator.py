"""Translation + explanation via the best available LLM provider."""

from app.llm.llm_provider import translate_text


def translate_and_explain(sanskrit: str, language: str):
    """
    Translate a Sanskrit shloka and provide an explanation.

    Returns:
        (translation, commentary) tuple
    """
    return translate_text(sanskrit, language)
