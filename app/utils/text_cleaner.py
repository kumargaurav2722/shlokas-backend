import re

WHITESPACE_RE = re.compile(r"\s+")


def normalize_sanskrit(text: str) -> str:
    if text is None:
        return ""
    cleaned = text.strip()
    cleaned = cleaned.replace("\u00a0", " ")
    cleaned = cleaned.replace("\r", " ")
    cleaned = cleaned.replace("\n", " ")
    cleaned = WHITESPACE_RE.sub(" ", cleaned)
    return cleaned
