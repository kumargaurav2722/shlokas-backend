from typing import Optional


_LANGUAGE_ALIASES = {
    "hi": "Hindi",
    "hindi": "Hindi",
    "en": "English",
    "english": "English",
    "mr": "Marathi",
    "marathi": "Marathi",
    "ta": "Tamil",
    "tamil": "Tamil",
    "kn": "Kannada",
    "kannada": "Kannada",
    "ml": "Malayalam",
    "malayalam": "Malayalam",
    "sa": "sanskrit",
    "sanskrit": "sanskrit",
}


def normalize_language(language: Optional[str]) -> Optional[str]:
    if not language:
        return language
    key = language.strip().lower()
    return _LANGUAGE_ALIASES.get(key, language)
