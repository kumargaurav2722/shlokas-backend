from __future__ import annotations

import html
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from sqlalchemy.orm import Session

from app.deps import get_db
from app.models.text import Text
from app.models.translation import Translation


router = APIRouter(prefix="/share", tags=["Share"])


def normalize_language(value: str | None) -> str:
    if not value:
        return ""
    lower = value.strip().lower()
    if lower in {"hi", "hindi"}:
        return "hi"
    if lower in {"en", "english"}:
        return "en"
    if lower in {"bn", "bengali"}:
        return "bn"
    if lower in {"mr", "marathi"}:
        return "mr"
    if lower in {"te", "telugu"}:
        return "te"
    if lower in {"ta", "tamil"}:
        return "ta"
    if lower in {"kn", "kannada"}:
        return "kn"
    return lower


def pick_translation(translations: list[Translation], lang: str | None) -> Translation | None:
    if not translations:
        return None
    by_lang = {}
    for tr in translations:
        key = normalize_language(tr.language)
        if key and key not in by_lang:
            by_lang[key] = tr
    normalized = normalize_language(lang)
    if normalized and normalized in by_lang:
        return by_lang[normalized]
    if "en" in by_lang:
        return by_lang["en"]
    return next(iter(by_lang.values()))


def wrap_text(text: str, max_chars: int) -> list[str]:
    words = text.split()
    if not words:
        return []
    lines: list[str] = []
    current: list[str] = []
    count = 0
    for word in words:
        extra = len(word) + (1 if current else 0)
        if count + extra > max_chars and current:
            lines.append(" ".join(current))
            current = [word]
            count = len(word)
        else:
            current.append(word)
            count += extra
    if current:
        lines.append(" ".join(current))
    return lines


@router.get("/verse")
def share_verse(
    text_id: str | None = Query(None, description="Text ID"),
    work: str | None = Query(None),
    sub_work: str | None = Query(None),
    chapter: int | None = Query(None),
    verse: int | None = Query(None),
    lang: str | None = Query(None),
    title: str | None = Query(None),
    db: Session = Depends(get_db),
):
    query = db.query(Text)
    if text_id:
        query = query.filter(Text.id == text_id)
    else:
        if not work or chapter is None or verse is None:
            raise HTTPException(400, "Provide text_id or work/chapter/verse.")
        query = query.filter(
            Text.work == work,
            Text.chapter == chapter,
            Text.verse == verse,
        )
        if sub_work:
            query = query.filter(Text.sub_work == sub_work)

    text_row = query.first()
    if not text_row:
        raise HTTPException(404, "Verse not found.")

    translations = (
        db.query(Translation)
        .filter(Translation.text_id == text_row.id)
        .all()
    )
    translation = pick_translation(translations, lang)

    heading = title or f"{text_row.sub_work or text_row.work} {text_row.chapter}.{text_row.verse}"
    sanskrit_lines = wrap_text(text_row.sanskrit or "", 40)[:4]
    translation_lines = wrap_text(translation.translation if translation else "", 56)[:5]

    def render_lines(lines: list[str], x: int, y: int, line_height: int, font_size: int, color: str) -> str:
        if not lines:
            return ""
        escaped = [html.escape(line) for line in lines]
        tspans = "\n".join(
            f'<tspan x="{x}" dy="{line_height}">{line}</tspan>'
            for line in escaped
        )
        return (
            f'<text x="{x}" y="{y}" font-family="Georgia, serif" '
            f'font-size="{font_size}" fill="{color}">{tspans}</text>'
        )

    svg = f"""<svg width="1200" height="630" viewBox="0 0 1200 630" fill="none" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="#FFF6E9"/>
      <stop offset="100%" stop-color="#FCE0B2"/>
    </linearGradient>
  </defs>
  <rect width="1200" height="630" fill="url(#bg)"/>
  <rect x="60" y="60" width="1080" height="510" rx="28" fill="white" opacity="0.95"/>
  <text x="120" y="140" font-family="Arial, sans-serif" font-size="20" fill="#B27222" letter-spacing="4">SHLOKAS</text>
  <text x="120" y="200" font-family="Georgia, serif" font-size="38" fill="#7A3E16" font-weight="700">{html.escape(heading)}</text>
  {render_lines(sanskrit_lines, 120, 260, 38, 30, "#5A2C0A")}
  {render_lines(translation_lines, 120, 430, 30, 22, "#6B4F3B")}
</svg>"""

    return Response(content=svg, media_type="image/svg+xml")
