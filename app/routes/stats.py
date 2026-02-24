from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func, desc

from app.deps import get_db
from app.models.text import Text
from app.models.text_stats import TextStats

router = APIRouter(prefix="/stats", tags=["Stats"])


def _slugify(text: str) -> str:
    compact = "".join(ch.lower() if ch.isalnum() else "-" for ch in text)
    while "--" in compact:
        compact = compact.replace("--", "-")
    return compact.strip("-")


def _category_slug(category: str | None, work: str | None, sub_work: str | None) -> str:
    if category == "veda":
        return "vedas"
    if category == "upanishad":
        return "upanishads"
    if category == "purana":
        return "puranas"
    if category == "itihasa":
        if work == "Mahabharata" and sub_work == "Bhagavad Gita":
            return "bhagavad-gita"
        if work == "Ramayana":
            return "ramayana"
        if work == "Mahabharata":
            return "mahabharata"
        return "itihasa"
    return "scriptures"


def _verse_route(text: Text) -> str:
    if text.work == "Mahabharata" and text.sub_work == "Bhagavad Gita":
        return f"/bhagavad-gita/chapter-{text.chapter}/verse-{text.verse}"
    if text.category == "upanishad":
        slug = _slugify(text.sub_work or text.work or "upanishads")
        return f"/upanishads/{slug}/verse-{text.chapter}-{text.verse}"
    category_slug = _category_slug(text.category, text.work, text.sub_work)
    text_slug = _slugify(text.sub_work or text.work or "text")
    return f"/scriptures/{category_slug}/{text_slug}/chapter-{text.chapter}/verse-{text.verse}"


@router.get("/trending")
def get_trending(limit: int = 10, db: Session = Depends(get_db)):
    rows = (
        db.query(Text, TextStats)
        .join(TextStats, Text.id == TextStats.text_id)
        .order_by(desc(TextStats.read_count), desc(TextStats.last_read_at))
        .limit(limit)
        .all()
    )
    return [
        {
            "textId": text.id,
            "title": f"{text.sub_work or text.work} {text.chapter}:{text.verse}",
            "route": _verse_route(text),
            "readCount": stats.read_count,
            "bookmarkCount": stats.bookmark_count,
        }
        for text, stats in rows
    ]


@router.get("/verse/{text_id}")
def get_verse_stats(text_id: str, db: Session = Depends(get_db)):
    stats = db.query(TextStats).filter(TextStats.text_id == text_id).first()
    if not stats:
        return {"textId": text_id, "readCount": 0, "bookmarkCount": 0}
    return {
        "textId": stats.text_id,
        "readCount": stats.read_count,
        "bookmarkCount": stats.bookmark_count,
    }
