from fastapi import APIRouter, Depends
from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models.chalisa import Chalisa
from app.models.puja_vidhi import PujaVidhi
from app.models.text import Text
from app.models.translation import Translation

router = APIRouter(tags=["Search"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _snippet(text: str | None, limit: int = 160) -> str:
    if not text:
        return ""
    compact = " ".join(text.split())
    return compact[:limit] + ("…" if len(compact) > limit else "")


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


def _text_path(
    category: str | None,
    work: str | None,
    sub_work: str | None,
    chapter: int | None = None,
    verse: int | None = None,
) -> str:
    if work == "Mahabharata" and sub_work == "Bhagavad Gita":
        if chapter is None:
            return "/bhagavad-gita"
        if verse is None:
            return f"/bhagavad-gita/chapter-{chapter}"
        return f"/bhagavad-gita/chapter-{chapter}/verse-{verse}"

    if category == "upanishad":
        text_slug = _slugify(sub_work or work or "upanishads")
        if verse is not None and chapter is not None:
            return f"/upanishads/{text_slug}/verse-{chapter}-{verse}"
        if chapter is not None:
            return f"/upanishads/{text_slug}/chapter-{chapter}"
        return f"/upanishads/{text_slug}"

    category_slug = _category_slug(category, work, sub_work)
    text_slug = _slugify(sub_work or work or "text")
    if chapter is None:
        return f"/scriptures/{category_slug}/{text_slug}"
    if verse is None:
        return f"/scriptures/{category_slug}/{text_slug}/chapter-{chapter}"
    return f"/scriptures/{category_slug}/{text_slug}/chapter-{chapter}/verse-{verse}"


@router.get("/search")
def search(
    query: str,
    type: str | None = None,
    limit: int = 30,
    db: Session = Depends(get_db),
):
    q = query.strip()
    if not q:
        return {"results": []}

    results: list[dict] = []
    like = f"%{q}%"

    if not type or type in {"shloka", "verse", "text"}:
        text_rows = (
            db.query(Text)
            .filter(
                or_(
                    Text.sanskrit.ilike(like),
                    Text.content.ilike(like),
                )
            )
            .limit(limit)
            .all()
        )
        for row in text_rows:
            results.append(
                {
                    "id": row.id,
                    "title": f"{row.sub_work or row.work} {row.chapter}:{row.verse}",
                    "snippet": _snippet(row.sanskrit),
                    "type": "verse",
                    "path": _text_path(
                        row.category,
                        row.work,
                        row.sub_work,
                        row.chapter,
                        row.verse,
                    ),
                }
            )

        if len(results) < limit:
            tr_rows = (
                db.query(Translation, Text)
                .join(Text, Translation.text_id == Text.id)
                .filter(
                    or_(
                        Translation.translation.ilike(like),
                        Translation.commentary.ilike(like),
                    )
                )
                .limit(limit - len(results))
                .all()
            )
            for tr, text in tr_rows:
                results.append(
                    {
                        "id": tr.id,
                        "title": f"{text.sub_work or text.work} {text.chapter}:{text.verse}",
                        "snippet": _snippet(tr.translation),
                        "type": "verse",
                    "path": _text_path(
                        text.category,
                        text.work,
                        text.sub_work,
                        text.chapter,
                        text.verse,
                    ),
                    }
                )

    if not type or type == "chalisa":
        rows = (
            db.query(Chalisa)
            .filter(
                or_(
                    Chalisa.title.ilike(like),
                    Chalisa.deity.ilike(like),
                    Chalisa.content.ilike(like),
                )
            )
            .limit(limit)
            .all()
        )
        for row in rows:
            results.append(
                {
                    "id": row.id,
                    "title": row.title,
                    "snippet": _snippet(row.content),
                    "type": "chalisa",
                    "path": f"/chalisas",
                }
            )

    if not type or type == "puja":
        rows = (
            db.query(PujaVidhi)
            .filter(
                or_(
                    PujaVidhi.title.ilike(like),
                    PujaVidhi.deity.ilike(like),
                    PujaVidhi.content.ilike(like),
                )
            )
            .limit(limit)
            .all()
        )
        for row in rows:
            results.append(
                {
                    "id": row.id,
                    "title": row.title,
                    "snippet": _snippet(row.content),
                    "type": "puja",
                    "path": f"/puja-vidhi",
                }
            )

    return {"results": results[:limit]}
