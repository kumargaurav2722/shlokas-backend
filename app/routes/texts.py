from fastapi import APIRouter, Depends, Response
from sqlalchemy.orm import Session
from sqlalchemy import func
from app.database import SessionLocal
from app.models.text import Text
from app.models.translation import Translation
from app.models.text_stats import TextStats
from fastapi_cache.decorator import cache

# Normalize category names (frontend uses plural, DB uses singular)
_CATEGORY_ALIASES = {
    "vedas": "veda",
    "puranas": "purana",
    "upanishads": "upanishad",
    "epics": "itihasa",
    "gita": "itihasa",
    "mahabharata": "itihasa",
    "ramayana": "itihasa",
}

def _normalize_category(category: str | None) -> str | None:
    if not category:
        return category
    return _CATEGORY_ALIASES.get(category.lower(), category)

router = APIRouter(prefix="/texts", tags=["Texts"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/categories")
@cache(expire=3600)
def get_categories(db: Session = Depends(get_db)):
    rows = db.query(Text.category).distinct().all()
    return [r[0] for r in rows]

@router.get("/works")
@cache(expire=3600)
def get_works(category: str | None = None, db: Session = Depends(get_db)):
    category = _normalize_category(category)
    query = db.query(Text.category, Text.work, Text.sub_work).distinct()
    if category:
        query = query.filter(Text.category == category)
    rows = query.all()
    return [{"category": r[0], "work": r[1], "sub_work": r[2]} for r in rows]

@router.get("/sub-works")
@cache(expire=3600)
def get_sub_works(work: str, category: str | None = None, db: Session = Depends(get_db)):
    import logging
    logger = logging.getLogger(__name__)
    category = _normalize_category(category)
    try:
        query = db.query(Text.sub_work).filter(Text.work == work)
        if category:
            query = query.filter(Text.category == category)
        rows = query.distinct().all()
        return [r[0] for r in rows]
    except Exception as e:
        logger.error("Error in /texts/sub-works: %s", e)
        db.rollback()
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=500, content={"detail": f"Database error: {str(e)}"})

@router.get("/sub-work-stats")
@cache(expire=3600)
def get_sub_work_stats(work: str, category: str | None = None, db: Session = Depends(get_db)):
    category = _normalize_category(category)
    query = db.query(
        Text.sub_work.label("sub_work"),
        func.count(func.distinct(Text.chapter)).label("chapter_count"),
        func.count(Text.id).label("verse_count"),
    ).filter(Text.work == work)
    if category:
        query = query.filter(Text.category == category)
    rows = query.group_by(Text.sub_work).all()
    return [
        {"sub_work": r.sub_work, "chapter_count": r.chapter_count, "verse_count": r.verse_count}
        for r in rows
    ]

@router.get("/chapters")
@cache(expire=3600)
def get_chapters(work: str, sub_work: str, category: str | None = None, db: Session = Depends(get_db)):
    category = _normalize_category(category)
    query = db.query(Text.chapter).filter(Text.work == work, Text.sub_work == sub_work)
    if category:
        query = query.filter(Text.category == category)
    chapters = query.distinct().all()
    return [c[0] for c in chapters]

@router.get("/chapter-stats")
@cache(expire=3600)
def get_chapter_stats(work: str, sub_work: str, category: str | None = None, db: Session = Depends(get_db)):
    category = _normalize_category(category)
    query = db.query(
        Text.chapter.label("chapter"),
        func.count(Text.id).label("verse_count"),
    ).filter(Text.work == work, Text.sub_work == sub_work)
    if category:
        query = query.filter(Text.category == category)
    rows = query.group_by(Text.chapter).order_by(Text.chapter).all()
    return [{"chapter": r.chapter, "verse_count": r.verse_count} for r in rows]

@router.get("/verses")
@cache(expire=3600)
def get_verses(
    work: str,
    sub_work: str,
    chapter: int,
    category: str | None = None,
    languages: str | None = None,
    db: Session = Depends(get_db)
):
    category = _normalize_category(category)
    query = db.query(Text).filter(
        Text.work == work,
        Text.sub_work == sub_work,
        Text.chapter == chapter
    )
    if category:
        query = query.filter(Text.category == category)
    rows = query.order_by(Text.verse).all()
    if not languages:
        payload = [
            {
                "id": r.id,
                "chapter": r.chapter,
                "verse": r.verse,
                "sanskrit": r.sanskrit,
                "category": r.category,
                "work": r.work,
                "sub_work": r.sub_work,
                "source": r.source,
            }
            for r in rows
        ]
        _cache_set(cache_key, payload)
        if response:
            _set_cache_headers(response)
        return payload

    lang_list = [l.strip() for l in languages.split(",") if l.strip()]
    ids = [r.id for r in rows]
    translations = (
        db.query(Translation)
        .filter(Translation.text_id.in_(ids), Translation.language.in_(lang_list))
        .all()
    )
    by_id = {}
    meta_by_id = {}
    for tr in translations:
        by_id.setdefault(tr.text_id, {})[tr.language] = tr.translation
        meta_by_id.setdefault(tr.text_id, {})[tr.language] = {
            "generated_by": tr.generated_by,
            "commentary": tr.commentary,
        }

    # text_stats has a type mismatch (uuid vs varchar) — gracefully degrade
    stats_by_id = {}
    try:
        stats_rows = db.query(TextStats).filter(TextStats.text_id.in_(ids)).all()
        stats_by_id = {s.text_id: s for s in stats_rows}
    except Exception:
        db.rollback()  # reset the failed transaction

    def lang_key(language: str) -> str:
        key = language.lower()
        if key == "hindi":
            return "hi"
        if key == "english":
            return "en"
        return key

    payload = []
    for r in rows:
        stats = stats_by_id.get(r.id)
        entry = {
            "id": r.id,
            "chapter": r.chapter,
            "verse": r.verse,
            "sanskrit": r.sanskrit,
            "category": r.category,
            "work": r.work,
            "sub_work": r.sub_work,
            "source": r.source,
            "readCount": stats.read_count if stats else 0,
            "bookmarkCount": stats.bookmark_count if stats else 0,
            "translationMeta": meta_by_id.get(r.id, {}),
        }
        for lang, value in by_id.get(r.id, {}).items():
            entry[lang_key(lang)] = value
        payload.append(entry)
    return payload

@router.get("/verse-index")
@cache(expire=3600)
def get_verse_index(
    category: str | None = None,
    work: str | None = None,
    sub_work: str | None = None,
    limit: int | None = None,
    offset: int = 0,
    db: Session = Depends(get_db),
):
    query = db.query(
        Text.category,
        Text.work,
        Text.sub_work,
        Text.chapter,
        Text.verse,
    )
    if category:
        query = query.filter(Text.category == category)
    if work:
        query = query.filter(Text.work == work)
    if sub_work:
        query = query.filter(Text.sub_work == sub_work)
    query = query.order_by(
        Text.category,
        Text.work,
        Text.sub_work,
        Text.chapter,
        Text.verse,
    )
    if limit is not None:
        query = query.limit(limit).offset(offset)
    rows = query.all()
    return [
        {
            "category": r[0],
            "work": r[1],
            "sub_work": r[2],
            "chapter": r[3],
            "verse": r[4],
        }
        for r in rows
    ]
