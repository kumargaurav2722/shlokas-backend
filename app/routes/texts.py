from fastapi import APIRouter, Depends, Response
from sqlalchemy.orm import Session
from sqlalchemy import func
from app.database import SessionLocal
from app.models.text import Text
from app.models.translation import Translation
from app.models.text_stats import TextStats
import time

_CACHE = {}
_CACHE_TTL = 300


def _cache_get(key):
    entry = _CACHE.get(key)
    if not entry:
        return None
    if time.time() - entry["ts"] > _CACHE_TTL:
        _CACHE.pop(key, None)
        return None
    return entry["value"]


def _cache_set(key, value):
    _CACHE[key] = {"ts": time.time(), "value": value}


def _set_cache_headers(response: Response):
    response.headers["Cache-Control"] = f"public, max-age={_CACHE_TTL}"

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
def get_categories(response: Response, db: Session = Depends(get_db)):
    cache_key = "categories"
    cached = _cache_get(cache_key)
    if cached is not None:
        _set_cache_headers(response)
        return cached
    rows = db.query(Text.category).distinct().all()
    payload = [r[0] for r in rows]
    _cache_set(cache_key, payload)
    _set_cache_headers(response)
    return payload


@router.get("/works")
def get_works(category: str | None = None, response: Response = None, db: Session = Depends(get_db)):
    category = _normalize_category(category)
    cache_key = f"works:{category or ''}"
    cached = _cache_get(cache_key)
    if cached is not None:
        if response:
            _set_cache_headers(response)
        return cached
    query = db.query(Text.category, Text.work, Text.sub_work).distinct()
    if category:
        query = query.filter(Text.category == category)
    rows = query.all()
    payload = [
        {"category": r[0], "work": r[1], "sub_work": r[2]}
        for r in rows
    ]
    _cache_set(cache_key, payload)
    if response:
        _set_cache_headers(response)
    return payload

@router.get("/sub-works")
def get_sub_works(
    work: str,
    category: str | None = None,
    response: Response = None,
    db: Session = Depends(get_db)
):
    category = _normalize_category(category)
    cache_key = f"subworks:{category or ''}:{work}"
    cached = _cache_get(cache_key)
    if cached is not None:
        if response:
            _set_cache_headers(response)
        return cached
    query = db.query(Text.sub_work).filter(Text.work == work)
    if category:
        query = query.filter(Text.category == category)
    rows = query.distinct().all()
    payload = [r[0] for r in rows]
    _cache_set(cache_key, payload)
    if response:
        _set_cache_headers(response)
    return payload


@router.get("/sub-work-stats")
def get_sub_work_stats(
    work: str,
    category: str | None = None,
    response: Response = None,
    db: Session = Depends(get_db),
):
    category = _normalize_category(category)
    cache_key = f"subworkstats:{category or ''}:{work}"
    cached = _cache_get(cache_key)
    if cached is not None:
        if response:
            _set_cache_headers(response)
        return cached
    query = db.query(
        Text.sub_work.label("sub_work"),
        func.count(func.distinct(Text.chapter)).label("chapter_count"),
        func.count(Text.id).label("verse_count"),
    ).filter(Text.work == work)
    if category:
        query = query.filter(Text.category == category)
    rows = query.group_by(Text.sub_work).all()
    payload = [
        {
            "sub_work": r.sub_work,
            "chapter_count": r.chapter_count,
            "verse_count": r.verse_count,
        }
        for r in rows
    ]
    _cache_set(cache_key, payload)
    if response:
        _set_cache_headers(response)
    return payload

@router.get("/chapters")
def get_chapters(
    work: str,
    sub_work: str,
    category: str | None = None,
    response: Response = None,
    db: Session = Depends(get_db)
):
    category = _normalize_category(category)
    cache_key = f"chapters:{category or ''}:{work}:{sub_work}"
    cached = _cache_get(cache_key)
    if cached is not None:
        if response:
            _set_cache_headers(response)
        return cached
    query = db.query(Text.chapter).filter(
        Text.work == work,
        Text.sub_work == sub_work
    )
    if category:
        query = query.filter(Text.category == category)
    chapters = query.distinct().all()
    payload = [c[0] for c in chapters]
    _cache_set(cache_key, payload)
    if response:
        _set_cache_headers(response)
    return payload


@router.get("/chapter-stats")
def get_chapter_stats(
    work: str,
    sub_work: str,
    category: str | None = None,
    response: Response = None,
    db: Session = Depends(get_db),
):
    category = _normalize_category(category)
    cache_key = f"chapterstats:{category or ''}:{work}:{sub_work}"
    cached = _cache_get(cache_key)
    if cached is not None:
        if response:
            _set_cache_headers(response)
        return cached
    query = db.query(
        Text.chapter.label("chapter"),
        func.count(Text.id).label("verse_count"),
    ).filter(
        Text.work == work,
        Text.sub_work == sub_work,
    )
    if category:
        query = query.filter(Text.category == category)
    rows = query.group_by(Text.chapter).order_by(Text.chapter).all()
    payload = [
        {"chapter": r.chapter, "verse_count": r.verse_count}
        for r in rows
    ]
    _cache_set(cache_key, payload)
    if response:
        _set_cache_headers(response)
    return payload

@router.get("/verses")
def get_verses(
    work: str,
    sub_work: str,
    chapter: int,
    category: str | None = None,
    languages: str | None = None,
    response: Response = None,
    db: Session = Depends(get_db)
):
    category = _normalize_category(category)
    cache_key = f"verses:{category or ''}:{work}:{sub_work}:{chapter}:{languages or ''}"
    cached = _cache_get(cache_key)
    if cached is not None:
        if response:
            _set_cache_headers(response)
        return cached
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
    _cache_set(cache_key, payload)
    if response:
        _set_cache_headers(response)
    return payload


@router.get("/verse-index")
def get_verse_index(
    category: str | None = None,
    work: str | None = None,
    sub_work: str | None = None,
    limit: int | None = None,
    offset: int = 0,
    response: Response = None,
    db: Session = Depends(get_db),
):
    cache_key = f"verseindex:{category or ''}:{work or ''}:{sub_work or ''}:{limit}:{offset}"
    cached = _cache_get(cache_key)
    if cached is not None:
        if response:
            _set_cache_headers(response)
        return cached
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
    payload = [
        {
            "category": r[0],
            "work": r[1],
            "sub_work": r[2],
            "chapter": r[3],
            "verse": r[4],
        }
        for r in rows
    ]
    _cache_set(cache_key, payload)
    if response:
        _set_cache_headers(response)
    return payload
