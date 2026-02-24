from datetime import date, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.deps import get_db, get_current_user
from app.models.text import Text
from app.models.translation import Translation
from app.models.user import User


router = APIRouter(prefix="/verse-of-day", tags=["Verse of Day"])

_CACHE: dict = {"key": None, "text": None}


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
    if lower in {"sa", "sanskrit"}:
        return "sanskrit"
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


@router.get("")
def get_verse_of_day(
    lang: str | None = Query(None),
    work: str | None = Query(None),
    sub_work: str | None = Query(None),
    db: Session = Depends(get_db),
):
    today = date.today()
    cache_key = (today.isoformat(), work or "", sub_work or "")
    text_row = _CACHE.get("text") if _CACHE.get("key") == cache_key else None

    if not text_row:
        query = db.query(Text)
        if work:
            query = query.filter(Text.work == work)
        if sub_work:
            query = query.filter(Text.sub_work == sub_work)

        total = query.with_entities(func.count(Text.id)).scalar() or 0
        if total == 0:
            raise HTTPException(404, "No verses available for verse of the day.")

        seed = int(today.strftime("%Y%m%d"))
        offset = seed % total
        text_row = query.order_by(Text.id).offset(offset).first()
        _CACHE["key"] = cache_key
        _CACHE["text"] = text_row

    translations = (
        db.query(Translation)
        .filter(Translation.text_id == text_row.id)
        .all()
    )
    translation = pick_translation(translations, lang)

    payload = {
        "date": today.isoformat(),
        "id": text_row.id,
        "category": text_row.category,
        "work": text_row.work,
        "sub_work": text_row.sub_work,
        "chapter": text_row.chapter,
        "verse": text_row.verse,
        "sanskrit": text_row.sanskrit,
    }
    if translation:
        payload["translation"] = translation.translation
        payload["translation_language"] = normalize_language(translation.language)

    return payload


@router.post("/seen")
def mark_seen(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    today = date.today()
    last_seen = user.last_seen_verse_of_day

    if last_seen == today:
        return {
            "lastSeenVerseOfDay": last_seen.isoformat(),
            "streakCount": user.streak_count or 0,
        }

    if last_seen and last_seen == today - timedelta(days=1):
        user.streak_count = (user.streak_count or 0) + 1
    else:
        user.streak_count = 1

    user.last_seen_verse_of_day = today
    db.add(user)
    db.commit()
    db.refresh(user)
    return {
        "lastSeenVerseOfDay": user.last_seen_verse_of_day.isoformat(),
        "streakCount": user.streak_count or 0,
    }
