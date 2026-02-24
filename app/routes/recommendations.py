from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.deps import get_db, get_current_user
from app.models.text import Text
from app.models.translation import Translation
from app.models.bookmark import Bookmark
from app.models.history import History
from app.models.user import User
from app.utils.language import normalize_language

router = APIRouter(prefix="/recommendations", tags=["Recommendations"])


INTEREST_VERSES = {
    "karma": [(2, 47), (3, 19), (18, 46)],
    "bhakti": [(9, 22), (12, 6), (12, 20)],
    "meditation": [(6, 5), (6, 10), (6, 26)],
    "fear": [(4, 10), (2, 56), (18, 30)],
    "anxiety": [(2, 14), (2, 48), (6, 32)],
    "success": [(2, 50), (3, 8), (18, 45)],
    "devotion": [(9, 26), (12, 13), (18, 66)],
    "self-realization": [(2, 20), (6, 29), (13, 22)],
}


def _make_route(chapter: int, verse: int) -> str:
    return (
        f"/scriptures/gita/Bhagavad%20Gita/{chapter}"
        f"?work=Mahabharata&sub_work=Bhagavad%20Gita&verse={verse}"
    )


@router.get("")
def get_recommendations(
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    preferred = normalize_language(user.preferred_language or "en")
    interests = [i.lower() for i in (user.interests or [])]

    candidates: list[tuple[int, int]] = []
    for interest in interests[:3]:
        candidates.extend(INTEREST_VERSES.get(interest, []))

    # Add recent history/bookmarks as fallback
    history_items = (
        db.query(History)
        .filter(History.user_id == user.id)
        .order_by(History.last_viewed_at.desc())
        .limit(10)
        .all()
    )
    for item in history_items:
        if item.type == "verse" and item.route:
            # skip; used only to avoid duplicates
            pass

    # Ensure unique list
    seen = set()
    unique_candidates = []
    for ch, vs in candidates:
        key = (ch, vs)
        if key not in seen:
            seen.add(key)
            unique_candidates.append(key)

    if not unique_candidates:
        unique_candidates = [(2, 47), (6, 5), (12, 20), (18, 66), (3, 19)]

    # Fetch texts for candidates
    texts = (
        db.query(Text)
        .filter(
            Text.work == "Mahabharata",
            Text.sub_work == "Bhagavad Gita",
            Text.chapter.in_([c for c, _ in unique_candidates]),
        )
        .all()
    )
    by_ch_verse = {(t.chapter, t.verse): t for t in texts}

    ids = [t.id for t in texts]
    translations = (
        db.query(Translation)
        .filter(Translation.text_id.in_(ids))
        .all()
    )
    tr_by_id = {}
    for tr in translations:
        tr_by_id.setdefault(tr.text_id, {})[tr.language.lower()] = tr.translation

    results = []
    for ch, vs in unique_candidates:
        text = by_ch_verse.get((ch, vs))
        if not text:
            continue
        tr_map = tr_by_id.get(text.id, {})
        preview = tr_map.get(preferred) or tr_map.get("English") or tr_map.get("Hindi") or ""
        results.append(
            {
                "type": "verse",
                "itemId": text.id,
                "title": f"Gita {ch}:{vs}",
                "route": _make_route(ch, vs),
                "sanskrit": text.sanskrit,
                "preview": preview,
            }
        )
        if len(results) >= 5:
            break

    return results
