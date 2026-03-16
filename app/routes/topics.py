from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.deps import get_db
from app.models.text import Text
from app.models.translation import Translation
from app.models.topic import Topic, TopicItem, TopicTranslation

router = APIRouter(prefix="/topics", tags=["Topics"])


INTEREST_VERSES = {
    "karma": [(2, 47), (3, 19), (18, 46)],
    "bhakti": [(9, 22), (12, 6), (12, 20)],
    "meditation": [(6, 5), (6, 10), (6, 26)],
    "fear": [(4, 10), (2, 56), (18, 30)],
    "anxiety": [(2, 14), (2, 48), (6, 32)],
    "anger": [(2, 63), (16, 21), (5, 26)],
    "detachment": [(2, 47), (5, 10), (12, 19)],
    "success": [(2, 50), (3, 8), (18, 45)],
    "failure": [(2, 47), (6, 5), (18, 66)],
    "death": [(2, 20), (2, 27), (8, 6)],
    "devotion": [(9, 26), (12, 13), (18, 66)],
    "self-realization": [(2, 20), (6, 29), (13, 22)],
}


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


@router.get("/")
def list_topics(lang: str | None = None, db: Session = Depends(get_db)):
    if lang:
        rows = (
            db.query(Topic, TopicTranslation)
            .outerjoin(
                TopicTranslation,
                (TopicTranslation.topic_id == Topic.id)
                & (TopicTranslation.language == lang),
            )
            .order_by(Topic.slug)
            .all()
        )
        if rows:
            return [
                {
                    "slug": topic.slug,
                    "name": translation.name if translation else topic.name,
                    "description": (translation.description if translation else topic.description),
                }
                for topic, translation in rows
            ]

    rows = db.query(Topic).order_by(Topic.slug).all()
    if rows:
        return [
            {"slug": row.slug, "name": row.name, "description": row.description}
            for row in rows
        ]
    return sorted(INTEREST_VERSES.keys())


@router.get("/{topic}")
def get_topic(topic: str, db: Session = Depends(get_db)):
    slug = topic.lower()
    topic_row = db.query(Topic).filter(Topic.slug == slug).first()
    if topic_row:
        items = (
            db.query(TopicItem, Text)
            .join(Text, TopicItem.text_id == Text.id)
            .filter(TopicItem.topic_id == topic_row.id)
            .order_by(TopicItem.score.desc().nullslast(), Text.chapter, Text.verse)
            .limit(20)
            .all()
        )
        if not items:
            return []

        text_ids = [text.id for _, text in items]
        translations = (
            db.query(Translation)
            .filter(Translation.text_id.in_(text_ids))
            .all()
        )
        tr_by_id = {}
        for tr in translations:
            tr_by_id.setdefault(tr.text_id, {})[tr.language] = tr.translation

        results = []
        for _, text in items:
            tr_map = tr_by_id.get(text.id, {})
            preview = tr_map.get("English") or tr_map.get("Hindi") or ""
            results.append(
                {
                    "type": "verse",
                    "itemId": text.id,
                    "title": f"{text.sub_work or text.work} {text.chapter}:{text.verse}",
                    "route": _verse_route(text),
                    "sanskrit": text.sanskrit,
                    "preview": preview,
                }
            )
        return results

    verses = INTEREST_VERSES.get(slug, [])
    if not verses:
        return []

    texts = (
        db.query(Text)
        .filter(
            Text.work == "Mahabharata",
            Text.sub_work == "Bhagavad Gita",
            Text.chapter.in_([c for c, _ in verses]),
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
        tr_by_id.setdefault(tr.text_id, {})[tr.language] = tr.translation

    results = []
    for ch, vs in verses:
        text = by_ch_verse.get((ch, vs))
        if not text:
            continue
        tr_map = tr_by_id.get(text.id, {})
        preview = tr_map.get("English") or tr_map.get("Hindi") or ""
        results.append(
            {
                "type": "verse",
                "itemId": text.id,
                "title": f"Gita {ch}:{vs}",
                "route": f"/bhagavad-gita/chapter-{ch}/verse-{vs}",
                "sanskrit": text.sanskrit,
                "preview": preview,
            }
        )
    return results
