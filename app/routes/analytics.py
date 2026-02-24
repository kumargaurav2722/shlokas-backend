from fastapi import APIRouter, Depends, Header
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import func, desc

from app.database import SessionLocal
from app.models.analytics import AnalyticsEvent
from app.models.user import User
from app.models.topic import Topic, TopicItem
from app.models.text_stats import TextStats
from app.deps import get_current_user

router = APIRouter(prefix="/analytics", tags=["Analytics"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_current_user_optional(
    authorization: str | None = Header(None),
    db: Session = Depends(get_db),
) -> User | None:
    if not authorization:
        return None
    try:
        return get_current_user(authorization=authorization, db=db)
    except Exception:
        return None


class EventRequest(BaseModel):
    eventType: str
    metadata: dict | None = None


@router.post("/event")
def log_event(
    payload: EventRequest,
    user: User | None = Depends(get_current_user_optional),
    db: Session = Depends(get_db),
):
    record = AnalyticsEvent(
        user_id=user.id if user else None,
        event_type=payload.eventType,
        meta=payload.metadata or {},
    )
    db.add(record)
    db.commit()
    return {"status": "ok"}


@router.get("/summary")
def analytics_summary(db: Session = Depends(get_db)):
    language_counts = (
        db.query(User.preferred_language, func.count(User.id))
        .group_by(User.preferred_language)
        .all()
    )

    top_topics = (
        db.query(Topic.slug, func.sum(TextStats.read_count))
        .join(TopicItem, TopicItem.topic_id == Topic.id)
        .join(TextStats, TextStats.text_id == TopicItem.text_id)
        .group_by(Topic.slug)
        .order_by(desc(func.sum(TextStats.read_count)))
        .limit(10)
        .all()
    )

    top_shares = (
        db.query(AnalyticsEvent.meta["textId"].astext, func.count(AnalyticsEvent.id))
        .filter(AnalyticsEvent.event_type == "share")
        .group_by(AnalyticsEvent.meta["textId"].astext)
        .order_by(desc(func.count(AnalyticsEvent.id)))
        .limit(10)
        .all()
    )

    return {
        "languages": [
            {"language": lang or "unknown", "count": count}
            for lang, count in language_counts
        ],
        "topTopics": [
            {"topic": slug, "readCount": count or 0}
            for slug, count in top_topics
        ],
        "topShares": [
            {"textId": text_id, "shareCount": count}
            for text_id, count in top_shares
        ],
    }
