from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.deps import get_db, get_current_user
from app.models.bookmark import Bookmark
from app.models.text_stats import TextStats
from datetime import datetime
from app.models.user import User

router = APIRouter(prefix="/bookmarks", tags=["Bookmarks"])


class BookmarkRequest(BaseModel):
    type: str
    itemId: str
    title: str
    route: str


@router.get("")
def list_bookmarks(
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    rows = (
        db.query(Bookmark)
        .filter(Bookmark.user_id == user.id)
        .order_by(Bookmark.created_at.desc())
        .all()
    )
    return [
        {
            "id": r.id,
            "type": r.type,
            "itemId": r.item_id,
            "title": r.title,
            "route": r.route,
            "createdAt": r.created_at,
        }
        for r in rows
    ]


@router.post("")
def add_bookmark(
    payload: BookmarkRequest,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    existing = (
        db.query(Bookmark)
        .filter(
            Bookmark.user_id == user.id,
            Bookmark.type == payload.type,
            Bookmark.item_id == payload.itemId,
        )
        .first()
    )
    if existing:
        return {
            "id": existing.id,
            "type": existing.type,
            "itemId": existing.item_id,
            "title": existing.title,
            "route": existing.route,
            "createdAt": existing.created_at,
        }

    record = Bookmark(
        user_id=user.id,
        type=payload.type,
        item_id=payload.itemId,
        title=payload.title,
        route=payload.route,
    )
    db.add(record)

    if payload.type in {"verse", "shloka"}:
        stats = db.query(TextStats).filter(TextStats.text_id == payload.itemId).first()
        now = datetime.utcnow()
        if not stats:
            stats = TextStats(
                text_id=payload.itemId,
                read_count=0,
                bookmark_count=1,
                last_bookmarked_at=now,
            )
        else:
            stats.bookmark_count = (stats.bookmark_count or 0) + 1
            stats.last_bookmarked_at = now
        db.add(stats)

    db.commit()
    db.refresh(record)
    return {
        "id": record.id,
        "type": record.type,
        "itemId": record.item_id,
        "title": record.title,
        "route": record.route,
        "createdAt": record.created_at,
    }


@router.delete("")
def remove_bookmark(
    type: str,
    itemId: str,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    record = (
        db.query(Bookmark)
        .filter(
            Bookmark.user_id == user.id,
            Bookmark.type == type,
            Bookmark.item_id == itemId,
        )
        .first()
    )
    if not record:
        raise HTTPException(404, "Bookmark not found")
    db.delete(record)

    if type in {"verse", "shloka"}:
        stats = db.query(TextStats).filter(TextStats.text_id == itemId).first()
        if stats and stats.bookmark_count:
            stats.bookmark_count = max(0, stats.bookmark_count - 1)
            db.add(stats)

    db.commit()
    return {"status": "deleted"}
