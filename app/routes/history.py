from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import asc
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.deps import get_db, get_current_user
from app.models.history import History
from app.models.text_stats import TextStats
from app.models.user import User

router = APIRouter(prefix="/history", tags=["History"])


class HistoryRequest(BaseModel):
    type: str
    itemId: str
    title: str
    route: str


@router.get("")
def list_history(
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    try:
        rows = (
            db.query(History)
            .filter(History.user_id == user.id)
            .order_by(History.last_viewed_at.desc())
            .limit(50)
            .all()
        )
        return [
            {
                "id": r.id,
                "type": r.type,
                "itemId": r.item_id,
                "title": r.title,
                "route": r.route,
                "lastViewedAt": r.last_viewed_at,
            }
            for r in rows
        ]
    except SQLAlchemyError:
        raise HTTPException(500, "History service unavailable.")


@router.post("")
def add_history(
    payload: HistoryRequest,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    try:
        record = (
            db.query(History)
            .filter(
                History.user_id == user.id,
                History.type == payload.type,
                History.item_id == payload.itemId,
            )
            .first()
        )
        now = datetime.utcnow()
        if record:
            record.title = payload.title
            record.route = payload.route
            record.last_viewed_at = now
            db.add(record)
        else:
            record = History(
                user_id=user.id,
                type=payload.type,
                item_id=payload.itemId,
                title=payload.title,
                route=payload.route,
                last_viewed_at=now,
            )
            db.add(record)

        if payload.type in {"verse", "shloka"}:
            stats = db.query(TextStats).filter(TextStats.text_id == payload.itemId).first()
            if not stats:
                stats = TextStats(
                    text_id=payload.itemId,
                    read_count=1,
                    bookmark_count=0,
                    last_read_at=now,
                )
            else:
                stats.read_count = (stats.read_count or 0) + 1
                stats.last_read_at = now
            db.add(stats)

        db.commit()

        # Trim to last 50
        rows = (
            db.query(History)
            .filter(History.user_id == user.id)
            .order_by(asc(History.last_viewed_at))
            .all()
        )
        if len(rows) > 50:
            for old in rows[: len(rows) - 50]:
                db.delete(old)
            db.commit()

        return {"status": "ok"}
    except SQLAlchemyError:
        db.rollback()
        raise HTTPException(500, "History service unavailable.")
