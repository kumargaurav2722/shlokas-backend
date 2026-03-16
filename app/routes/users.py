from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session

from app.deps import get_db, get_current_user
from app.models.user import User

router = APIRouter(prefix="/users", tags=["Users"])


class UserProfileUpdate(BaseModel):
    fullName: str | None = None
    age: int | None = None
    gender: str | None = None
    region: str | None = None
    preferredLanguage: str | None = None
    interests: list[str] | None = None


@router.get("/me")
def get_me(user: User = Depends(get_current_user)):
    return {
        "id": user.id,
        "email": user.email,
        "fullName": user.full_name,
        "age": user.age,
        "gender": user.gender,
        "region": user.region,
        "preferredLanguage": user.preferred_language,
        "interests": user.interests or [],
        "lastSeenVerseOfDay": user.last_seen_verse_of_day.isoformat()
        if user.last_seen_verse_of_day
        else None,
        "streakCount": user.streak_count or 0,
    }


@router.put("/me")
def update_me(
    payload: UserProfileUpdate,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    if payload.fullName is not None:
        user.full_name = payload.fullName
    if payload.age is not None:
        user.age = payload.age
    if payload.gender is not None:
        user.gender = payload.gender
    if payload.region is not None:
        user.region = payload.region
    if payload.preferredLanguage is not None:
        user.preferred_language = payload.preferredLanguage
    if payload.interests is not None:
        user.interests = payload.interests

    db.add(user)
    db.commit()
    db.refresh(user)
    return {
        "id": user.id,
        "email": user.email,
        "fullName": user.full_name,
        "age": user.age,
        "gender": user.gender,
        "region": user.region,
        "preferredLanguage": user.preferred_language,
        "interests": user.interests or [],
        "lastSeenVerseOfDay": user.last_seen_verse_of_day.isoformat()
        if user.last_seen_verse_of_day
        else None,
        "streakCount": user.streak_count or 0,
    }
