from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models.puja_vidhi import PujaVidhi

router = APIRouter(prefix="/puja-vidhi", tags=["Puja Vidhi"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/")
def list_puja_vidhi(
    deity: str | None = None,
    language: str | None = None,
    db: Session = Depends(get_db),
):
    query = db.query(PujaVidhi)
    if deity:
        query = query.filter(PujaVidhi.deity == deity)
    if language:
        query = query.filter(PujaVidhi.language == language)
    return query.all()


@router.get("/{vidhi_id}")
def get_puja_vidhi(vidhi_id: str, db: Session = Depends(get_db)):
    record = db.query(PujaVidhi).filter(PujaVidhi.id == vidhi_id).first()
    if not record:
        raise HTTPException(404, "Puja vidhi not found")
    return record
