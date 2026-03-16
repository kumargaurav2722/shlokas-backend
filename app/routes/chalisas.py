from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models.chalisa import Chalisa

router = APIRouter(prefix="/chalisas", tags=["Chalisas"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/")
def list_chalisas(
    deity: str | None = None,
    language: str | None = None,
    db: Session = Depends(get_db),
):
    query = db.query(Chalisa)
    if deity:
        query = query.filter(Chalisa.deity == deity)
    if language:
        query = query.filter(Chalisa.language == language)
    return query.all()


@router.get("/{chalisa_id}")
def get_chalisa(chalisa_id: str, db: Session = Depends(get_db)):
    record = db.query(Chalisa).filter(Chalisa.id == chalisa_id).first()
    if not record:
        raise HTTPException(404, "Chalisa not found")
    return record
