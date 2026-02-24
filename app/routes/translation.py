from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models.text import Text
from app.models.translation import Translation
from app.llm.translator import translate_and_explain
from app.utils.language import normalize_language

router = APIRouter(prefix="/translate", tags=["Translation"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.post("/{text_id}")
def get_translation(text_id: str, language: str, db: Session = Depends(get_db)):
    language = normalize_language(language)
    existing = db.query(Translation).filter_by(
        text_id=text_id, language=language
    ).first()

    if existing:
        return existing

    text = db.query(Text).filter_by(id=text_id).first()
    if not text:
        raise HTTPException(404, "Text not found")

    translation, commentary = translate_and_explain(
        text.sanskrit, language
    )

    record = Translation(
        text_id=text_id,
        language=language,
        translation=translation,
        commentary=commentary,
        generated_by="llm"
    )

    db.add(record)
    db.commit()
    db.refresh(record)

    return record
