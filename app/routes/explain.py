from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models.text import Text
from app.models.translation import Translation
from app.llm.llm_provider import _groq_generate

router = APIRouter(prefix="/explain", tags=["Explain"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.post("/{text_id}")
async def get_explanation(text_id: str, language: str, db: Session = Depends(get_db)):
    """Return a detailed explanation for a shloka in the requested language.
    The response includes the explanation and a list of related verses.
    """
    # Retrieve the verse
    verse = db.query(Text).filter_by(id=text_id).first()
    if not verse:
        raise HTTPException(status_code=404, detail="Verse not found")

    # Build context with Sanskrit and any existing translation
    context_parts = [f"Sanskrit: {verse.sanskrit}"]
    existing_translation = db.query(Translation).filter_by(text_id=text_id, language=language).first()
    if existing_translation:
        context_parts.append(f"Existing {language} translation: {existing_translation.translation}")
    context = "\n".join(context_parts)

    system_prompt = (
        "You are a Vedic scholar. Provide a detailed explanation of the given shloka "
        "in the requested language, include references to Vedic sources, and suggest related verses."
    )
    user_prompt = f"Context:\n{context}\n\nExplain the meaning in {language}."

    explanation = _groq_generate(user_prompt, system_prompt)
    if not explanation:
        raise HTTPException(status_code=502, detail="Failed to obtain explanation from LLM")

    # Suggest related verses (same chapter, different verse)
    related = []
    if verse.chapter:
        related_verses = (
            db.query(Text.id, Text.chapter, Text.verse)
            .filter(Text.chapter == verse.chapter, Text.id != verse.id)
            .limit(3)
            .all()
        )
        for rel in related_verses:
            related.append({"id": rel.id, "title": f"Chapter {rel.chapter} Verse {rel.verse}"})

    return {"explanation": explanation, "related": related}
