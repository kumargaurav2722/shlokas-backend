import mimetypes
import os
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models.audio import Audio
from app.models.text import Text
from app.models.translation import Translation
from app.audio.tts import generate_audio
from app.llm.translator import translate_and_explain
from app.utils.language import normalize_language

router = APIRouter(prefix="/audio", tags=["Audio"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def _audio_public_url(path: str | None) -> str | None:
    if not path:
        return None
    base = os.getenv("AUDIO_PUBLIC_BASE_URL") or os.getenv("AUDIO_CDN_BASE_URL")
    if not base:
        return None
    normalized = path[1:] if path.startswith("/") else path
    return f"{base.rstrip('/')}/{normalized}"


def _serialize_audio(audio: Audio) -> dict:
    return {
        "id": audio.id,
        "text_id": audio.text_id,
        "language": audio.language,
        "audio_path": audio.audio_path,
        "audio_url": _audio_public_url(audio.audio_path),
        "voice_type": audio.voice_type,
    }

@router.post("/{text_id}")
def get_audio(
    text_id: str,
    language: str,
    auto_translate: bool = True,
    db: Session = Depends(get_db)
):
    language = normalize_language(language)
    existing = db.query(Audio).filter_by(
        text_id=text_id, language=language
    ).first()

    if existing:
        return _serialize_audio(existing)

    if language == "sanskrit":
        text = db.query(Text).filter_by(id=text_id).first()
        if not text:
            raise HTTPException(404, "Text not found")
        content = text.sanskrit
    else:
        trans = db.query(Translation).filter_by(
            text_id=text_id, language=language
        ).first()
        if not trans and auto_translate:
            text = db.query(Text).filter_by(id=text_id).first()
            if not text:
                raise HTTPException(404, "Text not found")
            translation, commentary = translate_and_explain(
                text.sanskrit, language
            )
            trans = Translation(
                text_id=text_id,
                language=language,
                translation=translation,
                commentary=commentary,
                generated_by="llm"
            )
            db.add(trans)
            db.commit()
            db.refresh(trans)
        if not trans:
            raise HTTPException(404, "Translation not found")
        content = trans.translation

    path, voice_type = generate_audio(content, text_id, language)

    audio = Audio(
        text_id=text_id,
        language=language,
        audio_path=path,
        voice_type=voice_type
    )

    db.add(audio)
    db.commit()
    db.refresh(audio)

    return _serialize_audio(audio)


@router.get("/{text_id}/file")
def get_audio_file(
    text_id: str,
    language: str,
    auto_translate: bool = True,
    db: Session = Depends(get_db),
):
    language = normalize_language(language)
    existing = db.query(Audio).filter_by(text_id=text_id, language=language).first()
    if not existing:
        # Generate if missing
        audio = get_audio(text_id=text_id, language=language, auto_translate=auto_translate, db=db)
        existing = audio

    path = existing.audio_path
    if not path:
        raise HTTPException(404, "Audio not available")

    if not os.path.exists(path):
        # Attempt to regenerate if file missing
        audio = get_audio(text_id=text_id, language=language, auto_translate=auto_translate, db=db)
        path = audio.audio_path

    if not os.path.exists(path):
        raise HTTPException(404, "Audio file missing")

    media_type, _ = mimetypes.guess_type(path)
    headers = {"Cache-Control": "public, max-age=31536000, immutable"}
    return FileResponse(path, media_type=media_type or "audio/mpeg", headers=headers)
