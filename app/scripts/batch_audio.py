import argparse
from typing import Iterable, Optional

from app.audio.tts import generate_audio
from app.database import SessionLocal
from app.llm.translator import translate_and_explain
from app.models.audio import Audio
from app.models.text import Text
from app.models.translation import Translation


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch generate audio for texts")
    parser.add_argument("--scope", choices=["gita", "all"], default="gita")
    parser.add_argument("--language", default="English")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--commit-every", type=int, default=20)
    parser.add_argument("--auto-translate", action="store_true")
    return parser.parse_args()


def iter_texts(scope: str, db) -> Iterable[Text]:
    query = db.query(Text)
    if scope == "gita":
        query = query.filter(
            Text.category == "itihasa",
            Text.work == "Mahabharata",
            Text.sub_work == "Bhagavad Gita",
        )
    return query.order_by(Text.chapter, Text.verse)


def get_text_content(
    text: Text, language: str, db, auto_translate: bool
) -> Optional[str]:
    if language.lower() == "sanskrit":
        return text.sanskrit or text.content

    translation = (
        db.query(Translation)
        .filter(
            Translation.text_id == text.id,
            Translation.language == language,
        )
        .first()
    )
    if translation:
        return translation.translation

    if not auto_translate:
        return None

    translation_text, commentary = translate_and_explain(
        text.sanskrit, language
    )
    record = Translation(
        text_id=text.id,
        language=language,
        translation=translation_text,
        commentary=commentary,
        generated_by="llm",
    )
    db.add(record)
    db.commit()
    return translation_text


def main() -> None:
    args = parse_args()
    db = SessionLocal()
    try:
        created = 0
        processed = 0
        query = iter_texts(args.scope, db)
        if args.offset:
            query = query.offset(args.offset)
        if args.limit:
            query = query.limit(args.limit)

        for text in query:
            existing = (
                db.query(Audio)
                .filter(
                    Audio.text_id == text.id,
                    Audio.language == args.language,
                )
                .first()
            )
            if existing:
                continue

            content = get_text_content(
                text, args.language, db, args.auto_translate
            )
            if not content:
                continue

            path, voice_type = generate_audio(
                content, text.id, args.language
            )
            record = Audio(
                text_id=text.id,
                language=args.language,
                audio_path=path,
                voice_type=voice_type,
            )
            db.add(record)
            created += 1
            processed += 1

            if args.commit_every and processed % args.commit_every == 0:
                db.commit()

            if args.limit and created >= args.limit:
                break

        db.commit()
        print(f"Created {created} audio files")
    finally:
        db.close()


if __name__ == "__main__":
    main()
