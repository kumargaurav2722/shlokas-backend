import argparse
from typing import Iterable

from app.database import SessionLocal
from app.models.text import Text
from app.models.translation import Translation
from app.llm.translator import translate_and_explain


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch translate shlokas using local Ollama")
    parser.add_argument("--scope", choices=["gita", "all"], default="gita")
    parser.add_argument("--language", default="English")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--commit-every", type=int, default=20)
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
                db.query(Translation)
                .filter(
                    Translation.text_id == text.id,
                    Translation.language == args.language,
                )
                .first()
            )
            if existing:
                continue

            translation, commentary = translate_and_explain(
                text.sanskrit, args.language
            )
            record = Translation(
                text_id=text.id,
                language=args.language,
                translation=translation,
                commentary=commentary,
                generated_by="llm",
            )
            db.add(record)
            created += 1
            processed += 1

            if args.commit_every and processed % args.commit_every == 0:
                db.commit()

            if args.limit and created >= args.limit:
                break

        db.commit()
        print(f"Created {created} translations")
    finally:
        db.close()


if __name__ == "__main__":
    main()
