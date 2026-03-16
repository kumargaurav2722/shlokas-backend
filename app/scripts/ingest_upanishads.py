import argparse
from typing import Set, Tuple

from app.database import SessionLocal
from app.models.text import Text
from app.scripts.ingest_utils import load_rows, normalize_rows, clamp_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Upanishad mantras into texts table")
    parser.add_argument("--input", required=True, help="Path to JSON or CSV file")
    parser.add_argument("--format", choices=["json", "csv"], required=True)
    parser.add_argument("--category", default="upanishad")
    parser.add_argument("--work", default="Upanishads")
    parser.add_argument("--sub-work", default="Isha", dest="sub_work")
    parser.add_argument("--source", default="Public Domain")
    parser.add_argument("--sanskrit-field", default="sanskrit")
    parser.add_argument("--chapter-field", default="chapter")
    parser.add_argument("--verse-field", default="verse")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--dedupe", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    return parser.parse_args()


def existing_keys(db, category: str, work: str, sub_work: str) -> Set[Tuple[int, int]]:
    rows = (
        db.query(Text.chapter, Text.verse)
        .filter(Text.category == category, Text.work == work, Text.sub_work == sub_work)
        .all()
    )
    return {(r[0], r[1]) for r in rows}


def main() -> None:
    args = parse_args()

    rows = load_rows(
        path=args.input,
        fmt=args.format,
        chapter_field=args.chapter_field,
        verse_field=args.verse_field,
        sanskrit_field=args.sanskrit_field,
    )
    rows = normalize_rows(rows)
    rows = clamp_rows(rows, args.limit)

    db = SessionLocal()
    try:
        existing = existing_keys(db, args.category, args.work, args.sub_work) if args.dedupe else set()
        created = 0
        skipped = 0

        for row in rows:
            key = (row["chapter"], row["verse"])
            if key in existing:
                skipped += 1
                continue

            record = Text(
                category=args.category,
                work=args.work,
                sub_work=args.sub_work,
                chapter=row["chapter"],
                verse=row["verse"],
                sanskrit=row["sanskrit"],
                source=args.source,
                content=row["sanskrit"],
            )
            db.add(record)
            created += 1

        if args.dry_run:
            db.rollback()
        else:
            db.commit()

        print(f"Ingested {created} rows (skipped {skipped})")
    finally:
        db.close()


if __name__ == "__main__":
    main()
