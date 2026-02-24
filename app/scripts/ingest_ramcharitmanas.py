import argparse
import glob
import json
import os
from typing import Dict, Iterable, Set, Tuple

from app.database import SessionLocal
from app.models.text import Text


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Ramcharitmanas verses")
    parser.add_argument(
        "--root",
        required=True,
        help="Path to folder containing Ramcharitmanas JSON files",
    )
    parser.add_argument("--category", default="itihasa")
    parser.add_argument("--work", default="Ramcharitmanas")
    parser.add_argument("--source", default="DharmicData")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--dedupe", action="store_true")
    return parser.parse_args()


def iter_files(root: str) -> Iterable[str]:
    pattern = os.path.join(root, "*.json")
    return sorted(glob.glob(pattern))


def load_items(path: str) -> Iterable[Dict]:
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data if isinstance(data, list) else []


def format_kaand(value: str) -> str:
    if not value:
        return "Ramcharitmanas"
    cleaned = " ".join(str(value).split())
    return cleaned


def existing_keys(db, category: str, work: str) -> Set[Tuple[str, int, int]]:
    rows = (
        db.query(Text.sub_work, Text.chapter, Text.verse)
        .filter(Text.category == category, Text.work == work)
        .all()
    )
    return {(r[0], r[1], r[2]) for r in rows}


def main() -> None:
    args = parse_args()
    db = SessionLocal()
    try:
        existing = existing_keys(db, args.category, args.work) if args.dedupe else set()
        seen = set(existing)
        created = 0
        skipped = 0

        for path in iter_files(args.root):
            verse_counter = 0
            for item in load_items(path):
                kaand = format_kaand(item.get("kaand", ""))
                text = item.get("content")
                if not kaand or not text:
                    skipped += 1
                    continue

                verse_counter += 1
                chapter = 1
                key = (kaand, chapter, verse_counter)
                if key in seen:
                    skipped += 1
                    continue
                seen.add(key)

                record = Text(
                    category=args.category,
                    work=args.work,
                    sub_work=kaand,
                    chapter=chapter,
                    verse=verse_counter,
                    sanskrit=text,
                    source=args.source,
                    content=text,
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
