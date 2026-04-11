import argparse
import glob
import json
import os
from typing import Dict, Iterable, Optional

from app.database import SessionLocal
from app.models.text import Text
from app.models.translation import Translation


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest Bhagavad Gita translations from DharmicData JSON files"
    )
    parser.add_argument(
        "--root",
        required=True,
        help="Path to folder containing bhagavad_gita_chapter_*.json files",
    )
    parser.add_argument("--language", default="Hindi")
    parser.add_argument(
        "--translation-key",
        default="swami ramsukhdas",
        help="Key inside the translations dict to use",
    )
    parser.add_argument("--source", default="DharmicData")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def iter_files(root: str) -> Iterable[str]:
    pattern = os.path.join(root, "bhagavad_gita_chapter_*.json")
    return sorted(glob.glob(pattern))


def load_items(path: str) -> Iterable[Dict]:
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data.get("BhagavadGitaChapter", [])


def extract_translation(item: Dict, key: str) -> Optional[str]:
    translations = item.get("translations") or {}
    value = translations.get(key)
    if not value:
        return None
    return value.strip()


def main() -> None:
    args = parse_args()
    db = SessionLocal()
    try:
        created = 0
        skipped = 0
        missing = 0

        # Pre-fetch all text mappings for Gita
        gita_texts = (
            db.query(Text.id, Text.chapter, Text.verse)
            .filter(
                Text.category == "itihasa",
                Text.work == "Mahabharata",
                Text.sub_work == "Bhagavad Gita",
            )
            .all()
        )
        verse_map = {(t.chapter, t.verse): t.id for t in gita_texts}

        # Pre-fetch existing translations
        existing_translations = set(
            r[0] for r in
            db.query(Translation.text_id)
            .filter(Translation.language == args.language)
            .all()
        )

        for path in iter_files(args.root):
            for item in load_items(path):
                chapter = item.get("chapter")
                verse = item.get("verse")
                if chapter is None or verse is None:
                    skipped += 1
                    continue

                text_id = verse_map.get((chapter, verse))
                if not text_id:
                    missing += 1
                    continue

                if text_id in existing_translations:
                    skipped += 1
                    continue

                translation_text = extract_translation(item, args.translation_key)
                if not translation_text:
                    skipped += 1
                    continue

                record = Translation(
                    text_id=text_id,
                    language=args.language,
                    translation=translation_text,
                    commentary=None,
                    generated_by=args.source,
                )
                db.add(record)
                existing_translations.add(text_id)
                created += 1

                # Commit in batches to avoid huge transactions
                if created % 200 == 0:
                    db.commit()

        if args.dry_run:
            db.rollback()
        else:
            db.commit()

        print(
            f"Created {created} translations "
            f"(skipped {skipped}, missing_texts {missing})"
        )
    finally:
        db.close()


if __name__ == "__main__":
    main()
