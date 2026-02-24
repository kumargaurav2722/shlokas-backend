import argparse
import json
import os
from typing import Any, Dict, Iterable, List, Optional, Tuple

from app.database import SessionLocal
from app.models.text import Text
from app.utils.text_cleaner import normalize_sanskrit

CHAPTER_KEYS = ("chapter", "chapter_number", "chapterNumber", "adhyaya", "mandala", "kanda", "parva")
VERSE_KEYS = ("verse", "verse_number", "verseNumber", "verse_id", "shloka", "shloka_number", "sloka", "mantra", "rik")
TEXT_KEYS = ("sanskrit", "text", "sloka", "shloka", "verse_text", "devanagari")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Heuristic JSON ingester for scripture datasets")
    parser.add_argument("--input", required=True, help="Path to JSON file or folder")
    parser.add_argument("--category", required=True)
    parser.add_argument("--work", required=True)
    parser.add_argument("--sub-work", required=True, dest="sub_work")
    parser.add_argument("--source", required=True)
    parser.add_argument("--chapter-from-filename", action="store_true", help="Infer chapter from filename if missing")
    parser.add_argument("--default-chapter", type=int, default=1)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--dedupe", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    return parser.parse_args()


def _first_key(obj: Dict[str, Any], keys: Tuple[str, ...]) -> Optional[str]:
    for key in keys:
        if key in obj:
            return key
    return None


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except ValueError:
        return None


def _extract_from_dict(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    chapter_key = _first_key(obj, CHAPTER_KEYS)
    verse_key = _first_key(obj, VERSE_KEYS)
    text_key = _first_key(obj, TEXT_KEYS)
    if not text_key:
        return None

    chapter = _coerce_int(obj.get(chapter_key)) if chapter_key else None
    verse = _coerce_int(obj.get(verse_key)) if verse_key else None
    text = obj.get(text_key)
    if text in (None, ""):
        return None

    return {
        "chapter": chapter,
        "verse": verse,
        "sanskrit": normalize_sanskrit(str(text)),
    }


def _walk(node: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(node, dict):
        extracted = _extract_from_dict(node)
        if extracted:
            yield extracted
        for value in node.values():
            yield from _walk(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk(item)


def _iter_files(path: str) -> Iterable[str]:
    if os.path.isfile(path):
        yield path
        return
    for root, _, files in os.walk(path):
        for name in files:
            if name.endswith(".json"):
                yield os.path.join(root, name)


def _infer_chapter_from_filename(path: str) -> Optional[int]:
    name = os.path.basename(path).lower()
    for token in ("chapter", "adhyaya", "mandala", "kanda", "parva"):
        if token in name:
            digits = "".join(ch if ch.isdigit() else " " for ch in name)
            parts = [p for p in digits.split() if p]
            if parts:
                return _coerce_int(parts[0])
    return None


def main() -> None:
    args = parse_args()

    db = SessionLocal()
    try:
        existing = set()
        if args.dedupe:
            rows = (
                db.query(Text.chapter, Text.verse)
                .filter(
                    Text.category == args.category,
                    Text.work == args.work,
                    Text.sub_work == args.sub_work
                )
                .all()
            )
            existing = {(r[0], r[1]) for r in rows}

        created = 0
        skipped = 0
        for path in _iter_files(args.input):
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)

            inferred_chapter = None
            if args.chapter_from_filename:
                inferred_chapter = _infer_chapter_from_filename(path)

            for item in _walk(data):
                chapter = item.get("chapter")
                if chapter is None:
                    chapter = inferred_chapter if inferred_chapter is not None else args.default_chapter
                verse = item.get("verse")
                if chapter is None or verse is None:
                    continue

                key = (chapter, verse)
                if key in existing:
                    skipped += 1
                    continue

                record = Text(
                    category=args.category,
                    work=args.work,
                    sub_work=args.sub_work,
                    chapter=chapter,
                    verse=verse,
                    sanskrit=item.get("sanskrit", ""),
                    source=args.source,
                    content=item.get("sanskrit", ""),
                )
                db.add(record)
                created += 1

                if args.limit and created >= args.limit:
                    break
            if args.limit and created >= args.limit:
                break

        if args.dry_run:
            db.rollback()
        else:
            db.commit()

        print(f"Ingested {created} rows (skipped {skipped})")
    finally:
        db.close()


if __name__ == "__main__":
    main()
