"""
Comprehensive seed script — populates the entire texts table from all
local data files in data/dharmicdata/.

Usage:
    python3 -m app.scripts.seed_all                  # full ingest
    python3 -m app.scripts.seed_all --dry-run         # preview only
    python3 -m app.scripts.seed_all --only gita       # single scripture

Supports: Bhagavad Gita, Rigveda, Yajurveda, Atharvaveda,
          Valmiki Ramayana, Ramcharitmanas
"""

import json
import os
import re
import sys
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.database import SessionLocal
from app.models.text import Text

DATA_ROOT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
    "dharmicdata",
)

# ---------------------------------------------------------------------------
# Heuristic JSON parsing (based on ingest_from_json.py logic)
# ---------------------------------------------------------------------------

CHAPTER_KEYS = ("chapter", "chapter_number", "chapterNumber", "adhyaya", "mandala", "kanda", "parva")
VERSE_KEYS = ("verse", "verse_number", "verseNumber", "verse_id", "shloka", "shloka_number", "sloka", "mantra", "rik")
TEXT_KEYS = ("sanskrit", "text", "sloka", "shloka", "verse_text", "devanagari")


def _first_key(obj: Dict, keys: Tuple[str, ...]) -> Optional[str]:
    for k in keys:
        if k in obj:
            return k
    return None


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        return None


def _extract(obj: Dict) -> Optional[Dict]:
    text_key = _first_key(obj, TEXT_KEYS)
    if not text_key:
        return None
    text = obj.get(text_key)
    if not text or not str(text).strip():
        return None

    ck = _first_key(obj, CHAPTER_KEYS)
    vk = _first_key(obj, VERSE_KEYS)
    return {
        "chapter": _coerce_int(obj.get(ck)) if ck else None,
        "verse": _coerce_int(obj.get(vk)) if vk else None,
        "sanskrit": str(text).strip(),
    }


def _walk(node: Any) -> Iterable[Dict]:
    if isinstance(node, dict):
        extracted = _extract(node)
        if extracted:
            yield extracted
        for v in node.values():
            yield from _walk(v)
    elif isinstance(node, list):
        for item in node:
            yield from _walk(item)


def _infer_chapter(filename: str) -> Optional[int]:
    digits = re.findall(r'\d+', filename)
    if digits:
        return int(digits[-1])
    return None


def _load_json_dir(dirpath: str, default_chapter: int = 1) -> List[Dict]:
    """Load all JSON files in a directory and extract verse data."""
    results = []
    if not os.path.exists(dirpath):
        return results

    for root, _, files in os.walk(dirpath):
        for fname in sorted(files):
            if not fname.endswith(".json"):
                continue
            filepath = os.path.join(root, fname)
            chapter_from_file = _infer_chapter(fname)

            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                print(f"  ⚠ Skipping {fname}: {e}")
                continue

            verse_counter = 0
            for item in _walk(data):
                chapter = item.get("chapter")
                if chapter is None:
                    chapter = chapter_from_file or default_chapter
                verse = item.get("verse")
                if verse is None:
                    verse_counter += 1
                    verse = verse_counter
                results.append({
                    "chapter": chapter,
                    "verse": verse,
                    "sanskrit": item["sanskrit"],
                })

    return results


# ---------------------------------------------------------------------------
# Scripture definitions
# ---------------------------------------------------------------------------

SCRIPTURES = [
    {
        "name": "gita",
        "category": "itihasa",
        "work": "Mahabharata",
        "sub_work": "Bhagavad Gita",
        "source": "DharmicData",
        "data_path": os.path.join(DATA_ROOT, "gita", "SrimadBhagvadGita"),
    },
    {
        "name": "rigveda",
        "category": "veda",
        "work": "Rigveda",
        "sub_work": "Mandala {chapter}",
        "source": "DharmicData",
        "data_path": os.path.join(DATA_ROOT, "rigveda", "Rigveda"),
        "per_file_sub_work": True,
    },
    {
        "name": "atharvaveda",
        "category": "veda",
        "work": "Atharvaveda",
        "sub_work": "Kanda {chapter}",
        "source": "DharmicData",
        "data_path": os.path.join(DATA_ROOT, "atharvaveda", "AtharvaVeda"),
        "per_file_sub_work": True,
    },
    {
        "name": "yajurveda",
        "category": "veda",
        "work": "Yajurveda",
        "sub_work": "Adhyaya {chapter}",
        "source": "DharmicData",
        "data_path": os.path.join(DATA_ROOT, "yajurveda"),
        "per_file_sub_work": True,
    },
    {
        "name": "ramayana",
        "category": "itihasa",
        "work": "Ramayana",
        "sub_work": "Kanda {chapter}",
        "source": "DharmicData",
        "data_path": os.path.join(DATA_ROOT, "ValmikiRamayana"),
        "per_file_sub_work": True,
    },
    {
        "name": "ramcharitmanas",
        "category": "itihasa",
        "work": "Ramcharitmanas",
        "sub_work": "Part {chapter}",
        "source": "DharmicData",
        "data_path": os.path.join(DATA_ROOT, "Ramcharitmanas"),
        "per_file_sub_work": True,
    },
]


def _existing_keys(db, category: str, work: str) -> Set[Tuple[str, int, int]]:
    rows = (
        db.query(Text.sub_work, Text.chapter, Text.verse)
        .filter(Text.category == category, Text.work == work)
        .all()
    )
    return {(r[0], r[1], r[2]) for r in rows}


def seed_scripture(scripture: Dict, db, dry_run: bool = False) -> int:
    name = scripture["name"]
    data_path = scripture["data_path"]
    per_file = scripture.get("per_file_sub_work", False)

    if not os.path.exists(data_path):
        print(f"  ⚠ Data path not found: {data_path}")
        return 0

    existing = _existing_keys(db, scripture["category"], scripture["work"])
    created = 0

    if per_file:
        # Collect all JSON files (flat or nested one level)
        json_files = []
        for item in sorted(os.listdir(data_path)):
            item_path = os.path.join(data_path, item)
            if os.path.isdir(item_path):
                for f in sorted(os.listdir(item_path)):
                    if f.endswith(".json"):
                        json_files.append(os.path.join(item, f))
            elif item.endswith(".json"):
                json_files.append(item)

        for fname in json_files:
            filepath = os.path.join(data_path, fname)
            if not os.path.isfile(filepath):
                continue

            chapter_num = _infer_chapter(fname) or 1
            sub_work = scripture["sub_work"].format(chapter=chapter_num)

            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                print(f"  ⚠ Skipping {fname}: {e}")
                continue

            verse_counter = 0
            for item in _walk(data):
                chapter = item.get("chapter") or chapter_num
                verse = item.get("verse")
                if verse is None:
                    verse_counter += 1
                    verse = verse_counter

                key = (sub_work, chapter, verse)
                if key in existing:
                    continue

                record = Text(
                    category=scripture["category"],
                    work=scripture["work"],
                    sub_work=sub_work,
                    chapter=chapter,
                    verse=verse,
                    sanskrit=item["sanskrit"],
                    source=scripture["source"],
                    content=item["sanskrit"],
                )
                db.add(record)
                existing.add(key)
                created += 1
    else:
        # All files belong to one sub_work
        rows = _load_json_dir(data_path)
        sub_work = scripture["sub_work"]

        for row in rows:
            chapter = row["chapter"] or 1
            verse = row["verse"] or 1
            key = (sub_work, chapter, verse)
            if key in existing:
                continue

            record = Text(
                category=scripture["category"],
                work=scripture["work"],
                sub_work=sub_work,
                chapter=chapter,
                verse=verse,
                sanskrit=row["sanskrit"],
                source=scripture["source"],
                content=row["sanskrit"],
            )
            db.add(record)
            existing.add(key)
            created += 1

    if dry_run:
        db.rollback()
    else:
        db.commit()

    return created


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Seed all scriptures into the database")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing to DB")
    parser.add_argument("--only", type=str, help="Only seed a specific scripture (e.g. gita, rigveda)")
    args = parser.parse_args()

    db = SessionLocal()
    total = 0

    try:
        for scripture in SCRIPTURES:
            if args.only and scripture["name"] != args.only:
                continue

            print(f"\n📖 Seeding {scripture['name']}...")
            count = seed_scripture(scripture, db, dry_run=args.dry_run)
            total += count
            print(f"   ✅ {count} verses {'(dry run)' if args.dry_run else 'created'}")

        print(f"\n{'🔍 DRY RUN' if args.dry_run else '✨ DONE'}: {total} total verses")
    finally:
        db.close()


if __name__ == "__main__":
    main()
