import argparse
import json
import os
import re
from typing import Iterable, List, Tuple

from app.database import SessionLocal
from app.models.text import Text
from app.utils.text_cleaner import normalize_sanskrit

DEVANAGARI_RE = re.compile(r"[\u0900-\u097F]")
VERSE_BLOCK_RE = re.compile(r"(.*?)(?:[॥]{1,2}\s*([0-9०-९]+)\s*[॥]{1,2})", re.DOTALL)
DEVANAGARI_DIGITS = str.maketrans("०१२३४५६७८९", "0123456789")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Veda JSON datasets from DharmicData")
    parser.add_argument("--input", required=True, help="Folder containing JSON files")
    parser.add_argument("--veda", choices=["rigveda", "yajurveda", "atharvaveda"], required=True)
    parser.add_argument("--source", default="DharmicData")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--dedupe", action="store_true")
    return parser.parse_args()


def _iter_json(path: str) -> Iterable[str]:
    for root, _, files in os.walk(path):
        for name in files:
            if name.endswith(".json"):
                yield os.path.join(root, name)


def _to_int(value) -> int:
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip().translate(DEVANAGARI_DIGITS))
    except ValueError:
        return 0


def _clean_block(text: str) -> str:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    filtered = [line for line in lines if DEVANAGARI_RE.search(line)]
    return normalize_sanskrit(" ".join(filtered))


def extract_verses(text: str) -> List[Tuple[int, str]]:
    verses: List[Tuple[int, str]] = []
    for match in VERSE_BLOCK_RE.finditer(text):
        block = _clean_block(match.group(1))
        num_raw = match.group(2)
        if not block:
            continue
        verse_num = _to_int(num_raw)
        verses.append((verse_num, block))
    if not verses:
        cleaned = _clean_block(text)
        if cleaned:
            verses.append((1, cleaned))
    return verses


def ingest_rigveda(path: str, source: str, dry_run: bool, dedupe: bool) -> None:
    db = SessionLocal()
    try:
        existing_map = {}
        for file_path in _iter_json(path):
            with open(file_path, "r", encoding="utf-8") as handle:
                data = json.load(handle)

            for item in data:
                mandala = _to_int(item.get("mandala"))
                sukta = _to_int(item.get("sukta"))
                text = item.get("text", "")

                sub_work = f"Mandala {mandala}"
                if dedupe and sub_work not in existing_map:
                    rows = (
                        db.query(Text.chapter, Text.verse)
                        .filter(
                            Text.category == "veda",
                            Text.work == "Rigveda",
                            Text.sub_work == sub_work,
                        )
                        .all()
                    )
                    existing_map[sub_work] = {(r[0], r[1]) for r in rows}
                existing = existing_map.get(sub_work, set())

                for verse_num, verse_text in extract_verses(text):
                    key = (sukta, verse_num)
                    if key in existing:
                        continue
                    record = Text(
                        category="veda",
                        work="Rigveda",
                        sub_work=sub_work,
                        chapter=sukta,
                        verse=verse_num,
                        sanskrit=verse_text,
                        source=source,
                        content=verse_text,
                    )
                    db.add(record)
                    if dedupe:
                        existing.add(key)

        if dry_run:
            db.rollback()
        else:
            db.commit()
    finally:
        db.close()


def ingest_yajurveda(path: str, source: str, dry_run: bool, dedupe: bool) -> None:
    db = SessionLocal()
    try:
        existing_map = {}
        for file_path in _iter_json(path):
            with open(file_path, "r", encoding="utf-8") as handle:
                data = json.load(handle)

            for item in data:
                adhyaya = _to_int(item.get("adhyaya"))
                text = item.get("text", "")
                samhita = item.get("samhita") or "Yajurveda"
                sub_work = samhita.replace("-", " ").title()

                if dedupe and sub_work not in existing_map:
                    rows = (
                        db.query(Text.chapter, Text.verse)
                        .filter(
                            Text.category == "veda",
                            Text.work == "Yajurveda",
                            Text.sub_work == sub_work,
                        )
                        .all()
                    )
                    existing_map[sub_work] = {(r[0], r[1]) for r in rows}
                existing = existing_map.get(sub_work, set())

                for verse_num, verse_text in extract_verses(text):
                    key = (adhyaya, verse_num)
                    if key in existing:
                        continue
                    record = Text(
                        category="veda",
                        work="Yajurveda",
                        sub_work=sub_work,
                        chapter=adhyaya,
                        verse=verse_num,
                        sanskrit=verse_text,
                        source=source,
                        content=verse_text,
                    )
                    db.add(record)
                    if dedupe:
                        existing.add(key)

        if dry_run:
            db.rollback()
        else:
            db.commit()
    finally:
        db.close()


def ingest_atharvaveda(path: str, source: str, dry_run: bool, dedupe: bool) -> None:
    db = SessionLocal()
    try:
        existing_map = {}
        for file_path in _iter_json(path):
            with open(file_path, "r", encoding="utf-8") as handle:
                data = json.load(handle)

            for item in data:
                kaanda = _to_int(item.get("kaanda"))
                sukta = _to_int(item.get("sukta"))
                text = item.get("text", "")
                sub_work = f"Kaanda {kaanda}"

                if dedupe and sub_work not in existing_map:
                    rows = (
                        db.query(Text.chapter, Text.verse)
                        .filter(
                            Text.category == "veda",
                            Text.work == "Atharvaveda",
                            Text.sub_work == sub_work,
                        )
                        .all()
                    )
                    existing_map[sub_work] = {(r[0], r[1]) for r in rows}
                existing = existing_map.get(sub_work, set())

                for verse_num, verse_text in extract_verses(text):
                    key = (sukta, verse_num)
                    if key in existing:
                        continue
                    record = Text(
                        category="veda",
                        work="Atharvaveda",
                        sub_work=sub_work,
                        chapter=sukta,
                        verse=verse_num,
                        sanskrit=verse_text,
                        source=source,
                        content=verse_text,
                    )
                    db.add(record)
                    if dedupe:
                        existing.add(key)

        if dry_run:
            db.rollback()
        else:
            db.commit()
    finally:
        db.close()


def main() -> None:
    args = parse_args()
    if args.veda == "rigveda":
        ingest_rigveda(args.input, args.source, args.dry_run, args.dedupe)
    elif args.veda == "yajurveda":
        ingest_yajurveda(args.input, args.source, args.dry_run, args.dedupe)
    elif args.veda == "atharvaveda":
        ingest_atharvaveda(args.input, args.source, args.dry_run, args.dedupe)


if __name__ == "__main__":
    main()
