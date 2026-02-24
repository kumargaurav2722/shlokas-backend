import argparse
import re
from typing import List, Tuple, Optional

import requests
from bs4 import BeautifulSoup

from app.database import SessionLocal
from app.models.text import Text
from app.utils.text_cleaner import normalize_sanskrit

MARKER_RE = re.compile(r"(\d+)[,\.](\d+)(?:[\,\.](\d+))?")
REF_RE = re.compile(r"(?:[A-Za-z]{1,5}U(?:p)?)_(\d+)(?:[\,\.](\d+))?")
SKIP_TOKENS = (
    "upanisad",
    "upanisad",
    "gretill",
    "gretil",
    "input",
    "version",
    "structure",
    "based on",
    "commentary",
    "translated",
    "project",
    "formerly",
    "license",
)


def _strip_header(lines: List[str]) -> List[str]:
    for idx, line in enumerate(lines):
        if line.lower() in ("text", "## text", "txt"):
            return lines[idx + 1 :]
        if line.strip() == "****":
            return lines[idx + 1 :]
    return lines


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch GRETIL HTML and ingest Sanskrit verses")
    parser.add_argument("--url", required=True, help="GRETIL HTML URL")
    parser.add_argument("--category", default="upanishad")
    parser.add_argument("--work", required=True)
    parser.add_argument("--sub-work", required=True, dest="sub_work")
    parser.add_argument("--source", default="GRETIL")
    parser.add_argument("--default-chapter", type=int, default=1)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--dedupe", action="store_true")
    return parser.parse_args()


def fetch_text(url: str) -> List[str]:
    res = requests.get(url, timeout=60)
    res.raise_for_status()
    res.encoding = res.apparent_encoding or res.encoding
    soup = BeautifulSoup(res.text, "html.parser")
    text = soup.get_text("\n")
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return _strip_header(lines)


def split_verses(lines: List[str], default_chapter: int) -> List[Tuple[int, int, str]]:
    verses: List[Tuple[int, int, str]] = []
    verse_num = 0

    for line in lines:
        if line.startswith("//") and line.endswith("//"):
            continue
        if line.startswith("#"):
            continue
        if line.lower().startswith("content"):
            continue
        if line.lower().startswith("last"):
            continue
        if line.lower().startswith("http"):
            continue
        if line.lower().startswith("license"):
            continue
        has_ref = bool(REF_RE.search(line))
        has_structural = ("||" in line) or ("//" in line) or ("।" in line) or ("॥" in line)
        if not ("|" in line or "।" in line or "॥" in line or has_ref):
            continue
        if has_ref and not has_structural:
            continue
        if any(token in line.lower() for token in SKIP_TOKENS):
            continue

        marker_match = MARKER_RE.search(line)
        ref_match = REF_RE.search(line)
        if marker_match:
            chapter = int(marker_match.group(1))
            verse = int(marker_match.group(3) or marker_match.group(2))
        elif ref_match:
            first = int(ref_match.group(1))
            second = ref_match.group(2)
            if second:
                chapter = first
                verse = int(second)
            else:
                chapter = default_chapter
                verse = first
        else:
            chapter = default_chapter
            verse_num += 1
            verse = verse_num

        cleaned = normalize_sanskrit(line)
        if cleaned:
            verses.append((chapter, verse, cleaned))

    return verses


def main() -> None:
    args = parse_args()

    lines = fetch_text(args.url)
    verses = split_verses(lines, args.default_chapter)

    db = SessionLocal()
    try:
        existing = set()
        if args.dedupe:
            rows = (
                db.query(Text.chapter, Text.verse)
                .filter(Text.work == args.work, Text.sub_work == args.sub_work)
                .all()
            )
            existing = {(r[0], r[1]) for r in rows}

        created = 0
        skipped = 0
        for chapter, verse, text in verses:
            if (chapter, verse) in existing:
                skipped += 1
                continue
            record = Text(
                category=args.category,
                work=args.work,
                sub_work=args.sub_work,
                chapter=chapter,
                verse=verse,
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
