import argparse
import re
from typing import List, Tuple

import requests
from bs4 import BeautifulSoup

from app.database import SessionLocal
from app.models.text import Text
from app.utils.text_cleaner import normalize_sanskrit

REF_RE = re.compile(r"[ŚS]ivP_(\d+)(?:\.\d+)?,(\d+)\.(\d+)")
TRAILING_REF_RE = re.compile(r"\s*//\s*[ŚS]ivP_\d+(?:\.\d+)?,\d+\.\d+[a-z]*\s*/?\s*")
INLINE_REF_RE = re.compile(r"[ŚS]ivP_\d+(?:\.\d+)?,\d+\.\d+[a-z]*")
SKIP_TOKENS = (
    "ins.",
    "subst.",
    "var.",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Shiva Purana from GRETIL HTML")
    parser.add_argument(
        "--url",
        default="https://gretil.sub.uni-goettingen.de/gretil/corpustei/transformations/html/sa_zivapurANabooks-1-and-7.htm",
    )
    parser.add_argument("--category", default="purana")
    parser.add_argument("--work", default="Shiva Purana")
    parser.add_argument("--source", default="GRETIL")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--dedupe", action="store_true")
    parser.add_argument("--replace", action="store_true")
    return parser.parse_args()


def fetch_lines(url: str) -> List[str]:
    res = requests.get(url, timeout=60)
    res.raise_for_status()
    res.encoding = res.apparent_encoding or res.encoding
    soup = BeautifulSoup(res.text, "html.parser")
    text = soup.get_text("\n")
    return [line.strip() for line in text.splitlines() if line.strip()]


def parse_verses(lines: List[str]) -> List[Tuple[str, int, int, str]]:
    verses: List[Tuple[str, int, int, str]] = []
    for line in lines:
        if line.startswith("§"):
            continue
        if line.startswith("[[") or line.startswith("___"):
            continue
        if any(token in line.lower() for token in SKIP_TOKENS):
            continue

        match = REF_RE.search(line)
        if not match:
            continue

        book = int(match.group(1))
        adhyaya = int(match.group(2))
        verse = int(match.group(3))

        cleaned = TRAILING_REF_RE.sub("", line)
        cleaned = INLINE_REF_RE.sub("", cleaned)
        cleaned = normalize_sanskrit(cleaned)
        if not cleaned:
            continue

        sub_work = f"Book {book}"
        verses.append((sub_work, adhyaya, verse, cleaned))
    return verses


def main() -> None:
    args = parse_args()
    lines = fetch_lines(args.url)
    verses = parse_verses(lines)

    db = SessionLocal()
    try:
        if args.replace and not args.dry_run:
            db.query(Text).filter(
                Text.category == args.category,
                Text.work == args.work,
            ).delete()
            db.commit()

        existing = set()
        if args.dedupe:
            rows = (
                db.query(Text.sub_work, Text.chapter, Text.verse)
                .filter(Text.category == args.category, Text.work == args.work)
                .all()
            )
            existing = {(r[0], r[1], r[2]) for r in rows}

        created = 0
        skipped = 0
        for sub_work, chapter, verse, text in verses:
            key = (sub_work, chapter, verse)
            if key in existing:
                skipped += 1
                continue
            record = Text(
                category=args.category,
                work=args.work,
                sub_work=sub_work,
                chapter=chapter,
                verse=verse,
                sanskrit=text,
                source=args.source,
                content=text,
            )
            db.add(record)
            created += 1
            if args.dedupe:
                existing.add(key)

        if args.dry_run:
            db.rollback()
        else:
            db.commit()

        print(f"Ingested {created} rows (skipped {skipped})")
    finally:
        db.close()


if __name__ == "__main__":
    main()
