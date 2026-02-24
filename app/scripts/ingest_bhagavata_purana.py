import argparse
import re
from typing import Dict, List, Tuple

import requests
from bs4 import BeautifulSoup

from app.database import SessionLocal
from app.models.text import Text
from app.utils.text_cleaner import normalize_sanskrit

LINK_RE = re.compile(r"SB-Sanskrit/SB-Sanskrit[0-9ab-]+\\.html", re.IGNORECASE)
VERSE_LINE_RE = re.compile(r"^(\d{2})(\d{2})(\d{2})(\d{2})\s+(.*)$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Bhagavata Purana Sanskrit from bhagavata.org")
    parser.add_argument(
        "--index-url",
        default="https://www.bhagavata.org/downloads/SB-Sanskrit.html",
    )
    parser.add_argument("--category", default="purana")
    parser.add_argument("--work", default="Bhagavata Purana")
    parser.add_argument("--source", default="bhagavata.org")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--dedupe", action="store_true")
    parser.add_argument("--replace", action="store_true")
    return parser.parse_args()


def fetch_links(index_url: str) -> List[str]:
    res = requests.get(index_url, timeout=60)
    res.raise_for_status()
    res.encoding = res.apparent_encoding or res.encoding
    soup = BeautifulSoup(res.text, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "SB-Sanskrit/SB-Sanskrit" in href or LINK_RE.search(href):
            links.append(requests.compat.urljoin(index_url, href))
    return sorted(set(links))


def fetch_lines(url: str) -> List[str]:
    res = requests.get(url, timeout=60)
    res.raise_for_status()
    res.encoding = res.apparent_encoding or res.encoding
    soup = BeautifulSoup(res.text, "html.parser")
    text = soup.get_text("\n")
    return [line.strip() for line in text.splitlines() if line.strip()]


def parse_lines(lines: List[str]) -> List[Tuple[str, int, int, str]]:
    grouped: Dict[Tuple[int, int, int], List[str]] = {}
    for line in lines:
        match = VERSE_LINE_RE.match(line)
        if not match:
            continue

        canto = int(match.group(1))
        chapter = int(match.group(2))
        verse = int(match.group(4))
        text = normalize_sanskrit(match.group(5))
        if not text:
            continue
        key = (canto, chapter, verse)
        grouped.setdefault(key, []).append(text)

    verses = []
    for (canto, chapter, verse), parts in grouped.items():
        sub_work = f"Canto {canto}"
        verses.append((sub_work, chapter, verse, " ".join(parts)))

    return verses


def main() -> None:
    args = parse_args()
    links = fetch_links(args.index_url)
    if not links:
        print("No canto pages found.")
        return

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
        for url in links:
            lines = fetch_lines(url)
            verses = parse_lines(lines)
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
