import argparse
from dataclasses import dataclass
from typing import List

from app.scripts.gretil_ingest import fetch_text, split_verses
from app.database import SessionLocal
from app.models.text import Text


@dataclass
class GretilSource:
    url: str
    category: str
    work: str
    sub_work: str
    source: str = "GRETIL"
    default_chapter: int = 1


UPANISHADS = [
    GretilSource(
        url="https://gretil.sub.uni-goettingen.de/gretil/1_sanskr/1_veda/4_upa/isup___u.htm",
        category="upanishad",
        work="Upanishads",
        sub_work="Isha"
    ),
    GretilSource(
        url="https://gretil.sub.uni-goettingen.de/gretil/1_sanskr/1_veda/4_upa/kathop_u.htm",
        category="upanishad",
        work="Upanishads",
        sub_work="Katha"
    ),
    GretilSource(
        url="https://gretil.sub.uni-goettingen.de/gretil/1_sanskr/1_veda/4_upa/prasup_u.htm",
        category="upanishad",
        work="Upanishads",
        sub_work="Prashna"
    ),
    GretilSource(
        url="https://gretil.sub.uni-goettingen.de/gretil/1_sanskr/1_veda/4_upa/mandup_u.htm",
        category="upanishad",
        work="Upanishads",
        sub_work="Mandukya"
    ),
    GretilSource(
        url="https://gretil.sub.uni-goettingen.de/gretil/1_sanskr/1_veda/4_upa/brup___u.htm",
        category="upanishad",
        work="Upanishads",
        sub_work="Brihadaranyaka"
    ),
    GretilSource(
        url="https://gretil.sub.uni-goettingen.de/gretil/1_sanskr/1_veda/4_upa/chup___u.htm",
        category="upanishad",
        work="Upanishads",
        sub_work="Chandogya"
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch ingest selected GRETIL sources")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--dedupe", action="store_true")
    parser.add_argument("--replace", action="store_true", help="Delete existing rows for each sub_work before ingest")
    return parser.parse_args()


def ingest_source(source: GretilSource, dry_run: bool, dedupe: bool, replace: bool) -> None:
    lines = fetch_text(source.url)
    verses = split_verses(lines, source.default_chapter)

    db = SessionLocal()
    try:
        if replace and not dry_run:
            db.query(Text).filter(
                Text.category == source.category,
                Text.work == source.work,
                Text.sub_work == source.sub_work,
            ).delete()
            db.commit()

        existing = set()
        if dedupe:
            rows = (
                db.query(Text.chapter, Text.verse)
                .filter(
                    Text.category == source.category,
                    Text.work == source.work,
                    Text.sub_work == source.sub_work,
                )
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
                category=source.category,
                work=source.work,
                sub_work=source.sub_work,
                chapter=chapter,
                verse=verse,
                sanskrit=text,
                source=source.source,
                content=text,
            )
            db.add(record)
            created += 1
            if dedupe:
                existing.add((chapter, verse))

        if dry_run:
            db.rollback()
        else:
            db.commit()

        print(f"{source.sub_work}: {created} rows (skipped {skipped})")
    finally:
        db.close()


def main() -> None:
    args = parse_args()
    for source in UPANISHADS:
        ingest_source(source, args.dry_run, args.dedupe, args.replace)


if __name__ == "__main__":
    main()
