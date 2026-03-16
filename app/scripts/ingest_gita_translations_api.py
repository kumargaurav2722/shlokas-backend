"""Fetch Bhagavad Gita translations from the free Bhagavad Gita API and
insert them into the translations table.

Usage
-----
    python -m app.scripts.ingest_gita_translations_api --dry-run
    python -m app.scripts.ingest_gita_translations_api
    python -m app.scripts.ingest_gita_translations_api --languages hi,en
"""
import argparse
import time
import urllib.request
import json
from typing import List, Dict, Any

from app.database import SessionLocal
from app.models.text import Text
from app.models.translation import Translation


API_BASE = "https://bhagavadgitaapi.in"

LANGUAGE_MAP = {
    "hi": "hindi",
    "en": "english",
}


def fetch_json(url: str) -> Any:
    """Simple GET request using stdlib (no requests dependency needed)."""
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_chapter_verses(chapter: int) -> List[Dict]:
    """Fetch all verses for a given chapter from the public API."""
    url = f"{API_BASE}/slok/{chapter}"
    try:
        data = fetch_json(url)
        if isinstance(data, dict):
            return [data]
        return data if isinstance(data, list) else []
    except Exception:
        # Fall back to fetching individual verses
        verses = []
        for verse_num in range(1, 80):
            try:
                url = f"{API_BASE}/slok/{chapter}/{verse_num}"
                v = fetch_json(url)
                if v:
                    verses.append(v)
            except Exception:
                break
        return verses


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest Gita translations from bhagavadgitaapi.in"
    )
    parser.add_argument(
        "--languages",
        default="hi,en",
        help="Comma-separated language codes (hi, en)",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Delay in seconds between API requests",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    lang_codes = [l.strip() for l in args.languages.split(",") if l.strip()]

    db = SessionLocal()
    try:
        # Get all Gita verses from the texts table
        gita_verses = (
            db.query(Text)
            .filter(
                Text.work == "Mahabharata",
                Text.sub_work == "Bhagavad Gita",
            )
            .order_by(Text.chapter, Text.verse)
            .all()
        )
        if not gita_verses:
            print("No Gita verses found in texts table. Run ingest_gita.py first.")
            return

        # Build lookup: (chapter, verse) -> text_id
        verse_map = {(v.chapter, v.verse): v.id for v in gita_verses}
        print(f"Found {len(verse_map)} Gita verses in DB")

        # Get existing translations
        existing_ids = set()
        existing = (
            db.query(Translation.text_id, Translation.language)
            .filter(Translation.text_id.in_([v.id for v in gita_verses]))
            .all()
        )
        for text_id, lang in existing:
            existing_ids.add((text_id, lang))

        created = 0
        skipped = 0
        errors = 0

        chapters = sorted(set(v.chapter for v in gita_verses))
        for chapter in chapters:
            print(f"Fetching chapter {chapter}...")
            verses_data = fetch_chapter_verses(chapter)
            if not verses_data:
                # Try verse-by-verse
                chapter_verses = [v for v in gita_verses if v.chapter == chapter]
                for cv in chapter_verses:
                    try:
                        url = f"{API_BASE}/slok/{chapter}/{cv.verse}"
                        vdata = fetch_json(url)
                        if vdata:
                            verses_data.append(vdata)
                        time.sleep(args.delay)
                    except Exception as e:
                        print(f"  Error fetching {chapter}:{cv.verse}: {e}")
                        errors += 1

            for vdata in verses_data:
                verse_num = vdata.get("verse") or vdata.get("slok_number", 0)
                if isinstance(verse_num, str):
                    try:
                        verse_num = int(verse_num.split(".")[-1])
                    except (ValueError, IndexError):
                        continue

                text_id = verse_map.get((chapter, verse_num))
                if not text_id:
                    continue

                for lang_code in lang_codes:
                    lang_full = LANGUAGE_MAP.get(lang_code, lang_code)
                    if (text_id, lang_code) in existing_ids:
                        skipped += 1
                        continue

                    # Try to extract translation from API response
                    translation_text = None

                    # The API returns translations under various keys
                    if lang_full in vdata:
                        t = vdata[lang_full]
                        if isinstance(t, dict):
                            translation_text = t.get("ht") or t.get("et") or t.get("hc")
                        elif isinstance(t, str):
                            translation_text = t
                    elif lang_code in vdata:
                        t = vdata[lang_code]
                        if isinstance(t, dict):
                            translation_text = t.get("ht") or t.get("et")
                        elif isinstance(t, str):
                            translation_text = t

                    # Try transliteration or tej sections 
                    if not translation_text:
                        tej = vdata.get("tej", {})
                        if isinstance(tej, dict):
                            translation_text = tej.get("ht") or tej.get("et")
                        rams = vdata.get("rams", {})
                        if isinstance(rams, dict) and not translation_text:
                            translation_text = rams.get("ht") or rams.get("et")
                        if not translation_text:
                            spiw = vdata.get("spiw", {})
                            if isinstance(spiw, dict):
                                translation_text = spiw.get("et") or spiw.get("ht")

                    if not translation_text:
                        continue

                    record = Translation(
                        text_id=text_id,
                        language=lang_code,
                        translation=translation_text.strip(),
                        commentary=None,
                        generated_by="bhagavadgitaapi.in",
                    )
                    db.add(record)
                    existing_ids.add((text_id, lang_code))
                    created += 1

            time.sleep(args.delay)

        if args.dry_run:
            db.rollback()
            print(f"[DRY RUN] Would create {created}, skipped {skipped}, errors {errors}")
        else:
            db.commit()
            print(f"Created {created} translations, skipped {skipped}, errors {errors}")

    finally:
        db.close()


if __name__ == "__main__":
    main()
