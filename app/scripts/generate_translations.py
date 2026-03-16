"""
Batch translation generator for scripture verses.

Scans the database for verses missing Hindi/English translations,
generates them using Groq API (free tier), and stores in the
translations table.

Usage:
    # Dry run — show what would be translated
    python3 -m app.scripts.generate_translations --dry-run

    # Translate 50 verses (test batch)
    python3 -m app.scripts.generate_translations --limit 50

    # Translate specific scripture
    python3 -m app.scripts.generate_translations --work "Ramayana"

    # Translate only Hindi
    python3 -m app.scripts.generate_translations --lang hi

    # Full run (all verses, all languages)
    python3 -m app.scripts.generate_translations
"""

import argparse
import hashlib
import json
import logging
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Load .env FIRST so API keys are available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from app.database import SessionLocal
from app.models.text import Text
from app.models.translation import Translation
from sqlalchemy import func, and_, not_

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

# Rate limiting: Groq free tier = 30 req/min, we use 20 to be safe
RATE_LIMIT_RPM = 20
RATE_LIMIT_DELAY = 6.0  # 6 seconds between calls (conservative to avoid 429s)

# Batch size: number of verses per API call
BATCH_SIZE = 5

# Languages to generate
LANG_MAP = {
    "hi": "Hindi",
    "en": "English",
}

# ---------------------------------------------------------------------------
# Translation prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are an expert Sanskrit scholar and translator.
You translate ancient Sanskrit scripture verses with precision and devotion.
Your Hindi translations use natural, conversational Indian Hindi — respectful and easy to understand.
Your English translations are accurate with contextual clarity.
You ALWAYS preserve Sanskrit proper nouns (Krishna, Arjuna, Shiva, Rama, etc.).
You respond ONLY with valid JSON, no extra text."""

def build_translation_prompt(verses: List[Dict]) -> str:
    """Build a batch translation prompt for multiple verses."""
    verse_block = "\n\n".join(
        f"[{i+1}] ID: {v['id']}\n{v['sanskrit']}"
        for i, v in enumerate(verses)
    )

    return f"""Translate these {len(verses)} Sanskrit verses into Hindi and English.

RULES:
- Hindi: natural conversational Indian Hindi, respectful tone, no vulgar slang
- English: accurate literal meaning with contextual clarity
- Preserve Sanskrit proper nouns (Krishna, Arjuna, Shiva, Rama, Brahman, etc.)
- Keep each translation 1-3 sentences, concise
- Return ONLY a valid JSON array, nothing else

OUTPUT FORMAT (JSON array):
[
  {{"id": "<verse_id>", "hi": "<Hindi translation>", "en": "<English translation>"}},
  ...
]

VERSES:
{verse_block}"""


def build_single_lang_prompt(verses: List[Dict], lang_code: str) -> str:
    """Build a prompt for a single language."""
    lang_name = LANG_MAP.get(lang_code, lang_code)
    verse_block = "\n\n".join(
        f"[{i+1}] ID: {v['id']}\n{v['sanskrit']}"
        for i, v in enumerate(verses)
    )

    if lang_code == "hi":
        style = "natural conversational Indian Hindi, respectful tone, no vulgar slang, easy to understand"
    else:
        style = "accurate literal meaning with contextual clarity, scholarly yet accessible"

    return f"""Translate these {len(verses)} Sanskrit verses into {lang_name}.

RULES:
- Style: {style}
- Preserve Sanskrit proper nouns (Krishna, Arjuna, Shiva, Rama, Brahman, etc.)
- Keep each translation 1-3 sentences, concise
- Return ONLY a valid JSON array, nothing else

OUTPUT FORMAT:
[
  {{"id": "<verse_id>", "{lang_code}": "<{lang_name} translation>"}},
  ...
]

VERSES:
{verse_block}"""


# ---------------------------------------------------------------------------
# Groq API
# ---------------------------------------------------------------------------

def call_groq(prompt: str, retries: int = 3) -> Optional[str]:
    """Call Groq API with retry logic."""
    if not GROQ_API_KEY:
        logger.error("GROQ_API_KEY not set!")
        return None

    body = json.dumps({
        "model": GROQ_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
        "max_tokens": 4096,
    }).encode("utf-8")

    req = urllib.request.Request(
        GROQ_URL,
        data=body,
        headers={
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json",
            "User-Agent": "ShlokasApp/1.0",
        },
        method="POST",
    )

    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data["choices"][0]["message"]["content"]
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = min(30, 5 * (attempt + 1))
                logger.warning("Rate limited, waiting %ds...", wait)
                time.sleep(wait)
                continue
            elif e.code == 503:
                logger.warning("Groq overloaded, waiting 10s...")
                time.sleep(10)
                continue
            else:
                logger.error("Groq HTTP %d: %s", e.code, e.read().decode()[:200])
                return None
        except Exception as e:
            logger.warning("Groq attempt %d failed: %s", attempt + 1, e)
            if attempt < retries - 1:
                time.sleep(3)
            continue

    return None


def parse_translations(raw: str) -> List[Dict]:
    """Parse LLM JSON response into list of translation dicts.
    Uses multiple strategies to handle various LLM output formats."""
    import re

    if not raw or not raw.strip():
        return []

    text = raw.strip()

    # Strategy 1: Strip markdown code fences first
    fence_match = re.search(r'```(?:json)?\s*([\s\S]*?)```', text)
    if fence_match:
        text = fence_match.group(1).strip()

    # Strategy 2: Direct JSON parse
    try:
        data = json.loads(text)
        result = _extract_list(data)
        if result:
            return result
    except json.JSONDecodeError:
        pass

    # Strategy 3: Find JSON array in the text
    arr_match = re.search(r'(\[\s*\{[\s\S]*)', text)
    if arr_match:
        json_text = arr_match.group(1)

        # Try as-is
        try:
            data = json.loads(json_text)
            if isinstance(data, list):
                return data
        except json.JSONDecodeError:
            pass

        # Fix truncated JSON: close open brackets
        fixed = json_text.rstrip().rstrip(',')
        # Count brackets
        open_braces = fixed.count('{') - fixed.count('}')
        open_brackets = fixed.count('[') - fixed.count(']')
        fixed += '}' * max(0, open_braces)
        fixed += ']' * max(0, open_brackets)
        try:
            data = json.loads(fixed)
            if isinstance(data, list):
                return data
        except json.JSONDecodeError:
            pass

    # Strategy 4: Extract individual JSON objects via regex
    objects = re.findall(r'\{[^{}]*"id"[^{}]*\}', text)
    if objects:
        results = []
        for obj_str in objects:
            try:
                obj = json.loads(obj_str)
                if isinstance(obj, dict) and 'id' in obj:
                    results.append(obj)
            except json.JSONDecodeError:
                continue
        if results:
            return results

    # Strategy 5: Try line-by-line
    results = []
    for line in text.split('\n'):
        line = line.strip().rstrip(',')
        if line.startswith('{') and '"id"' in line:
            try:
                obj = json.loads(line)
                results.append(obj)
            except json.JSONDecodeError:
                pass
    if results:
        return results

    logger.warning("All parse strategies failed for: %s", text[:150])
    return []


def _extract_list(data) -> List[Dict]:
    """Extract list of dicts from parsed JSON."""
    if isinstance(data, list):
        return [d for d in data if isinstance(d, dict)]
    if isinstance(data, dict):
        for key in ('translations', 'results', 'data', 'verses'):
            if key in data and isinstance(data[key], list):
                return data[key]
        if 'id' in data:
            return [data]
        for v in data.values():
            if isinstance(v, list):
                return [d for d in v if isinstance(d, dict)]
    return []


# ---------------------------------------------------------------------------
# DB operations
# ---------------------------------------------------------------------------

def get_verses_needing_translation(
    db, lang_codes: List[str], work: Optional[str] = None, limit: int = 0
) -> List[Dict]:
    """Find verses missing translations for the specified languages."""

    # Get all text IDs that already have translations for ALL requested languages
    # We want verses that are missing at least one language
    base_query = db.query(
        Text.id, Text.sanskrit, Text.category, Text.work,
        Text.sub_work, Text.chapter, Text.verse
    ).filter(Text.sanskrit.isnot(None), Text.sanskrit != "")

    if work:
        base_query = base_query.filter(Text.work == work)

    all_verses = base_query.order_by(Text.category, Text.work, Text.chapter, Text.verse).all()

    # Get existing translations
    existing = {}
    for lang in lang_codes:
        ids = {
            r[0] for r in
            db.query(Translation.text_id).filter(Translation.language == LANG_MAP.get(lang, lang)).all()
        }
        existing[lang] = ids

    # Find verses missing at least one language
    result = []
    for v in all_verses:
        missing_langs = [lang for lang in lang_codes if v.id not in existing[lang]]
        if missing_langs:
            result.append({
                "id": v.id,
                "sanskrit": v.sanskrit,
                "category": v.category,
                "work": v.work,
                "sub_work": v.sub_work,
                "chapter": v.chapter,
                "verse": v.verse,
                "missing": missing_langs,
            })

    if limit > 0:
        result = result[:limit]

    return result


def save_translations(db, translations: List[Dict], model_name: str) -> Tuple[int, int]:
    """Save translations to the database. Returns (saved, skipped)."""
    saved = 0
    skipped = 0

    for tr in translations:
        text_id = tr.get("id", "")
        if not text_id:
            skipped += 1
            continue

        for lang_code in ["hi", "en"]:
            text = tr.get(lang_code, "").strip()
            if not text:
                continue

            lang_name = LANG_MAP.get(lang_code, lang_code)

            # Check if translation already exists
            exists = db.query(Translation.id).filter(
                Translation.text_id == text_id,
                Translation.language == lang_name
            ).first()

            if exists:
                skipped += 1
                continue

            record = Translation(
                text_id=text_id,
                language=lang_name,
                translation=text,
                generated_by=model_name,
                commentary=None,
            )
            db.add(record)
            saved += 1

    try:
        db.commit()
    except Exception as e:
        logger.error("DB commit failed: %s", e)
        db.rollback()
        return 0, saved + skipped

    return saved, skipped


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_pipeline(
    dry_run: bool = False,
    work: Optional[str] = None,
    limit: int = 0,
    lang_codes: Optional[List[str]] = None,
):
    """Run the batch translation pipeline."""
    if not GROQ_API_KEY and not dry_run:
        logger.error("GROQ_API_KEY not set! Set it in .env or environment.")
        sys.exit(1)

    if lang_codes is None:
        lang_codes = ["hi", "en"]

    db = SessionLocal()
    model_name = f"groq-{GROQ_MODEL}"

    try:
        # Step 1: Scan DB
        logger.info("🔍 Scanning database for missing translations...")
        verses = get_verses_needing_translation(db, lang_codes, work, limit)

        if not verses:
            logger.info("✅ All verses have translations! Nothing to do.")
            return

        # Stats
        by_work = {}
        for v in verses:
            key = f"{v['category']}/{v['work']}"
            by_work[key] = by_work.get(key, 0) + 1

        logger.info("📊 Found %d verses needing translation:", len(verses))
        for key, count in sorted(by_work.items()):
            logger.info("   %s: %d", key, count)

        if dry_run:
            logger.info("🔍 DRY RUN — no translations will be generated.")
            logger.info("   Sample verse: %s Ch%d V%d: %s",
                        verses[0]["work"], verses[0]["chapter"], verses[0]["verse"],
                        verses[0]["sanskrit"][:80])
            return

        # Step 2: Process in batches
        total_saved = 0
        total_skipped = 0
        total_failed = 0
        total_batches = (len(verses) + BATCH_SIZE - 1) // BATCH_SIZE
        start_time = time.time()

        logger.info("🚀 Starting translation (%d batches of %d)...", total_batches, BATCH_SIZE)

        for batch_idx in range(0, len(verses), BATCH_SIZE):
            batch = verses[batch_idx:batch_idx + BATCH_SIZE]
            batch_num = batch_idx // BATCH_SIZE + 1
            elapsed = time.time() - start_time
            rate = batch_num / max(elapsed, 1) * 60
            eta_min = (total_batches - batch_num) / max(rate, 0.1)

            logger.info(
                "📝 Batch %d/%d (%d verses) | Saved: %d | Failed: %d | ETA: %.0fm",
                batch_num, total_batches, len(batch),
                total_saved, total_failed, eta_min
            )

            # Build prompt
            if len(lang_codes) == 1:
                prompt = build_single_lang_prompt(batch, lang_codes[0])
            else:
                prompt = build_translation_prompt(batch)

            # Call Groq
            raw = call_groq(prompt)
            if not raw:
                total_failed += len(batch)
                logger.warning("   ❌ Batch %d failed (no response)", batch_num)
                continue

            # Parse response
            parsed = parse_translations(raw)
            if not parsed:
                total_failed += len(batch)
                logger.warning("   ❌ Batch %d failed (bad JSON)", batch_num)
                continue

            # Save to DB
            saved, skipped = save_translations(db, parsed, model_name)
            total_saved += saved
            total_skipped += skipped

            if saved > 0:
                logger.info("   ✅ Saved %d translations", saved)

            # Rate limiting
            time.sleep(RATE_LIMIT_DELAY)

        # Summary
        elapsed_total = time.time() - start_time
        logger.info("\n" + "=" * 60)
        logger.info("✨ TRANSLATION COMPLETE")
        logger.info("   Total processed: %d verses", len(verses))
        logger.info("   Translations saved: %d", total_saved)
        logger.info("   Skipped (existing): %d", total_skipped)
        logger.info("   Failed: %d", total_failed)
        logger.info("   Time: %.1f minutes", elapsed_total / 60)
        logger.info("=" * 60)

    finally:
        db.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    global GROQ_API_KEY
    GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()

    parser = argparse.ArgumentParser(description="Batch translate scripture verses")
    parser.add_argument("--dry-run", action="store_true", help="Scan only, don't translate")
    parser.add_argument("--work", type=str, help="Translate only this work (e.g., 'Ramayana')")
    parser.add_argument("--limit", type=int, default=0, help="Max verses to process (0 = all)")
    parser.add_argument("--lang", type=str, help="Language code(s), comma-separated (e.g., 'hi,en')")
    args = parser.parse_args()

    lang_codes = None
    if args.lang:
        lang_codes = [l.strip() for l in args.lang.split(",")]

    run_pipeline(
        dry_run=args.dry_run,
        work=args.work,
        limit=args.limit,
        lang_codes=lang_codes,
    )


if __name__ == "__main__":
    main()
