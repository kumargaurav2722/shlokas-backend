"""
Hindi audio generator for scripture verse translations.

Scans the database for verses WITH Hindi translation but WITHOUT audio,
generates MP3 audio using edge-tts (Microsoft Edge voices, free),
and stores paths in the audio table.

Usage:
    # Dry run
    python3 -m app.scripts.generate_audio --dry-run

    # Generate for 50 verses
    python3 -m app.scripts.generate_audio --limit 50

    # Generate for specific work
    python3 -m app.scripts.generate_audio --work "Ramayana"

    # Full run
    python3 -m app.scripts.generate_audio
"""

import argparse
import asyncio
import logging
import os
import sys
import time
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from app.database import SessionLocal
from app.models.text import Text
from app.models.translation import Translation
from app.models.audio import Audio
from sqlalchemy import func

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

AUDIO_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "audio", "hindi")
VOICE = "hi-IN-SwaraNeural"  # Indian Hindi female voice (natural, clear)
RATE = "+0%"
BATCH_SIZE = 10


# ---------------------------------------------------------------------------
# TTS
# ---------------------------------------------------------------------------

async def generate_audio_file(text: str, output_path: str) -> bool:
    """Generate MP3 audio from Hindi text using edge-tts."""
    try:
        import edge_tts
        communicate = edge_tts.Communicate(text, VOICE, rate=RATE)
        await communicate.save(output_path)
        # Verify file was created and has content
        if os.path.exists(output_path) and os.path.getsize(output_path) > 100:
            return True
        return False
    except Exception as e:
        logger.warning("TTS failed: %s", e)
        return False


def generate_audio_sync(text: str, output_path: str) -> bool:
    """Synchronous wrapper for edge-tts."""
    return asyncio.run(generate_audio_file(text, output_path))


# ---------------------------------------------------------------------------
# DB operations
# ---------------------------------------------------------------------------

def get_verses_needing_audio(
    db, work: Optional[str] = None, limit: int = 0
) -> List[Dict]:
    """Find verses with Hindi translation but no Hindi audio."""

    # Get all text_ids that have Hindi translations
    hindi_translations = db.query(
        Translation.text_id, Translation.translation
    ).filter(Translation.language == "Hindi").subquery()

    # Get text_ids that already have Hindi audio
    existing_audio = {
        r[0] for r in
        db.query(Audio.text_id).filter(Audio.language == "Hindi").all()
    }

    # Join to get full verse info
    query = db.query(
        Text.id, Text.category, Text.work, Text.sub_work,
        Text.chapter, Text.verse, hindi_translations.c.translation
    ).join(
        hindi_translations, Text.id == hindi_translations.c.text_id
    )

    if work:
        query = query.filter(Text.work == work)

    query = query.order_by(Text.category, Text.work, Text.chapter, Text.verse)
    rows = query.all()

    result = []
    for r in rows:
        if r.id not in existing_audio:
            result.append({
                "id": r.id,
                "category": r.category,
                "work": r.work,
                "sub_work": r.sub_work,
                "chapter": r.chapter,
                "verse": r.verse,
                "hindi_text": r.translation,
            })

    if limit > 0:
        result = result[:limit]

    return result


def save_audio_record(db, text_id: str, audio_path: str) -> bool:
    """Save audio record to database."""
    try:
        record = Audio(
            text_id=text_id,
            language="Hindi",
            audio_path=audio_path,
            voice_type=VOICE,
        )
        db.add(record)
        db.commit()
        return True
    except Exception as e:
        logger.error("DB save failed: %s", e)
        db.rollback()
        return False


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_pipeline(
    dry_run: bool = False,
    work: Optional[str] = None,
    limit: int = 0,
):
    """Run the Hindi audio generation pipeline."""
    os.makedirs(AUDIO_DIR, exist_ok=True)

    db = SessionLocal()

    try:
        logger.info("🔍 Scanning for verses needing Hindi audio...")
        verses = get_verses_needing_audio(db, work, limit)

        if not verses:
            logger.info("✅ All verses with Hindi translations have audio!")
            return

        # Stats
        by_work = {}
        for v in verses:
            key = f"{v['category']}/{v['work']}"
            by_work[key] = by_work.get(key, 0) + 1

        logger.info("📊 Found %d verses needing audio:", len(verses))
        for key, count in sorted(by_work.items()):
            logger.info("   %s: %d", key, count)

        if dry_run:
            logger.info("🔍 DRY RUN — no audio will be generated.")
            return

        total_generated = 0
        total_failed = 0
        start_time = time.time()

        logger.info("🎵 Starting audio generation...")

        for i, v in enumerate(verses):
            # Build filename: work_subwork_ch_v.mp3
            safe_work = v["work"].replace(" ", "_").lower()[:20]
            safe_sub = (v["sub_work"] or "").replace(" ", "_").lower()[:20]
            filename = f"{safe_work}_{safe_sub}_ch{v['chapter']}_v{v['verse']}.mp3"
            filepath = os.path.join(AUDIO_DIR, filename)
            audio_url = f"audio/hindi/{filename}"

            # Skip if file already exists
            if os.path.exists(filepath):
                if save_audio_record(db, v["id"], audio_url):
                    total_generated += 1
                continue

            # Generate
            success = generate_audio_sync(v["hindi_text"], filepath)
            if success:
                if save_audio_record(db, v["id"], audio_url):
                    total_generated += 1
                else:
                    total_failed += 1
            else:
                total_failed += 1

            # Progress
            if (i + 1) % 10 == 0 or i == len(verses) - 1:
                elapsed = time.time() - start_time
                rate = (i + 1) / max(elapsed, 1) * 60
                eta = (len(verses) - i - 1) / max(rate, 0.1)
                logger.info(
                    "🎵 %d/%d | Generated: %d | Failed: %d | %.0f/min | ETA: %.0fm",
                    i + 1, len(verses), total_generated, total_failed, rate, eta
                )

        elapsed_total = time.time() - start_time
        logger.info("\n" + "=" * 60)
        logger.info("✨ AUDIO GENERATION COMPLETE")
        logger.info("   Total processed: %d", len(verses))
        logger.info("   Audio generated: %d", total_generated)
        logger.info("   Failed: %d", total_failed)
        logger.info("   Time: %.1f minutes", elapsed_total / 60)
        logger.info("=" * 60)

    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description="Generate Hindi audio for verses")
    parser.add_argument("--dry-run", action="store_true", help="Scan only")
    parser.add_argument("--work", type=str, help="Only this work")
    parser.add_argument("--limit", type=int, default=0, help="Max verses (0=all)")
    args = parser.parse_args()

    run_pipeline(dry_run=args.dry_run, work=args.work, limit=args.limit)


if __name__ == "__main__":
    main()
