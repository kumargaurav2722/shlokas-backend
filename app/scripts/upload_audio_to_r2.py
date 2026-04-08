"""
Migration script: upload existing local audio files to Cloudflare R2.

Scans the local audio/ directory for MP3 files, uploads them to R2,
and updates the audio_path in the database to the R2 public URL.

Usage:
    # Dry run — show what would be uploaded
    python3 -m app.scripts.upload_audio_to_r2 --dry-run

    # Upload all files
    python3 -m app.scripts.upload_audio_to_r2

    # Upload with limit
    python3 -m app.scripts.upload_audio_to_r2 --limit 10
"""

import argparse
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from app.database import SessionLocal
from app.models.audio import Audio
from app.storage import r2_storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def find_local_audio_files(audio_dir: str):
    """Walk the audio directory and find all MP3 files."""
    files = []
    for root, _, filenames in os.walk(audio_dir):
        for fname in filenames:
            if fname.lower().endswith(".mp3"):
                full_path = os.path.join(root, fname)
                # Build the R2 key from relative path
                rel_path = os.path.relpath(full_path, audio_dir)
                r2_key = f"audio/{rel_path}"
                files.append((full_path, r2_key))
    return files


def run(dry_run: bool = False, limit: int = 0):
    audio_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "audio"
    )

    if not os.path.exists(audio_dir):
        logger.error("Audio directory not found: %s", audio_dir)
        return

    if not r2_storage.is_configured():
        logger.error("R2 credentials not configured. Set R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ACCOUNT_ID")
        return

    files = find_local_audio_files(audio_dir)
    if limit > 0:
        files = files[:limit]

    logger.info("Found %d MP3 files to upload", len(files))

    if dry_run:
        for local_path, r2_key in files:
            size_kb = os.path.getsize(local_path) / 1024
            logger.info("  [DRY RUN] %s → %s (%.1f KB)", local_path, r2_key, size_kb)
        total_mb = sum(os.path.getsize(f[0]) for f in files) / 1024 / 1024
        logger.info("Total size: %.2f MB", total_mb)
        return

    db = SessionLocal()
    uploaded = 0
    skipped = 0
    failed = 0
    start = time.time()

    try:
        for i, (local_path, r2_key) in enumerate(files):
            try:
                # Skip if already on R2
                if r2_storage.file_exists(r2_key):
                    logger.info("  [SKIP] Already on R2: %s", r2_key)
                    skipped += 1
                    continue

                # Upload to R2
                public_url = r2_storage.upload_file(local_path, r2_key)
                uploaded += 1

                # Update database records that point to local path
                # Try matching by filename
                filename = os.path.basename(local_path)
                records = db.query(Audio).filter(
                    Audio.audio_path.contains(filename)
                ).all()
                for record in records:
                    record.audio_path = public_url
                if records:
                    db.commit()
                    logger.info("  [DB] Updated %d records for %s", len(records), filename)

            except Exception as exc:
                logger.warning("  [FAIL] %s: %s", r2_key, exc)
                failed += 1
                db.rollback()

            if (i + 1) % 10 == 0:
                elapsed = time.time() - start
                rate = (i + 1) / max(elapsed, 1) * 60
                logger.info("  Progress: %d/%d | Uploaded: %d | Skipped: %d | Failed: %d | %.0f/min",
                            i + 1, len(files), uploaded, skipped, failed, rate)

    finally:
        db.close()

    elapsed = time.time() - start
    logger.info("")
    logger.info("=" * 50)
    logger.info("Migration complete!")
    logger.info("  Uploaded: %d", uploaded)
    logger.info("  Skipped: %d", skipped)
    logger.info("  Failed: %d", failed)
    logger.info("  Time: %.1fs", elapsed)
    logger.info("=" * 50)


def main():
    parser = argparse.ArgumentParser(description="Upload existing audio files to Cloudflare R2")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be uploaded")
    parser.add_argument("--limit", type=int, default=0, help="Max files to upload (0=all)")
    args = parser.parse_args()
    run(dry_run=args.dry_run, limit=args.limit)


if __name__ == "__main__":
    main()
