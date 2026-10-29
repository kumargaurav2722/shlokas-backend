"""
Comprehensive Audio Upload Script
==================================
Uploads ALL local audio files to Cloudflare R2 and updates Neon DB records.

Handles two layouts:
  1. audio/hindi/*.mp3      -- bulk edge-tts generated files
  2. audio/<uuid>/*.m4a     -- per-verse macOS say-generated files

Usage:
    # Dry run (shows what would be uploaded)
    python3 -m app.scripts.upload_all_audio_to_r2 --dry-run

    # Full upload (skips R2 existence check - faster for fresh uploads)
    python3 -m app.scripts.upload_all_audio_to_r2

    # Full upload with existence check (safe re-run)
    python3 -m app.scripts.upload_all_audio_to_r2 --check-exists

    # Upload limit for testing
    python3 -m app.scripts.upload_all_audio_to_r2 --limit 10
"""

import argparse
import logging
import os
import sys
import time
import mimetypes
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from app.database import SessionLocal, Base, engine

# Import ALL models so SQLAlchemy resolves FK relationships correctly
from app.models import (  # noqa: F401
    user, text, translation, audio as audio_model,
    embedding, chalisa, puja_vidhi, bookmark,
    history, topic, text_stats, analytics,
)
from app.models.audio import Audio
from app.storage import r2_storage

# Ensure tables exist (idempotent)
Base.metadata.create_all(bind=engine)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

AUDIO_BASE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "audio"
)

SUPPORTED_EXTENSIONS = {".mp3", ".m4a", ".aiff", ".wav", ".ogg"}


def collect_audio_files():
    """
    Walk audio/ directory and collect all audio files with their planned R2 key.
    Returns list of (local_path, r2_key, local_rel_path) tuples.
    """
    files = []
    for root, dirs, filenames in os.walk(AUDIO_BASE_DIR):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for fname in filenames:
            ext = os.path.splitext(fname)[1].lower()
            if ext in SUPPORTED_EXTENSIONS:
                full_path = os.path.join(root, fname)
                # Build relative path from project root (so it starts with "audio/")
                rel_path = os.path.relpath(full_path, os.path.dirname(AUDIO_BASE_DIR))
                r2_key = rel_path.replace("\\", "/")
                files.append((full_path, r2_key, rel_path))
    return files


def update_db_records(db, local_rel_path, r2_url):
    """Update DB audio records that reference this local path. Returns count updated."""
    updated = 0

    # Try exact match first
    records = db.query(Audio).filter(Audio.audio_path == local_rel_path).all()

    # Also try matching by filename suffix
    if not records:
        records = db.query(Audio).filter(
            Audio.audio_path.endswith(os.path.basename(local_rel_path))
        ).all()

    for record in records:
        if not record.audio_path.startswith("http"):
            record.audio_path = r2_url
            updated += 1

    if updated:
        db.commit()

    return updated


def upload_one(args):
    """Upload a single file. Returns (rel_path, r2_url, success, error_msg)."""
    local_path, r2_key, rel_path = args
    try:
        content_type, _ = mimetypes.guess_type(local_path)
        content_type = content_type or "audio/mpeg"
        public_url = r2_storage.upload_file(local_path, r2_key, content_type=content_type)
        return (rel_path, public_url, True, None)
    except Exception as exc:
        return (rel_path, None, False, str(exc))


def run(dry_run=False, limit=0, check_exists=False, workers=4):
    if not r2_storage.is_configured():
        logger.error("R2 is not configured! Check R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ACCOUNT_ID or R2_S3_ENDPOINT.")
        return

    logger.info("Collecting local audio files from: %s", AUDIO_BASE_DIR)
    files = collect_audio_files()
    logger.info("Found %d audio files total", len(files))

    if limit > 0:
        files = files[:limit]
        logger.info("Limited to %d files", limit)

    if dry_run:
        total_mb = sum(os.path.getsize(f[0]) for f in files) / 1024 / 1024
        logger.info("=== DRY RUN -- no uploads will happen ===")
        for local_path, r2_key, rel_path in files[:20]:
            size_kb = os.path.getsize(local_path) / 1024
            public_url = r2_storage.get_public_url(r2_key)
            logger.info("  %s -> %s (%.1f KB)", rel_path, public_url, size_kb)
        if len(files) > 20:
            logger.info("  ... and %d more files", len(files) - 20)
        logger.info("Total estimated upload size: %.2f MB", total_mb)
        return

    # Filter: skip files already in DB with an R2 URL
    if check_exists:
        logger.info("Checking R2 existence for each file (slow but safe)...")
        files = [(lp, rk, rr) for lp, rk, rr in files if not r2_storage.file_exists(rk)]
        logger.info("%d files need uploading (after R2 check)", len(files))

    db = SessionLocal()
    uploaded = 0
    skipped = 0
    failed = 0
    db_updated = 0
    start = time.time()

    logger.info("Starting upload with %d workers...", workers)

    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(upload_one, f): f for f in files}
            completed = 0
            for future in as_completed(futures):
                completed += 1
                rel_path, r2_url, success, error_msg = future.result()

                if success:
                    uploaded += 1
                    # Update DB
                    try:
                        n = update_db_records(db, rel_path, r2_url)
                        db_updated += n
                    except Exception as db_err:
                        logger.warning("DB update failed for %s: %s", rel_path, db_err)
                        try:
                            db.rollback()
                        except Exception:
                            pass
                else:
                    logger.warning("  [FAIL] %s: %s", rel_path, error_msg)
                    failed += 1

                # Progress every 50 files
                if completed % 50 == 0 or completed == len(files):
                    elapsed = time.time() - start
                    rate = completed / max(elapsed, 1) * 60
                    remaining = len(files) - completed
                    eta_min = remaining / max(rate, 0.1)
                    logger.info(
                        "Progress: %d/%d | Uploaded: %d | Failed: %d | DB: %d | %.0f/min | ETA: %.0fm",
                        completed, len(files), uploaded, failed, db_updated, rate, eta_min
                    )

    finally:
        db.close()

    elapsed = time.time() - start
    logger.info("")
    logger.info("=" * 60)
    logger.info("UPLOAD COMPLETE")
    logger.info("  Uploaded:    %d files", uploaded)
    logger.info("  Failed:      %d", failed)
    logger.info("  DB updated:  %d records", db_updated)
    logger.info("  Time:        %.1f minutes", elapsed / 60)
    logger.info("=" * 60)
    logger.info("Public URL base: %s", r2_storage.R2_PUBLIC_URL)


def main():
    parser = argparse.ArgumentParser(description="Upload all local audio to Cloudflare R2 and update DB")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be uploaded (no actual upload)")
    parser.add_argument("--limit", type=int, default=0, help="Max files to upload (0=all)")
    parser.add_argument("--check-exists", action="store_true", help="Check R2 before uploading (slower but safe for re-runs)")
    parser.add_argument("--workers", type=int, default=4, help="Parallel upload workers (default: 4)")
    args = parser.parse_args()
    run(dry_run=args.dry_run, limit=args.limit, check_exists=args.check_exists, workers=args.workers)


if __name__ == "__main__":
    main()
