"""
Cloudflare R2 storage client (S3-compatible).

Upload, download, and delete audio files from a Cloudflare R2 bucket.
Works with boto3's S3 API since R2 is S3-compatible.

Required env vars:
    R2_ACCESS_KEY_ID      — API token access key
    R2_SECRET_ACCESS_KEY  — API token secret key
    R2_ACCOUNT_ID         — Cloudflare account ID
    R2_BUCKET_NAME        — Bucket name (default: shlokas-audio)
    R2_PUBLIC_URL         — Public bucket URL (e.g. https://pub-xxx.r2.dev)
"""

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config from environment
# ---------------------------------------------------------------------------

R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "shlokas-audio")
R2_PUBLIC_URL = os.getenv("R2_PUBLIC_URL", "")  # e.g. https://pub-xxx.r2.dev

_client = None


def is_configured() -> bool:
    """Check if R2 credentials are available."""
    return bool(R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY and R2_ACCOUNT_ID)


def _get_client():
    """Lazy-init the S3 client for R2."""
    global _client
    if _client is not None:
        return _client
    if not is_configured():
        raise RuntimeError("Cloudflare R2 credentials not configured. Set R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ACCOUNT_ID env vars.")
    import boto3
    _client = boto3.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto",
    )
    return _client


def get_public_url(r2_key: str) -> str:
    """Get the public CDN URL for an R2 object."""
    base = R2_PUBLIC_URL.rstrip("/") if R2_PUBLIC_URL else f"https://{R2_BUCKET_NAME}.{R2_ACCOUNT_ID}.r2.dev"
    return f"{base}/{r2_key}"


def upload_file(local_path: str, r2_key: str, content_type: str = "audio/mpeg") -> str:
    """
    Upload a local file to R2.

    Args:
        local_path: Path to the local file.
        r2_key: Object key in R2 (e.g. 'hindi/mahabharata_ch1_v1.mp3').
        content_type: MIME type.

    Returns:
        Public URL of the uploaded file.
    """
    client = _get_client()
    client.upload_file(
        Filename=local_path,
        Bucket=R2_BUCKET_NAME,
        Key=r2_key,
        ExtraArgs={
            "ContentType": content_type,
            "CacheControl": "public, max-age=31536000, immutable",
        },
    )
    url = get_public_url(r2_key)
    logger.info("Uploaded %s → %s", r2_key, url)
    return url


def upload_bytes(data: bytes, r2_key: str, content_type: str = "audio/mpeg") -> str:
    """Upload raw bytes to R2."""
    client = _get_client()
    client.put_object(
        Bucket=R2_BUCKET_NAME,
        Key=r2_key,
        Body=data,
        ContentType=content_type,
        CacheControl="public, max-age=31536000, immutable",
    )
    url = get_public_url(r2_key)
    logger.info("Uploaded %s (%d bytes) → %s", r2_key, len(data), url)
    return url


def delete_file(r2_key: str) -> bool:
    """Delete a file from R2."""
    try:
        client = _get_client()
        client.delete_object(Bucket=R2_BUCKET_NAME, Key=r2_key)
        logger.info("Deleted %s from R2", r2_key)
        return True
    except Exception as exc:
        logger.warning("Failed to delete %s: %s", r2_key, exc)
        return False


def file_exists(r2_key: str) -> bool:
    """Check if a file exists in R2."""
    try:
        client = _get_client()
        client.head_object(Bucket=R2_BUCKET_NAME, Key=r2_key)
        return True
    except Exception:
        return False
