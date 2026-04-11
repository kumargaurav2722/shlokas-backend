import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://app_user:strongpassword@localhost:5432/shlokas_db"
)

# Neon / Render use postgres:// but SQLAlchemy requires postgresql://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Detect if this is a remote DB (Neon, Render Postgres, etc.)
_is_remote = "@" in DATABASE_URL and "localhost" not in DATABASE_URL and "127.0.0.1" not in DATABASE_URL

logger.info("Database URL host: %s (remote=%s)", DATABASE_URL.split("@")[-1].split("/")[0] if "@" in DATABASE_URL else "localhost", _is_remote)

# Build connect_args — Neon requires sslmode=require
_connect_args = {"connect_timeout": 10}
if _is_remote:
    _connect_args["sslmode"] = "require"

engine = create_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    pool_size=15,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=1800,
    connect_args=_connect_args,
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

Base = declarative_base()

