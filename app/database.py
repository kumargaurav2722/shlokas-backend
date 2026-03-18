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

logger.info("Database URL host: %s", DATABASE_URL.split("@")[-1].split("/")[0] if "@" in DATABASE_URL else "localhost")

engine = create_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    pool_timeout=10,
    connect_args={"connect_timeout": 10},
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

Base = declarative_base()

