from dotenv import load_dotenv
# Load environment variables FIRST, before any app modules read them
load_dotenv()

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.gzip import GZipMiddleware
import time
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from app.database import Base, engine

# ✅ IMPORT ALL MODELS (CRITICAL)
from app.models import (
    user,
    text,
    translation,
    audio,
    embedding,
    chalisa,
    puja_vidhi,
    bookmark,
    history,
    topic,
    text_stats,
    analytics,
)

# ✅ IMPORT ROUTES
from app.routes import (
    auth,
    texts,
    translation as translation_route,
    audio as audio_route,
    chat,
    chalisas,
    puja_vidhi as puja_vidhi_route,
    devotion,
    search as search_route,
    users,
    bookmarks,
    history,
    recommendations,
    topics,
    verse_of_day,
    share,
    stats,
    analytics as analytics_route,
)

# Create tables if they don't exist (safe now — database.py has connect_timeout=10)
import logging as _logging
_logger = _logging.getLogger(__name__)
try:
    Base.metadata.create_all(bind=engine)
    _logger.info("Database tables verified/created.")
except Exception as _table_err:
    _logger.warning("Table creation failed (will retry on first request): %s", _table_err)

app = FastAPI(
    title="Shlokas Platform API",
    version="1.0"
)

# Simple in-memory rate limiting
_rate_limits = {}
_rate_window = 60
_rate_max = 200


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    if request.method == "OPTIONS":
        return await call_next(request)
    client_ip = request.client.host if request.client else "unknown"
    now = time.time()
    bucket = _rate_limits.get(client_ip)
    if not bucket or now - bucket["start"] > _rate_window:
        _rate_limits[client_ip] = {"start": now, "count": 1}
    else:
        if bucket["count"] >= _rate_max:
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded. Try again later."},
            )
        bucket["count"] += 1
    return await call_next(request)

# GZip compression for text-heavy responses
app.add_middleware(GZipMiddleware, minimum_size=500)

# CORS — must be the LAST add_middleware call so it becomes the
# outermost wrapper and handles ALL responses (including errors).
import os as _os
_frontend_url = _os.environ.get("FRONTEND_URL", "http://localhost:5173")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# Ensure CORS headers are present even on unhandled 500 errors
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    _logger.error("Unhandled error on %s %s: %s", request.method, request.url.path, exc)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
            "Access-Control-Allow-Headers": "*",
        },
    )

# 🔌 ROUTES
app.include_router(auth.router)
app.include_router(texts.router)
app.include_router(translation_route.router)
app.include_router(audio_route.router)
app.include_router(chat.router)   # 👈 PHASE 5
app.include_router(chalisas.router)
app.include_router(puja_vidhi_route.router)
app.include_router(devotion.router)
app.include_router(search_route.router)
app.include_router(users.router)
app.include_router(bookmarks.router)
app.include_router(history.router)
app.include_router(recommendations.router)
app.include_router(topics.router)
app.include_router(verse_of_day.router)
app.include_router(share.router)
app.include_router(stats.router)
app.include_router(analytics_route.router)

# 🔊 STATIC FILE SERVING
import os
os.makedirs("audio", exist_ok=True)
os.makedirs("data", exist_ok=True)
app.mount("/audio", StaticFiles(directory="audio"), name="audio")
app.mount("/assets", StaticFiles(directory="data"), name="assets")

@app.get("/")
def root():
    return {
        "status": "running",
        "features": [
            "auth",
            "shlokas",
            "translations",
            "audio",
            "ask-the-gita"
        ]
    }


@app.get("/debug/db")
def debug_db():
    """Temporary diagnostic endpoint to check DB connectivity."""
    import traceback
    from sqlalchemy import text as sql_text
    from app.database import SessionLocal, DATABASE_URL
    result = {"db_host": DATABASE_URL.split("@")[-1].split("/")[0] if "@" in DATABASE_URL else "localhost"}
    try:
        db = SessionLocal()
        # Check connection
        db.execute(sql_text("SELECT 1"))
        result["connection"] = "OK"
        # List tables
        tables = db.execute(sql_text(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        )).fetchall()
        result["tables"] = [t[0] for t in tables]
        # Count texts
        try:
            count = db.execute(sql_text("SELECT COUNT(*) FROM texts")).scalar()
            result["text_count"] = count
        except Exception as e:
            result["text_count_error"] = str(e)
        db.close()
    except Exception as e:
        result["error"] = str(e)
        result["traceback"] = traceback.format_exc()
    return result

