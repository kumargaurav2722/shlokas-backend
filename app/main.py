from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.gzip import GZipMiddleware
import time
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from app.database import Base, engine

# Load environment variables from .env for local development
load_dotenv()

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

# Table creation is handled offline via Alembic or scripts, avoiding startup hangs.

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
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        _frontend_url,
        "*",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
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
