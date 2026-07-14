import sys
print("1")
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.gzip import GZipMiddleware
import time
print("2")
from dotenv import load_dotenv
load_dotenv()
print("3")
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
print("4")
from app.database import Base, engine
print("5")
from app.models import (
    user, text, translation, audio, embedding, chalisa, puja_vidhi, bookmark, history, topic, text_stats, analytics,
)
print("6")
from app.routes import (
    auth, texts, translation as translation_route, audio as audio_route, chat, chalisas, puja_vidhi as puja_vidhi_route, devotion, search as search_route, users, bookmarks, history, recommendations, topics, verse_of_day, share, stats, analytics as analytics_route,
)
print("7")
