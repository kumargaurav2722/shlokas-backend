import os
import secrets
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, HTTPException, Body
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
from pydantic import BaseModel, EmailStr

from app.database import SessionLocal
from app.models.user import User
from app.utils.security import hash_password, verify_password, create_token
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
import requests

router = APIRouter(prefix="/auth", tags=["Auth"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str
    fullName: str
    age: int | None = None
    gender: str | None = None
    region: str | None = None
    preferredLanguage: str | None = None
    interests: list[str] | None = None


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


@router.post("/register")
def register(
    payload: RegisterRequest | None = Body(None),
    email: str | None = None,
    password: str | None = None,
    db: Session = Depends(get_db),
):
    if payload:
        email = payload.email
        password = payload.password
        if not payload.fullName:
            raise HTTPException(400, "Full name is required")
    if not email or not password:
        raise HTTPException(400, "Email and password are required")
    if db.query(User).filter(User.email == email).first():
        raise HTTPException(400, "Email already exists")

    user = User(
        email=email,
        password_hash=hash_password(password),
        full_name=payload.fullName if payload else None,
        age=payload.age if payload else None,
        gender=payload.gender if payload else None,
        region=payload.region if payload else None,
        preferred_language=payload.preferredLanguage if payload else "en",
        interests=payload.interests if payload and payload.interests else [],
    )
    db.add(user)
    db.commit()
    return {"message": "User registered"}

@router.post("/login")
def login(
    payload: LoginRequest | None = Body(None),
    email: str | None = None,
    password: str | None = None,
    db: Session = Depends(get_db),
):
    if payload:
        email = payload.email
        password = payload.password
    if not email or not password:
        raise HTTPException(400, "Email and password are required")

    user = db.query(User).filter(User.email == email).first()
    if not user or not user.password_hash or not verify_password(password, user.password_hash):
        raise HTTPException(401, "Invalid credentials")

    token = create_token({"user_id": user.id})
    return {"access_token": token}


@router.get("/google/start")
def google_start(next: str | None = None):
    client_id = os.getenv("GOOGLE_CLIENT_ID")
    redirect_uri = os.getenv(
        "GOOGLE_REDIRECT_URI",
        "http://127.0.0.1:8000/auth/google/callback"
    )
    if not client_id:
        raise HTTPException(500, "Google OAuth not configured")

    state = secrets.token_urlsafe(16)
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": "openid email profile",
        "access_type": "offline",
        "prompt": "consent",
        "state": state,
    }
    if next:
        params["next"] = next

    auth_url = "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)
    return RedirectResponse(auth_url)


@router.get("/google/callback")
def google_callback(
    code: str | None = None,
    state: str | None = None,
    next: str | None = None,
    db: Session = Depends(get_db),
):
    if not code:
        raise HTTPException(400, "Missing code")

    client_id = os.getenv("GOOGLE_CLIENT_ID")
    client_secret = os.getenv("GOOGLE_CLIENT_SECRET")
    redirect_uri = os.getenv(
        "GOOGLE_REDIRECT_URI",
        "http://127.0.0.1:8000/auth/google/callback"
    )
    frontend_url = os.getenv("FRONTEND_URL", "http://localhost:5173")

    if not client_id or not client_secret:
        raise HTTPException(500, "Google OAuth not configured")

    token_res = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "code": code,
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        },
        timeout=30,
    )
    if token_res.status_code != 200:
        raise HTTPException(400, "Failed to exchange code")

    token_data = token_res.json()
    id_token_str = token_data.get("id_token")
    if not id_token_str:
        raise HTTPException(400, "Missing id_token")

    try:
        info = id_token.verify_oauth2_token(
            id_token_str,
            google_requests.Request(),
            client_id,
        )
    except Exception:
        raise HTTPException(401, "Invalid Google token")

    email = info.get("email")
    name = info.get("name")
    if not email:
        raise HTTPException(400, "No email in token")

    user = db.query(User).filter(User.email == email).first()
    if not user:
        user = User(
            email=email,
            password_hash=None,
            full_name=name,
            preferred_language="en",
            interests=[]
        )
        db.add(user)
        db.commit()
    elif not user.full_name and name:
        user.full_name = name
        db.commit()

    token = create_token({"user_id": user.id})
    redirect_target = next or f"{frontend_url}/auth/google/callback"
    return RedirectResponse(f"{redirect_target}?token={token}")




