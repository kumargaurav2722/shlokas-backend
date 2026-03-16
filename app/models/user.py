from sqlalchemy import Column, Integer, String, Date
from sqlalchemy.dialects.postgresql import JSONB
from app.database import Base
import uuid

class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    email = Column(String, unique=True, index=True, nullable=False)
    password_hash = Column(String)
    full_name = Column(String)
    age = Column(Integer)
    gender = Column(String)
    region = Column(String)
    preferred_language = Column(String, default="en")
    interests = Column(JSONB, default=list)
    last_seen_verse_of_day = Column(Date)
    streak_count = Column(Integer, default=0)
