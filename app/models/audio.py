from sqlalchemy import Column, String, ForeignKey
from app.database import Base
import uuid

class Audio(Base):
    __tablename__ = "audio"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    text_id = Column(String, ForeignKey("texts.id", ondelete="CASCADE"))
    language = Column(String)
    audio_path = Column(String)
    voice_type = Column(String)
