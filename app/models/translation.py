from sqlalchemy import Column, String, Text, ForeignKey
from app.database import Base
import uuid

class Translation(Base):
    __tablename__ = "translations"

    id = Column(
        String,
        primary_key=True,
        default=lambda: str(uuid.uuid4())
    )

    text_id = Column(
        String,
        ForeignKey("texts.id", ondelete="CASCADE"),
        nullable=False
    )

    language = Column(String, nullable=False)
    translation = Column(Text, nullable=False)
    commentary = Column(Text)
    generated_by = Column(String)
