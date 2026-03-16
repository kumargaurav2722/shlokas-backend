from sqlalchemy import Column, String, Integer, Text, UniqueConstraint, Index
from app.database import Base
import uuid

class Text(Base):
    __tablename__ = "texts"
    __table_args__ = (
        UniqueConstraint(
            "category", "work", "sub_work", "chapter", "verse",
            name="uq_texts_category_work_subwork_chapter_verse"
        ),
        Index("ix_texts_category_work_subwork", "category", "work", "sub_work"),
    )

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    category = Column(String)        # itihasa
    work = Column(String)            # Mahabharata
    sub_work = Column(String)        # Bhagavad Gita
    chapter = Column(Integer)        # 1–18
    verse = Column(Integer)          # shloka number
    sanskrit = Column(Text)
    source = Column(String)
    content = Column(Text)           # legacy column for backward compatibility
