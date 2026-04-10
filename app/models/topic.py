from sqlalchemy import Column, String, Text, ForeignKey, Integer, UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import UUID
from app.database import Base
import uuid


class Topic(Base):
    __tablename__ = "topics"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    slug = Column(String, unique=True, nullable=False, index=True)
    name = Column(String, nullable=False)
    description = Column(Text)


class TopicItem(Base):
    __tablename__ = "topic_items"
    __table_args__ = (
        UniqueConstraint("topic_id", "text_id", name="uq_topic_items_topic_text"),
        Index("ix_topic_items_topic_text", "topic_id", "text_id"),
    )

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    topic_id = Column(
        String,
        ForeignKey("topics.id", ondelete="CASCADE"),
        nullable=False,
    )
    text_id = Column(
        String,
        ForeignKey("texts.id", ondelete="CASCADE"),
        nullable=False,
    )
    score = Column(Integer)
    matched_keyword = Column(String)


class TopicTranslation(Base):
    __tablename__ = "topic_translations"
    __table_args__ = (
        UniqueConstraint("topic_id", "language", name="uq_topic_translations_topic_lang"),
        Index("ix_topic_translations_topic_lang", "topic_id", "language"),
    )

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    topic_id = Column(
        String,
        ForeignKey("topics.id", ondelete="CASCADE"),
        nullable=False,
    )
    language = Column(String, nullable=False)
    name = Column(String, nullable=False)
    description = Column(Text)
