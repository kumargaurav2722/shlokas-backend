from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Index
from sqlalchemy.sql import func
from app.database import Base


class TextStats(Base):
    __tablename__ = "text_stats"

    text_id = Column(String, ForeignKey("texts.id", ondelete="CASCADE"), primary_key=True)
    read_count = Column(Integer, nullable=False, default=0)
    bookmark_count = Column(Integer, nullable=False, default=0)
    last_read_at = Column(DateTime(timezone=True))
    last_bookmarked_at = Column(DateTime(timezone=True))

    __table_args__ = (
        Index("ix_text_stats_read_count", "read_count"),
        Index("ix_text_stats_bookmark_count", "bookmark_count"),
    )
