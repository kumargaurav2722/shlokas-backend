from sqlalchemy import Column, DateTime, String, Text, ForeignKey, UniqueConstraint
from sqlalchemy.sql import func
from app.database import Base
import uuid


class Bookmark(Base):
    __tablename__ = "bookmarks"
    __table_args__ = (
        UniqueConstraint("user_id", "type", "item_id", name="uq_bookmark_user_item"),
    )

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, ForeignKey("users.id"), nullable=False)
    type = Column(String, nullable=False)
    item_id = Column(String, nullable=False)
    title = Column(String, nullable=False)
    route = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
