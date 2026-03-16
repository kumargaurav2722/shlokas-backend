from sqlalchemy import Column, String, Text
from app.database import Base
import uuid


class Chalisa(Base):
    __tablename__ = "chalisas"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    deity = Column(String, nullable=False)
    title = Column(String, nullable=False)
    language = Column(String, nullable=False)
    script = Column(String)
    content_type = Column(String, nullable=False)  # text | pdf
    content = Column(Text)
    file_path = Column(String)
    source_url = Column(String)
    license = Column(String)
    attribution = Column(Text)
