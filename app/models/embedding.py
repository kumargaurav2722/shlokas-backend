from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy import Float
from app.database import Base

class Embedding(Base):
    __tablename__ = "embeddings"

    # one embedding per shloka
    text_id = Column(String, primary_key=True)
    embedding = Column(ARRAY(Float))  # stored as float array
