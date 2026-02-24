from app.database import SessionLocal
from app.models.text import Text
from app.models.translation import Translation
from app.embeddings.embedder import generate_embedding
from sqlalchemy import text as sql_text

db = SessionLocal()

verses = db.query(Text).all()

for v in verses:
    tr = db.query(Translation).filter_by(
        text_id=v.id, language="English"
    ).first()

    if not tr:
        continue

    emb = generate_embedding(tr.translation)

    db.execute(
        sql_text(
            """
            INSERT INTO embeddings (text_id, embedding)
            VALUES (:id, :emb)
            ON CONFLICT (text_id) DO UPDATE SET embedding = :emb
            """
        ),
        {"id": v.id, "emb": emb}
    )

db.commit()
db.close()

print("Embeddings created")
