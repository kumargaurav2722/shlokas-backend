"""
Chat endpoint — "Ask the Gita" RAG pipeline.

Flow:
  1. Check if embeddings exist → auto-build if empty
  2. Semantic search for top-K relevant verses
  3. Build context from matching verses + translations
  4. Call LLM (Groq → Ollama → keyword fallback)
  5. Return structured answer with verse references
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models.text import Text
from app.models.translation import Translation
from app.models.embedding import Embedding
from app.embeddings.embedder import generate_embedding
from app.search.semantic import semantic_search
from app.llm.llm_provider import generate_answer

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/chat", tags=["Chat"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class VerseReference(BaseModel):
    chapter: int
    verse: int
    work: str
    sub_work: str
    sanskrit: Optional[str] = None
    translation: Optional[str] = None


class ChatResponse(BaseModel):
    answer: str
    references: list[VerseReference]
    provider: str
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Auto-build embeddings when table is empty
# ---------------------------------------------------------------------------

def _auto_build_embeddings(db: Session) -> int:
    """Build embeddings for all texts that don't have one yet.
    Uses translations when available, falls back to Sanskrit text."""
    existing_ids = {
        row[0]
        for row in db.query(Embedding.text_id).all()
    }

    texts = db.query(Text).all()
    created = 0

    for t in texts:
        if t.id in existing_ids:
            continue

        # Prefer English translation, then any translation, then Sanskrit
        content = None
        tr = db.query(Translation).filter_by(
            text_id=t.id, language="en"
        ).first()
        if tr:
            content = tr.translation
        else:
            tr = db.query(Translation).filter_by(text_id=t.id).first()
            if tr:
                content = tr.translation

        if not content:
            content = t.sanskrit or t.content

        if not content:
            continue

        try:
            emb = generate_embedding(content)
            embedding = Embedding(text_id=t.id, embedding=emb)
            db.merge(embedding)
            created += 1
        except Exception as exc:
            logger.warning("Failed to embed text %s: %s", t.id, exc)

    if created > 0:
        db.commit()
        logger.info("Auto-built %d embeddings", created)

    return created


# ---------------------------------------------------------------------------
# Chat endpoint
# ---------------------------------------------------------------------------

@router.post("/", response_model=ChatResponse)
def ask(
    question: str,
    scope: Optional[str] = Query(
        None,
        description="Filter to a specific scripture, e.g. 'Bhagavad Gita'"
    ),
    db: Session = Depends(get_db),
):
    # Step 1: Ensure we have embeddings
    embedding_count = db.query(Embedding).count()
    if embedding_count == 0:
        text_count = db.query(Text).count()
        if text_count == 0:
            return ChatResponse(
                answer="No scripture data has been ingested yet. "
                       "Please run the ingestion scripts first.",
                references=[],
                provider="none",
                error="no_data",
            )
        logger.info(
            "Embeddings table empty, auto-building from %d texts...",
            text_count,
        )
        _auto_build_embeddings(db)

    # Step 2: Load embeddings with their verse + translation data
    query = (
        db.query(Embedding, Text, Translation)
        .join(Text, Text.id == Embedding.text_id)
        .outerjoin(
            Translation,
            (Translation.text_id == Text.id) & (Translation.language == "en"),
        )
    )

    # Apply scope filter
    if scope:
        query = query.filter(Text.sub_work == scope)
    else:
        # Default to Bhagavad Gita if available
        gita_count = (
            db.query(Embedding)
            .join(Text, Text.id == Embedding.text_id)
            .filter(Text.sub_work == "Bhagavad Gita")
            .count()
        )
        if gita_count > 0:
            query = query.filter(Text.sub_work == "Bhagavad Gita")

    rows = query.all()

    if not rows:
        return ChatResponse(
            answer="No indexed verses found. Embeddings may still be building.",
            references=[],
            provider="none",
            error="no_embeddings",
        )

    # Step 3: Semantic search — find top-K matching verses
    # Build search-compatible row objects
    class SearchRow:
        def __init__(self, embedding, text, translation):
            self.embedding = embedding.embedding
            self.id = text.id
            self.chapter = text.chapter
            self.verse = text.verse
            self.work = text.work
            self.sub_work = text.sub_work
            self.sanskrit = text.sanskrit
            self.translation = translation.translation if translation else None

    search_rows = [SearchRow(emb, txt, tr) for emb, txt, tr in rows]

    try:
        top = semantic_search(question, search_rows, top_k=5)
    except Exception as exc:
        logger.error("Semantic search failed: %s", exc)
        return ChatResponse(
            answer="Search failed. Please try again.",
            references=[],
            provider="none",
            error="search_error",
        )

    # Step 4: Build context string and references
    references = []
    context_parts = []
    for r in top:
        ref = VerseReference(
            chapter=r.chapter,
            verse=r.verse,
            work=r.work,
            sub_work=r.sub_work,
            sanskrit=r.sanskrit,
            translation=r.translation,
        )
        references.append(ref)

        verse_label = f"{r.sub_work or r.work} {r.chapter}:{r.verse}"
        if r.translation:
            context_parts.append(f"{verse_label} — {r.translation}")
        elif r.sanskrit:
            context_parts.append(f"{verse_label} — {r.sanskrit}")

    context = "\n\n".join(context_parts)

    if not context:
        return ChatResponse(
            answer="Could not find relevant verses for your question.",
            references=[],
            provider="none",
            error="no_context",
        )

    # Step 5: Generate answer using LLM (Groq → Ollama → fallback)
    answer, provider = generate_answer(question, context)

    return ChatResponse(
        answer=answer,
        references=references,
        provider=provider,
    )
