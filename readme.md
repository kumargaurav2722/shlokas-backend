Backend Vision
The backend must act as:
A source of truth for Hindu scriptures
A language + audio engine
A non-hallucinating AI guide
A low-cost, scalable system
A future-proof API layer
🛠️ Backend Tech Stack
FastAPI
PostgreSQL
SQLAlchemy
Alembic (future migrations)
Local LLM (Ollama)
Local TTS (Coqui XTTS)
Sentence Transformers (embeddings)
RAG (Retrieval Augmented Generation)
📁 Backend Folder Structure (Current + Planned)
app/
├── main.py
├── database.py
│
├── models/
│   ├── user.py
│   ├── text.py
│   ├── translation.py
│   ├── audio.py
│   └── embedding.py
│
├── routes/
│   ├── auth.py
│   ├── texts.py
│   ├── translation.py
│   ├── audio.py
│   └── chat.py
│
├── audio/
│   └── tts.py
│
├── embeddings/
│   └── embedder.py
│
├── scripts/
│   ├── ingest_gita.py
│   ├── ingest_vedas.py
│   ├── ingest_upanishads.py
│   ├── ingest_puranas.py
│   └── build_embeddings.py
│
└── utils/
    └── text_cleaner.py
🟢 BACKEND PHASE 1 — CORE DATA MODEL
Status: ✅ COMPLETED
Tables Implemented
users
texts
translations
audio
embeddings
texts table design (core)
Each row represents ONE verse / mantra / shloka.
Fields:
category (veda, upanishad, itihasa, purana)
work (Mahabharata, Rigveda, etc.)
sub_work (Bhagavad Gita, Mandala, Kanda)
chapter
verse
sanskrit
source
This design is deliberately generic so any scripture can be added without schema change.
🟡 BACKEND PHASE 2 — DATA INGESTION (CRITICAL)
Status: ⏳ PARTIALLY DONE (Gita sample only)
This is the MOST IMPORTANT phase for authenticity.
🔥 TASK 2.1 — Bhagavad Gita (MANDATORY)
Data to ingest
All 18 chapters
All 700+ shlokas
Sanskrit text only (translations handled separately)
Recommended authentic sources
Gita Press Gorakhpur (public domain editions)
IIT Kanpur Gita archive
SacredTexts.org (verify consistency)
Task
Create script:
📄 scripts/ingest_gita.py
Responsibilities:
Load Gita data (JSON / CSV / scraped text)
Normalize Sanskrit text
Insert into texts table
Example record:
{
  "category": "itihasa",
  "work": "Mahabharata",
  "sub_work": "Bhagavad Gita",
  "chapter": 2,
  "verse": 47,
  "sanskrit": "कर्मण्येवाधिकारस्ते...",
  "source": "Gita Press"
}
🔥 TASK 2.2 — VEDAS (MANDATORY)
Scope
Rigveda (ALL mandalas + suktas)
Yajurveda (major sections)
Samaveda (key chants)
Atharvaveda (important hymns)
Important Note
Do NOT try to ingest everything at once.
Phase-wise ingestion
Start with Rigveda Mandala 1
Add important suktas
Expand gradually
Script
📄 scripts/ingest_vedas.py
🔥 TASK 2.3 — UPANISHADS (IMPORTANT)
Priority list
Isha
Kena
Katha
Prashna
Mundaka
Mandukya
Brihadaranyaka
Chandogya
Data format
Each mantra = one texts row
📄 scripts/ingest_upanishads.py
🔥 TASK 2.4 — PURANAS (SELECTIVE)
Do NOT ingest full puranas initially.
Instead ingest:
Important stories
Important verses
Philosophical sections
Priority:
Vishnu Purana
Bhagavata Purana
Shiva Purana
📄 scripts/ingest_puranas.py
🟣 BACKEND PHASE 3 — TRANSLATION & COMMENTARY
Status: ✅ COMPLETED (ON-DEMAND)
APIs Implemented
POST /translate/{text_id}
Features
Uses local LLM
Generates:
translation
commentary
Caches result in DB
Multi-language support
🔧 Remaining Improvements
Batch translation jobs
Manual override for human translations
Source tagging (AI vs human)
🔵 BACKEND PHASE 4 — AUDIO (TTS)
Status: ✅ COMPLETED
Design (IMPORTANT)
Audio is NOT stored in DB
Audio files stored on filesystem
DB stores only metadata
APIs
POST /audio/{text_id}?language=hi
POST /audio/{text_id}?language=sanskrit
Tasks Remaining
Sanskrit pronunciation tuning
Slow / fast chanting modes
Verse-level pause tuning
Optional mantra-style chanting
🟠 BACKEND PHASE 5 — EMBEDDINGS & SEARCH
Status: ✅ COMPLETED (Basic)
Tables
embeddings
Script
📄 scripts/build_embeddings.py
Current behavior
Embeddings built from English translation
Stored as vectors
Used for semantic search
Improvements Needed
Per-language embeddings
Topic-level embeddings
Chapter-level embeddings
🔴 BACKEND PHASE 6 — ASK THE GITA (RAG CHATBOT)
Status: ✅ FUNCTIONAL (Needs Expansion)
Current Implementation
User question
Semantic search over embeddings
Top-k verses retrieved
Prompt sent to LLM
Answer returned with grounding
🔥 TASK 6.1 — RAG FROM AUTHENTIC SOURCE (MANDATORY)
Goal
Build Gita-only RAG using trusted source.
Options
Gita PDF (Gita Press)
IIT Kanpur Gita dataset
Verified online Gita corpus
Steps
Parse PDF / source
Chunk by verse
Store text → texts
Build embeddings
Restrict RAG scope to Gita
🔥 TASK 6.2 — MULTI-SCRIPTURE RAG
Add filters:
Only Gita
Only Upanishads
Only Vedas
All scriptures
API Extension
POST /chat
{
  "question": "...",
  "scope": "gita"
}
🔥 TASK 6.3 — ANSWER TRANSPARENCY
Return:
Answer
Verse references
Confidence note if unclear
⚫ BACKEND PHASE 7 — AUTH & USER FEATURES
Status: ⏳ PARTIAL
To be implemented
Forgot password
Google OAuth
OTP login
User preferences (language, audio)
🧪 BACKEND PHASE 8 — QA & VALIDATION
Required Tasks
Sanskrit text verification
Duplicate verse detection
Translation consistency checks
RAG hallucination tests
📡 BACKEND APIs (FINAL LIST)
Texts
GET /texts/chapters
GET /texts/verses
Translation
POST /translate/{text_id}
Audio
POST /audio/{text_id}
Chat
POST /chat
Auth
POST /auth/login
POST /auth/register
🧭 HOW TO ASSIGN THIS TO CODEX / DEV
Example task:
“Implement Backend Phase 2.1:
Ingest all Bhagavad Gita verses
Use Gita Press source
Validate chapter & verse counts
Write reusable ingestion script”
Another:
“Extend RAG to support scope filtering:
Only Gita
Only Upanishads”