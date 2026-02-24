from sqlalchemy import text
from app.database import engine


STATEMENTS = [
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS full_name TEXT",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS age INTEGER",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS gender TEXT",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS region TEXT",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS preferred_language TEXT DEFAULT 'en'",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS interests JSONB DEFAULT '[]'::jsonb",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_seen_verse_of_day DATE",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS streak_count INTEGER DEFAULT 0",
]


def main():
    with engine.begin() as conn:
        for stmt in STATEMENTS:
            conn.execute(text(stmt))
    print("User profile columns ensured.")


if __name__ == "__main__":
    main()
