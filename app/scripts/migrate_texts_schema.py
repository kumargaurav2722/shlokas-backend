import os
import sqlalchemy as sa

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://app_user:strongpassword@localhost:5432/shlokas_db"
)

TARGET_COLUMNS = {
    "category": "TEXT",
    "work": "TEXT",
    "sub_work": "TEXT",
    "chapter": "INTEGER",
    "verse": "INTEGER",
    "sanskrit": "TEXT",
    "source": "TEXT",
}


def column_exists(conn, name: str) -> bool:
    result = conn.execute(sa.text(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'texts' AND column_name = :name
        """
    ), {"name": name}).fetchone()
    return result is not None


def constraint_exists(conn, name: str) -> bool:
    result = conn.execute(sa.text(
        """
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_name = 'texts' AND constraint_name = :name
        """
    ), {"name": name}).fetchone()
    return result is not None


def index_exists(conn, name: str) -> bool:
    result = conn.execute(sa.text(
        """
        SELECT 1
        FROM pg_indexes
        WHERE tablename = 'texts' AND indexname = :name
        """
    ), {"name": name}).fetchone()
    return result is not None


def main() -> None:
    engine = sa.create_engine(DATABASE_URL)
    with engine.begin() as conn:
        for col, ddl in TARGET_COLUMNS.items():
            if not column_exists(conn, col):
                conn.execute(sa.text(f"ALTER TABLE texts ADD COLUMN {col} {ddl}"))

        if column_exists(conn, "content") and column_exists(conn, "sanskrit"):
            conn.execute(sa.text(
                """
                UPDATE texts
                SET sanskrit = COALESCE(sanskrit, content)
                WHERE sanskrit IS NULL OR sanskrit = ''
                """
            ))

        if not index_exists(conn, "ix_texts_category_work_subwork"):
            conn.execute(sa.text(
                """
                CREATE INDEX ix_texts_category_work_subwork
                ON texts (category, work, sub_work)
                """
            ))

        if not constraint_exists(conn, "uq_texts_category_work_subwork_chapter_verse"):
            conn.execute(sa.text(
                """
                ALTER TABLE texts
                ADD CONSTRAINT uq_texts_category_work_subwork_chapter_verse
                UNIQUE (category, work, sub_work, chapter, verse)
                """
            ))

    print("texts table migration complete")


if __name__ == "__main__":
    main()
