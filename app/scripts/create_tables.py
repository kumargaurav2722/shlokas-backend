from app.database import Base, engine

from app.models import (
    user,
    text,
    translation,
    audio,
    embedding,
    chalisa,
    puja_vidhi,
)


def main() -> None:
    Base.metadata.create_all(bind=engine)
    print("Tables ensured.")


if __name__ == "__main__":
    main()
