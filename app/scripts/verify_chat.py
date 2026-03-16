import argparse
from fastapi.testclient import TestClient
from app.main import app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify /chat end-to-end")
    parser.add_argument("--question", default="What is the meaning of selfless action?")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    client = TestClient(app)
    res = client.post("/chat/", params={"question": args.question})
    print("status", res.status_code)
    print(res.json())


if __name__ == "__main__":
    main()
