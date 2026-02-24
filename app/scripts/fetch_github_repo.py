import argparse
import os
import requests
from typing import Optional

GITHUB_API = "https://api.github.com"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recursively download files from a GitHub repo path")
    parser.add_argument("--repo", required=True, help="owner/repo")
    parser.add_argument("--path", default="", help="Path inside repo, e.g. 'data'")
    parser.add_argument("--out", default="data/github")
    parser.add_argument("--ext", default=".json")
    parser.add_argument("--token", default=os.getenv("GITHUB_TOKEN"))
    parser.add_argument("--max-files", type=int, default=0)
    return parser.parse_args()


def request_json(url: str, token: Optional[str]) -> list:
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    res = requests.get(url, headers=headers, timeout=30)
    res.raise_for_status()
    return res.json()


def download_file(url: str, out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    res = requests.get(url, timeout=60)
    res.raise_for_status()
    with open(out_path, "wb") as handle:
        handle.write(res.content)


def walk(repo: str, path: str, out_root: str, ext: str, token: Optional[str], max_files: int) -> int:
    url = f"{GITHUB_API}/repos/{repo}/contents/{path}" if path else f"{GITHUB_API}/repos/{repo}/contents"
    items = request_json(url, token)

    downloaded = 0
    for item in items:
        if max_files and downloaded >= max_files:
            break
        item_type = item.get("type")
        item_path = item.get("path", "")

        if item_type == "dir":
            downloaded += walk(repo, item_path, out_root, ext, token, max_files - downloaded if max_files else 0)
            continue

        if item_type == "file":
            name = item.get("name", "")
            if ext and not name.endswith(ext):
                continue
            download_url = item.get("download_url")
            if not download_url:
                continue
            out_path = os.path.join(out_root, item_path)
            download_file(download_url, out_path)
            downloaded += 1

    return downloaded


def main() -> None:
    args = parse_args()
    os.makedirs(args.out, exist_ok=True)
    count = walk(args.repo, args.path, args.out, args.ext, args.token, args.max_files)
    print(f"Downloaded {count} files into {args.out}")


if __name__ == "__main__":
    main()
