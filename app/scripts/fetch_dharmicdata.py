import argparse
import os
import requests

GITHUB_API = "https://api.github.com"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download files from DharmicData GitHub repo")
    parser.add_argument("--repo", default="bhavykhatri/DharmicData")
    parser.add_argument("--path", required=True, help="Path inside repo, e.g. 'Texts/BhagavadGita' or similar")
    parser.add_argument("--out", default="data/dharmicdata")
    parser.add_argument("--ext", default=".json", help="File extension to download")
    parser.add_argument("--token", default=os.getenv("GITHUB_TOKEN"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    os.makedirs(args.out, exist_ok=True)

    url = f"{GITHUB_API}/repos/{args.repo}/contents/{args.path}"
    headers = {"Accept": "application/vnd.github+json"}
    if args.token:
        headers["Authorization"] = f"Bearer {args.token}"

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    items = response.json()

    downloaded = 0
    for item in items:
        if item.get("type") != "file":
            continue
        name = item.get("name", "")
        if not name.endswith(args.ext):
            continue
        download_url = item.get("download_url")
        if not download_url:
            continue

        out_path = os.path.join(args.out, name)
        file_response = requests.get(download_url, timeout=60)
        file_response.raise_for_status()
        with open(out_path, "wb") as handle:
            handle.write(file_response.content)
        downloaded += 1

    print(f"Downloaded {downloaded} files to {args.out}")


if __name__ == "__main__":
    main()
