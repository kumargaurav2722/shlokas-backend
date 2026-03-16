import argparse
import os
import re
from typing import Optional

import requests
from bs4 import BeautifulSoup
from PIL import Image

from app.database import SessionLocal
from app.models.chalisa import Chalisa
from app.utils.text_cleaner import normalize_sanskrit

DEVANAGARI_RE = re.compile(r"[\u0900-\u097F]")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest chalisas (text or pdf)")
    parser.add_argument("--replace", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def fetch_wikisource_text(page: str, lang: str = "sa") -> str:
    api = f"https://{lang}.wikisource.org/w/api.php"
    res = requests.get(
        api,
        params={
            "action": "parse",
            "format": "json",
            "page": page,
            "prop": "text",
        },
        headers={"User-Agent": "shlokas-backend/1.0"},
        timeout=60,
    )
    res.raise_for_status()
    html = res.json().get("parse", {}).get("text", {}).get("*", "")
    soup = BeautifulSoup(html, "html.parser")
    for sup in soup.find_all("sup"):
        sup.decompose()
    text = soup.get_text("\n")
    raw_lines = [line.strip() for line in text.splitlines() if line.strip()]
    lines = []
    for line in raw_lines:
        if not DEVANAGARI_RE.search(line):
            continue
        line = (
            line.replace("मूल पाठ", "")
            .replace("[ सम्पाद्यताम् ]", "")
            .replace("सम्पाद्यताम्", "")
            .strip()
        )
        if not line:
            continue
        lines.append(line)
        if "॥४०॥" in line or "॥40॥" in line or "॥ ४० ॥" in line:
            break
    return normalize_sanskrit("\n".join(lines))


def resolve_commons_url(file_name: str) -> Optional[str]:
    api = "https://commons.wikimedia.org/w/api.php"
    res = requests.get(
        api,
        params={
            "action": "query",
            "format": "json",
            "titles": f"File:{file_name}",
            "prop": "imageinfo",
            "iiprop": "url",
        },
        headers={"User-Agent": "shlokas-backend/1.0"},
        timeout=60,
    )
    res.raise_for_status()
    pages = res.json().get("query", {}).get("pages", {})
    for _, page in pages.items():
        infos = page.get("imageinfo") or []
        if infos:
            return infos[0].get("url")
    return None


def download_to_pdf(file_name: str, out_path: str) -> Optional[str]:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    image_url = resolve_commons_url(file_name)
    if not image_url:
        return None
    res = requests.get(
        image_url,
        headers={"User-Agent": "shlokas-backend/1.0"},
        timeout=60,
    )
    res.raise_for_status()
    image_path = out_path.replace(".pdf", ".jpg")
    with open(image_path, "wb") as handle:
        handle.write(res.content)
    image = Image.open(image_path).convert("RGB")
    image.save(out_path, "PDF")
    return out_path


def main() -> None:
    args = parse_args()
    db = SessionLocal()
    try:
        if args.replace and not args.dry_run:
            db.query(Chalisa).delete()
            db.commit()

        # Durga Chalisa (text, Wikisource)
        durga_page = "दुर्गा_चालीसा"
        durga_url = "https://sa.wikisource.org/wiki/दुर्गा_चालीसा"
        durga_text = fetch_wikisource_text(durga_page, "sa")
        if durga_text:
            db.add(
                Chalisa(
                    deity="Durga",
                    title="Durga Chalisa",
                    language="hi",
                    script="Devanagari",
                    content_type="text",
                    content=durga_text,
                    source_url=durga_url,
                    license="CC BY-SA 4.0",
                    attribution="Wikisource contributors",
                )
            )

        # Hanuman Chalisa (PDF from Commons image)
        hanuman_file = "Hanuman_Chalisa_4x3.jpg"
        pdf_path = download_to_pdf(
            hanuman_file,
            os.path.join("data", "chalisas", "hanuman_chalisa.pdf"),
        )
        if pdf_path:
            db.add(
                Chalisa(
                    deity="Hanuman",
                    title="Hanuman Chalisa",
                    language="hi",
                    script="Devanagari",
                    content_type="pdf",
                    file_path=pdf_path,
                    source_url="https://commons.wikimedia.org/wiki/File:Hanuman_Chalisa_4x3.jpg",
                    license="CC BY-SA 4.0",
                    attribution="Wikimedia Commons contributors",
                )
            )

        if args.dry_run:
            db.rollback()
        else:
            db.commit()
        print("Chalisas ingested.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
