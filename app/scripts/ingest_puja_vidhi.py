import argparse
from typing import Optional, Tuple

import requests
from bs4 import BeautifulSoup

from app.database import SessionLocal
from app.models.puja_vidhi import PujaVidhi
from app.utils.text_cleaner import normalize_sanskrit

HI_SECTION_KEYS = [
    "विधि",
    "पूजा",
    "पूजन",
    "अनुष्ठान",
    "रीति",
    "परंपरा",
]
EN_SECTION_KEYS = [
    "Ritual",
    "Worship",
    "Observance",
    "Practice",
    "Celebration",
    "Puja",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest puja vidhi text from Wikipedia")
    parser.add_argument("--replace", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def fetch_section_text(page: str, lang: str) -> Tuple[Optional[str], Optional[str]]:
    api = f"https://{lang}.wikipedia.org/w/api.php"
    res = requests.get(
        api,
        params={
            "action": "parse",
            "format": "json",
            "page": page,
            "prop": "sections",
            "redirects": 1,
        },
        headers={"User-Agent": "shlokas-backend/1.0"},
        timeout=60,
    )
    if res.status_code != 200:
        return None, None
    data = res.json()
    sections = data.get("parse", {}).get("sections", [])
    keys = HI_SECTION_KEYS if lang == "hi" else EN_SECTION_KEYS
    section_index = None
    section_title = None
    for sec in sections:
        title = sec.get("line", "")
        if any(k.lower() in title.lower() for k in keys):
            section_index = sec.get("index")
            section_title = title
            break

    if section_index is None:
        section_index = 0
        section_title = "Lead"

    res = requests.get(
        api,
        params={
            "action": "parse",
            "format": "json",
            "page": page,
            "prop": "text",
            "section": section_index,
            "redirects": 1,
        },
        headers={"User-Agent": "shlokas-backend/1.0"},
        timeout=60,
    )
    if res.status_code != 200:
        return None, None
    html = res.json().get("parse", {}).get("text", {}).get("*", "")
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.select("sup, .reference, .mw-editsection, .navbox, .infobox, .metadata"):
        tag.decompose()
    text = soup.get_text("\n")
    lines = []
    for line in text.splitlines():
        cleaned = line.strip()
        if not cleaned:
            continue
        if "संपादित करें" in cleaned or "edit" in cleaned.lower():
            continue
        if cleaned.startswith("^") or cleaned.startswith("↑"):
            continue
        if "अभिगमन तिथि" in cleaned or "ISBN" in cleaned:
            continue
        if "Archived" in cleaned or "वेबैक मशीन" in cleaned:
            continue
        if "http://" in cleaned or "https://" in cleaned:
            continue
        if "CS1 maint" in cleaned or "link )" in cleaned or "maint:" in cleaned:
            continue
        lines.append(cleaned)
    return normalize_sanskrit("\n".join(lines)), section_title


def ingest_entry(db, deity: str, hi_page: str, en_page: str) -> None:
    text, section = fetch_section_text(hi_page, "hi")
    lang = "hi"
    source_url = f"https://hi.wikipedia.org/wiki/{hi_page}"
    if not text:
        text, section = fetch_section_text(en_page, "en")
        lang = "en"
        source_url = f"https://en.wikipedia.org/wiki/{en_page}"
    if not text:
        return
    title = f"{deity} Puja Vidhi ({section})"
    db.add(
        PujaVidhi(
            deity=deity,
            title=title,
            language=lang,
            script="Devanagari" if lang == "hi" else "Latin",
            content_type="text",
            content=text,
            source_url=source_url,
            license="CC BY-SA 4.0",
            attribution="Wikipedia contributors",
        )
    )


def main() -> None:
    args = parse_args()
    db = SessionLocal()
    try:
        if args.replace and not args.dry_run:
            db.query(PujaVidhi).delete()
            db.commit()

        ingest_entry(db, "Ganesh", "गणेश_चतुर्थी", "Ganesh_Chaturthi")
        ingest_entry(db, "Lakshmi", "लक्ष्मी_पूजा", "Lakshmi_Puja")
        ingest_entry(db, "Saraswati", "सरस्वती_पूजा", "Saraswati_Puja")
        ingest_entry(db, "Durga", "दुर्गा_पूजा", "Durga_Puja")
        ingest_entry(db, "Shiva", "महाशिवरात्रि", "Maha_Shivaratri")
        ingest_entry(db, "Krishna", "जन्माष्टमी", "Krishna_Janmashtami")

        if args.dry_run:
            db.rollback()
        else:
            db.commit()
        print("Puja vidhi ingested.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
