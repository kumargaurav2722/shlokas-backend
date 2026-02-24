import argparse
import csv
import json
from typing import Iterable, Dict, Any, List, Tuple

from app.utils.text_cleaner import normalize_sanskrit

REQUIRED_FIELDS = ("chapter", "verse", "sanskrit")


def parse_args(description: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--input", required=True, help="Path to JSON or CSV file")
    parser.add_argument("--format", choices=["json", "csv"], required=True)
    parser.add_argument("--category", required=True)
    parser.add_argument("--work", required=True)
    parser.add_argument("--sub-work", required=True, dest="sub_work")
    parser.add_argument("--source", required=True)
    parser.add_argument("--sanskrit-field", default="sanskrit")
    parser.add_argument("--chapter-field", default="chapter")
    parser.add_argument("--verse-field", default="verse")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--dedupe", action="store_true", help="Skip rows that already exist")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of rows ingested")
    return parser.parse_args()


def _coerce_int(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    value = str(value).strip()
    return int(value)


def _validate_row(row: Dict[str, Any], row_idx: int) -> None:
    missing = [f for f in REQUIRED_FIELDS if row.get(f) in (None, "")]
    if missing:
        raise ValueError(f"Row {row_idx} missing required fields: {missing}")


def load_rows(
    path: str,
    fmt: str,
    chapter_field: str,
    verse_field: str,
    sanskrit_field: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    if fmt == "json":
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        if not isinstance(data, list):
            raise ValueError("JSON input must be a list of objects")
        for idx, item in enumerate(data, start=1):
            row = {
                "chapter": item.get(chapter_field),
                "verse": item.get(verse_field),
                "sanskrit": item.get(sanskrit_field),
            }
            _validate_row(row, idx)
            rows.append(row)

    elif fmt == "csv":
        with open(path, "r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for idx, item in enumerate(reader, start=1):
                row = {
                    "chapter": item.get(chapter_field),
                    "verse": item.get(verse_field),
                    "sanskrit": item.get(sanskrit_field),
                }
                _validate_row(row, idx)
                rows.append(row)

    return rows


def normalize_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        normalized.append(
            {
                "chapter": _coerce_int(row["chapter"]),
                "verse": _coerce_int(row["verse"]),
                "sanskrit": normalize_sanskrit(row["sanskrit"]),
            }
        )
    return normalized


def clamp_rows(rows: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    if limit and limit > 0:
        return rows[:limit]
    return rows
