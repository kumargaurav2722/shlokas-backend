import argparse
import os
from typing import List, Tuple

from app.scripts.fetch_github_repo import main as fetch_main
from app.scripts.ingest_from_json import main as ingest_main
from app.scripts.vedic_ingest import main as vedic_main


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download + ingest DharmicData datasets")
    parser.add_argument("--out", default="data/dharmicdata")
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--dedupe", action="store_true")
    return parser.parse_args()


def run_fetch(repo: str, path: str, out_dir: str) -> None:
    os.environ.setdefault("GITHUB_REPO", repo)
    os.environ.setdefault("GITHUB_PATH", path)


def invoke_fetch(repo: str, path: str, out_dir: str) -> None:
    import sys
    sys.argv = [
        "fetch_github_repo",
        "--repo", repo,
        "--path", path,
        "--out", out_dir,
        "--ext", ".json",
    ]
    fetch_main()


def invoke_ingest(
    input_path: str,
    category: str,
    work: str,
    sub_work: str,
    source: str,
    dry_run: bool,
    dedupe: bool,
) -> None:
    import sys
    args = [
        "ingest_from_json",
        "--input", input_path,
        "--category", category,
        "--work", work,
        "--sub-work", sub_work,
        "--source", source,
        "--chapter-from-filename",
    ]
    if dry_run:
        args.append("--dry-run")
    if dedupe:
        args.append("--dedupe")
    sys.argv = args
    ingest_main()


def invoke_vedic_ingest(
    input_path: str,
    veda: str,
    source: str,
    dry_run: bool,
    dedupe: bool,
) -> None:
    import sys
    args = [
        "vedic_ingest",
        "--input", input_path,
        "--veda", veda,
        "--source", source,
    ]
    if dry_run:
        args.append("--dry-run")
    if dedupe:
        args.append("--dedupe")
    sys.argv = args
    vedic_main()

def main() -> None:
    args = parse_args()
    repo = "bhavykhatri/DharmicData"

    downloads: List[Tuple[str, str, str]] = [
        (repo, "SrimadBhagvadGita", os.path.join(args.out, "gita")),
        (repo, "Rigveda", os.path.join(args.out, "rigveda")),
        (repo, "Yajurveda", os.path.join(args.out, "yajurveda")),
        (repo, "AtharvaVeda", os.path.join(args.out, "atharvaveda")),
    ]

    if not args.skip_download:
        for repo_name, path, out_dir in downloads:
            invoke_fetch(repo_name, path, out_dir)

    invoke_ingest(
        input_path=os.path.join(args.out, "gita"),
        category="itihasa",
        work="Mahabharata",
        sub_work="Bhagavad Gita",
        source="DharmicData (Gita)",
        dry_run=args.dry_run,
        dedupe=args.dedupe,
    )

    invoke_vedic_ingest(
        input_path=os.path.join(args.out, "rigveda"),
        veda="rigveda",
        source="DharmicData (Rigveda)",
        dry_run=args.dry_run,
        dedupe=args.dedupe,
    )
    invoke_vedic_ingest(
        input_path=os.path.join(args.out, "yajurveda"),
        veda="yajurveda",
        source="DharmicData (Yajurveda)",
        dry_run=args.dry_run,
        dedupe=args.dedupe,
    )
    invoke_vedic_ingest(
        input_path=os.path.join(args.out, "atharvaveda"),
        veda="atharvaveda",
        source="DharmicData (Atharvaveda)",
        dry_run=args.dry_run,
        dedupe=args.dedupe,
    )


if __name__ == "__main__":
    main()
