"""
Forget the Search Console days that were pulled through the API so the next pipeline passes
re-pull them from the BigQuery export (higher coverage). Only touches `gsc.days` in index.json;
the stored parquet files are overwritten day by day as the job re-pulls them.

    # Same STORAGE / GRAPH_* / LOCAL_ROOT variables as the job
    python tools/reset_gsc_days.py            # shows what would change
    python tools/reset_gsc_days.py --apply    # writes index.json
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.config import Settings  # noqa: E402
from pipeline.storage import make_storage  # noqa: E402
from pipeline.store import ResultStore  # noqa: E402


def days_to_forget(index: dict, start: str) -> list[str]:
    """Days on or after `start` that did not come from BigQuery."""
    gsc = index.get("gsc") or {}
    sources = gsc.get("sources") or {}
    return sorted(d for d in gsc.get("days", []) if d >= start and sources.get(d) != "bq")


def main(argv: list[str]) -> int:
    apply = "--apply" in argv
    s = Settings.from_env()
    try:
        store = ResultStore(make_storage(s), s.processed_dir)
    except ValueError as exc:
        print(f"Config: {exc}")
        return 2
    index = store.load_index()
    forget = days_to_forget(index, s.gsc_bq_start)
    stored = index["gsc"].get("days", [])
    print(f"Search Console days stored: {len(stored)} ({stored[0] if stored else '-'} … {stored[-1] if stored else '-'})")
    print(f"Days ≥ {s.gsc_bq_start} not from BigQuery: {len(forget)}")
    if not forget:
        print("Nothing to do.")
        return 0
    print(f"  {forget[0]} … {forget[-1]}  → will be re-pulled from BigQuery over ~{-(-len(forget) // 30)} passes")
    if not apply:
        print("Dry run. Re-run with --apply to write index.json.")
        return 0
    keep = set(stored) - set(forget)
    index["gsc"]["days"] = sorted(keep)
    for day in forget:
        index["gsc"].get("sources", {}).pop(day, None)
    index["gsc"]["as_of"] = max(keep) if keep else ""
    store.save_index(index)
    print(f"index.json updated: {len(keep)} days kept, {len(forget)} forgotten.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
