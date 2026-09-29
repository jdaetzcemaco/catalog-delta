"""
Read-only: explain a jump in "With Stock %" between processed days.

    python3 tools/diagnose_stock.py            # uses the same env vars as the job
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd  # noqa: E402

from catalog_core.text import YES_VALUES  # noqa: E402
from pipeline.config import Settings  # noqa: E402
from pipeline.storage import make_storage  # noqa: E402
from pipeline.store import ResultStore  # noqa: E402


def describe(day: str, df: pd.DataFrame) -> None:
    stock = pd.to_numeric(df["STOCK"], errors="coerce")
    flag = df["TIENE STOCK"].astype(str).str.strip().str.lower().isin(YES_VALUES)
    print(f"\n── {day}: {len(df):,} SKUs")
    print(f"   STOCK > 0: {(stock.fillna(0) > 0).mean():.2%}   STOCK not numeric: {stock.isna().mean():.2%}")
    print(f"   TIENE STOCK = yes: {flag.mean():.2%}")
    print("   TIENE STOCK values:", df["TIENE STOCK"].value_counts(dropna=False).head(6).to_dict())
    print("   STOCK sample values:", df["STOCK"].value_counts(dropna=False).head(8).to_dict())
    print("   STOCK>0 vs TIENE STOCK:\n", pd.crosstab(stock.fillna(0) > 0, flag,
                                                     rownames=["STOCK>0"], colnames=["TIENE STOCK"]).to_string())


def main() -> int:
    s = Settings.from_env()
    store = ResultStore(make_storage(s), s.processed_dir)
    index = store.load_index()["catalog"]
    days = sorted(index)
    rows = []
    for d in days:
        m = store.load_manifest(d)
        rows.append({"day": d, "with_stock_%": m["summary"]["With Stock %"], "total": m["summary"]["Total SKUs"],
                     "processed_at": index[d].get("processed_at", "")[:16]})
    hist = pd.DataFrame(rows)
    print(hist.to_string(index=False))

    jumps = hist["with_stock_%"].diff().abs()
    if jumps.dropna().empty:
        return 0
    i = int(jumps.idxmax())
    before, after = hist.loc[i - 1, "day"], hist.loc[i, "day"]
    print(f"\nBiggest jump: {before} → {after}")
    cols = ["SKU", "STOCK", "TIENE STOCK"]
    for d in (before, after):
        describe(d, store.load_snapshot(d, columns=cols))
    return 0


if __name__ == "__main__":
    sys.exit(main())
