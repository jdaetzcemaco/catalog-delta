"""
Read-only: compare the STEP export's `Inventarios` sheet (Total per SKU) and the SKUs
sheet STOCK with VTEX's sellable quantity, on a sample of SKUs.

    python3 tools/check_vtex_stock.py path/to/catalog-daily-YYYY-MM-DD.xlsx [n_per_group]
    python3 tools/check_vtex_stock.py --latest            # downloads the newest export from OneDrive
"""

from __future__ import annotations

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd  # noqa: E402

from pipeline.config import Settings  # noqa: E402
from pipeline.process import latest_per_day  # noqa: E402
from pipeline.storage import make_storage  # noqa: E402
from pipeline.vtex import VTEXClient  # noqa: E402


def main() -> int:
    s = Settings.from_env()
    if not s.vtex_account:
        print("Config: set VTEX_ACCOUNT (and optionally VTEX_APP_KEY / VTEX_APP_TOKEN)")
        return 2
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    n = int(args[1]) if len(args) > 1 else 10
    tmpdir = None
    if "--latest" in sys.argv:
        storage = make_storage(s)
        files = latest_per_day(storage.list(s.incoming_dir, s.catalog_pattern))
        day, f = list(files.items())[-1]
        tmpdir = tempfile.mkdtemp()
        path = os.path.join(tmpdir, f.name)
        print(f"Downloading {f.name} ({f.size / 1e6:.0f} MB)…")
        storage.download_to(f.path, path)
    else:
        path = args[0]
    xl = pd.ExcelFile(path, engine="calamine")
    print("Sheets:", xl.sheet_names)
    skus = pd.read_excel(xl, sheet_name="SKUs", dtype=str)[["SKU", "STOCK", "TIENE STOCK", "VISIBLE", "PRECIO"]]
    skus["SKU"] = skus["SKU"].str.strip()
    inv = pd.read_excel(xl, sheet_name="Inventarios", dtype=str) if "Inventarios" in xl.sheet_names else None
    if inv is None:
        print("✗ No 'Inventarios' sheet in this export")
        return 1
    inv = inv.rename(columns={"Sku": "SKU"})
    inv["SKU"] = inv["SKU"].astype(str).str.strip()
    inv["Total"] = pd.to_numeric(inv["Total"], errors="coerce").fillna(0)
    warehouses = [c for c in inv.columns if c not in ("SKU", "ProductName", "Total")]
    print(f"Inventarios: {len(inv):,} SKUs, {len(warehouses)} warehouse columns, Total>0 for {(inv['Total'] > 0).mean():.1%}")
    both = skus.merge(inv[["SKU", "Total"]], on="SKU", how="left")
    stock_num = pd.to_numeric(both["STOCK"], errors="coerce")
    print(f"SKUs sheet: STOCK >= 1,000,000 on {(stock_num >= 1_000_000).mean():.1%}; "
          f"Inventarios.Total missing for {both['Total'].isna().mean():.1%} of SKUs")
    both["Total"] = both["Total"].fillna(0)
    agree = ((both["Total"] > 0) == (stock_num.fillna(0) > 0)).mean()
    print(f"Inventarios.Total>0 agrees with SKUs.STOCK>0 on {agree:.1%} of SKUs (placeholder rows count as >0)")

    vis = both[both["VISIBLE"].str.lower().eq("si")]
    sample = pd.concat([
        vis[vis["Total"] > 0].sample(min(n, (vis["Total"] > 0).sum()), random_state=1),
        vis[vis["Total"] == 0].sample(min(n, (vis["Total"] == 0).sum()), random_state=2),
    ])
    client = VTEXClient(s.vtex_account, s.vtex_app_key, s.vtex_app_token)
    rows = []
    for _, r in sample.iterrows():
        try:
            offer = client.sku_offer(r["SKU"])
        except Exception as exc:
            print(f"✗ VTEX error on SKU {r['SKU']}: {str(exc)[:200]}")
            return 1
        rows.append({"SKU": r["SKU"], "Inventarios.Total": int(r["Total"]), "STOCK (SKUs)": r["STOCK"],
                     "VTEX available": None if offer is None else offer["available"],
                     "VTEX price": None if offer is None else offer["price"], "PRECIO": r["PRECIO"]})
    out = pd.DataFrame(rows)
    print(out.to_string(index=False))
    known = out.dropna(subset=["VTEX available"])
    if len(known):
        match = ((known["Inventarios.Total"] > 0) == (known["VTEX available"] > 0)).mean()
        exact = (known["Inventarios.Total"] == known["VTEX available"]).mean()
        print(f"\n→ Inventarios.Total>0 vs VTEX available>0 agree on {match:.0%} of {len(known)} SKUs; exact quantity on {exact:.0%}")
    print(f"→ {out['VTEX available'].isna().sum()} SKUs not found in VTEX search (likely not visible/indexed)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
