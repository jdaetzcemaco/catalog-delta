"""
Read-only GA4 check: confirms the service account can read the property and whether
GA4 `itemId` is the catalog SKU (vs PRODUCT ID), plus how many landing pages map to a SKU.

    python3 tools/check_ga4.py              # uses .env: GA4_PROPERTY_ID + credentials
    python3 tools/check_ga4.py 2026-10-10   # a specific day (default: 3 days ago)
"""

from __future__ import annotations

import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd  # noqa: E402

from pipeline.config import Settings  # noqa: E402
from pipeline.ga4 import GA4Client, summarize_landing  # noqa: E402
from pipeline.storage import make_storage  # noqa: E402
from pipeline.store import ResultStore  # noqa: E402


def main() -> int:
    s = Settings.from_env()
    if not s.ga4_property_id or not s.google_service_account:
        print("Config: set GA4_PROPERTY_ID and GOOGLE_SERVICE_ACCOUNT_JSON (or GOOGLE_APPLICATION_CREDENTIALS=path)")
        return 2
    day = sys.argv[1] if len(sys.argv) > 1 else (date.today() - timedelta(days=3)).isoformat()
    print(f"Service account: {s.google_service_account.get('client_email')}")
    print(f"Property {s.ga4_property_id}, day {day}")
    ga = GA4Client(s.ga4_property_id, s.google_service_account)
    try:
        items = ga.items_report(day)
    except Exception as exc:
        msg = str(exc)
        print(f"✗ GA4 error: {msg[:400]}")
        if "PERMISSION_DENIED" in msg or "403" in msg:
            print("→ Add the service account e-mail above as Viewer in GA4 → Admin → Property access management.")
        return 1
    print(f"✓ Items report: {len(items):,} rows, quota tokens left today: {items.attrs.get('quota_tokens_remaining')}"
          + ("  (thresholded)" if items.attrs.get("thresholded") else ""))
    print(items.sort_values("itemsViewed", ascending=False).head(5).to_string(index=False))

    # Join against the catalog to see what itemId is
    try:
        store = ResultStore(make_storage(s), s.processed_dir)
        day_cat = store.run_days()[-1]
        snap = store.load_snapshot(day_cat, columns=["SKU", "PRODUCT ID"])
        skus = set(snap["SKU"].astype(str))
        var_rate = items["SKU"].isin(skus).mean()
        pid_rate = items["itemId"].astype(str).str.strip().isin(
            set(snap["PRODUCT ID"].astype(str).str.replace(r"\.0$", "", regex=True))).mean()
        print(f"\nvs catalog {day_cat}: itemVariant matches SKU {var_rate:.1%}, itemId matches PRODUCT ID {pid_rate:.1%}")
        print("→ OK: item metrics will be stored per SKU" if var_rate >= 0.8 else
              "→ itemVariant is NOT the SKU: check the GA4 e-commerce tagging")
    except Exception as exc:
        print(f"(catalog join skipped: {exc})")

    landing = ga.landing_report(day)
    mapped = landing["SKU"].notna().mean() if len(landing) else 0
    prod_sessions = landing.loc[landing["SKU"].notna(), "sessions"].sum()
    print(f"\n✓ Landing report: {len(landing):,} rows; {mapped:.1%} of landing rows are product pages "
          f"({int(prod_sessions):,} sessions). Channel groups: {sorted(landing['sessionDefaultChannelGroup'].unique())[:8]}")
    print(summarize_landing(landing).sort_values("organic_sessions", ascending=False).head(5).to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
