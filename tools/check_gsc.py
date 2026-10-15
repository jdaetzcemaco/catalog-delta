"""Read-only Search Console check: access, row counts and how many pages map to a SKU.

    python3 tools/check_gsc.py              # 4 days ago (GSC lags 2–3 days)
"""

from __future__ import annotations

import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.config import Settings  # noqa: E402
from pipeline.gsc import GSCClient  # noqa: E402


def main() -> int:
    s = Settings.from_env()
    if not s.google_service_account:
        print("Config: set GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_APPLICATION_CREDENTIALS")
        return 2
    day = sys.argv[1] if len(sys.argv) > 1 else (date.today() - timedelta(days=4)).isoformat()
    print(f"Service account: {s.google_service_account.get('client_email')}\nSite {s.gsc_site}, day {day}")
    try:
        pages = GSCClient(s.gsc_site, s.google_service_account).pages_report(day)
    except Exception as exc:
        msg = str(exc)
        print(f"✗ Search Console error: {msg[:400]}")
        if "403" in msg or "permission" in msg.lower():
            print(f"→ Add the service account as a user of {s.gsc_site} in Search Console (Settings → Users).")
        if "404" in msg or "not found" in msg.lower():
            print("→ The property name is wrong: try GSC_SITE=https://cemaco.com/ (URL-prefix) in .env.")
        return 1
    prod = pages.dropna(subset=["SKU"])
    print(f"✓ {len(pages):,} pages; {len(prod):,} are product pages ({prod['SKU'].nunique():,} SKUs), "
          f"{int(prod['clicks'].sum()):,} clicks / {int(prod['impressions'].sum()):,} impressions on product pages")
    print(prod.sort_values("clicks", ascending=False).head(5)[["SKU", "clicks", "impressions", "position"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
