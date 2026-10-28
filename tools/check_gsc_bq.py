"""Read-only check of the Search Console BigQuery export: access, rows, SKU coverage, bytes scanned.

    python3 tools/check_gsc_bq.py              # 3 days ago (the export lands ~2 days late)
    python3 tools/check_gsc_bq.py 2026-10-10
"""

from __future__ import annotations

import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.config import Settings  # noqa: E402
from pipeline.gsc_bq import GSCBigQuery, GSCNotReady  # noqa: E402


def main() -> int:
    s = Settings.from_env()
    creds = s.gsc_bq_service_account or s.google_service_account
    if not creds:
        print("Config: set GSC_BQ_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_JSON / GOOGLE_APPLICATION_CREDENTIALS")
        return 2
    day = sys.argv[1] if len(sys.argv) > 1 else (date.today() - timedelta(days=3)).isoformat()
    client = GSCBigQuery(s.gsc_bq_project, s.gsc_bq_dataset, s.gsc_bq_table, creds, s.gsc_bq_billing_project)
    print(f"Service account: {creds.get('client_email')}\nTable {client.full_table}, day {day}")
    try:
        pages = client.pages_report(day)
    except GSCNotReady:
        print(f"✗ No rows for {day} yet (the export lags ~2 days); try an earlier day.")
        return 1
    except Exception as exc:
        msg = str(exc)
        print(f"✗ BigQuery error: {msg[:400]}")
        if "403" in msg or "permission" in msg.lower() or "Access Denied" in msg:
            print(f"→ Grant the service account BigQuery Data Viewer on dataset {s.gsc_bq_dataset} and BigQuery Job User "
                  f"on project {s.gsc_bq_billing_project or s.gsc_bq_project}.")
        return 1
    prod = pages.dropna(subset=["SKU"])
    print(f"✓ {len(pages):,} URLs; {len(prod):,} product pages ({prod['SKU'].nunique():,} SKUs), "
          f"{int(prod['clicks'].sum()):,} clicks / {int(prod['impressions'].sum()):,} impressions on product pages; "
          f"{client.last_bytes_processed / 1e6:.0f} MB scanned")
    print(prod.sort_values("clicks", ascending=False).head(5)[["SKU", "clicks", "impressions", "position", "consultas"]]
          .to_string(index=False))
    queries = client.queries_report(day)
    print(f"✓ {len(queries):,} query rows ({queries['SKU'].nunique():,} SKUs, {int(queries['anonima'].sum()):,} anonymised "
          f"rows); {client.last_bytes_processed / 1e6:.0f} MB scanned")
    return 0


if __name__ == "__main__":
    sys.exit(main())
