"""
Google Search Console: daily clicks, impressions and position per page URL, mapped to
SKUs through the product-page pattern. Final data lags 2–3 days; `dataState=all` also
returns fresh (provisional) rows.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .ga4 import path_to_sku

ROW_LIMIT = 25_000


@dataclass
class GSCClient:
    site: str
    credentials_info: dict

    def _service(self):
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build

        creds = Credentials.from_service_account_info(
            self.credentials_info, scopes=["https://www.googleapis.com/auth/webmasters.readonly"])
        return build("searchconsole", "v1", credentials=creds, cache_discovery=False)

    def pages_report(self, day: str) -> pd.DataFrame:
        service = self._service()
        rows, start = [], 0
        while True:
            body = {"startDate": day, "endDate": day, "dimensions": ["page"], "rowLimit": ROW_LIMIT,
                    "startRow": start, "dataState": "all"}
            resp = service.searchanalytics().query(siteUrl=self.site, body=body).execute()
            batch = resp.get("rows", [])
            rows += [{"page": r["keys"][0], "clicks": r["clicks"], "impressions": r["impressions"],
                      "ctr": r["ctr"], "position": r["position"]} for r in batch]
            if len(batch) < ROW_LIMIT:
                break
            start += ROW_LIMIT
        df = pd.DataFrame(rows, columns=["page", "clicks", "impressions", "ctr", "position"])
        df.insert(0, "day", day)
        df["SKU"] = df["page"].map(path_to_sku)
        return df
