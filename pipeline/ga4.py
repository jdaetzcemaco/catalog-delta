"""
Google Analytics 4 Data API: daily item-scoped e-commerce metrics and landing-page
sessions by channel, mapped to catalog SKUs.

Two reports per day (item metrics cannot be mixed with session-scoped dimensions):
  items:   itemId, itemName × itemsViewed, itemsAddedToCart, itemsPurchased, itemRevenue
  landing: landingPagePlusQueryString, sessionDefaultChannelGroup × sessions, engagedSessions
Product URLs on the VTEX store look like https://cemaco.com/<slug>-<sku>/p, so the
landing path maps to a SKU with a regex on the trailing number.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import pandas as pd

PRODUCT_PATH_RE = re.compile(r"-(\d{4,})/p/?$")
ITEM_METRICS = ["itemsViewed", "itemsAddedToCart", "itemsPurchased", "itemRevenue"]
LANDING_METRICS = ["sessions", "engagedSessions"]
PAGE_LIMIT = 250_000


def path_to_sku(url_or_path: str) -> str | None:
    """'/cortina-bano-830548/p?x=1' -> '830548'; None when it is not a product page."""
    if not isinstance(url_or_path, str):
        return None
    path = url_or_path.split("?", 1)[0].split("#", 1)[0]
    m = PRODUCT_PATH_RE.search(path)
    return m.group(1) if m else None


@dataclass
class GA4Client:
    property_id: str
    credentials_info: dict

    def _client(self):
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.oauth2.service_account import Credentials

        creds = Credentials.from_service_account_info(
            self.credentials_info, scopes=["https://www.googleapis.com/auth/analytics.readonly"])
        return BetaAnalyticsDataClient(credentials=creds)

    def _run(self, dimensions: list[str], metrics: list[str], day: str, dimension_filter=None) -> pd.DataFrame:
        from google.analytics.data_v1beta.types import (DateRange, Dimension, Metric, RunReportRequest)

        client = self._client()
        rows, offset, quota = [], 0, None
        while True:
            req = RunReportRequest(
                property=f"properties/{self.property_id}",
                dimensions=[Dimension(name=d) for d in dimensions],
                metrics=[Metric(name=m) for m in metrics],
                date_ranges=[DateRange(start_date=day, end_date=day)],
                limit=PAGE_LIMIT, offset=offset, return_property_quota=True,
                dimension_filter=dimension_filter,
            )
            resp = client.run_report(req)
            quota = resp.property_quota
            for r in resp.rows:
                rows.append([v.value for v in r.dimension_values] + [v.value for v in r.metric_values])
            offset += len(resp.rows)
            if offset >= resp.row_count or not resp.rows:
                break
        df = pd.DataFrame(rows, columns=dimensions + metrics)
        for m in metrics:
            df[m] = pd.to_numeric(df[m], errors="coerce").fillna(0)
        df.attrs["quota_tokens_remaining"] = getattr(getattr(quota, "tokens_per_day", None), "remaining", None)
        df.attrs["thresholded"] = bool(getattr(getattr(resp, "metadata", None), "subject_to_thresholding", False))
        return df

    def items_report(self, day: str) -> pd.DataFrame:
        df = self._run(["itemId", "itemName"], ITEM_METRICS, day)
        df.insert(0, "day", day)
        return df

    def landing_report(self, day: str) -> pd.DataFrame:
        df = self._run(["landingPagePlusQueryString", "sessionDefaultChannelGroup"], LANDING_METRICS, day)
        df.insert(0, "day", day)
        df["SKU"] = df["landingPagePlusQueryString"].map(path_to_sku)
        return df


def summarize_landing(landing: pd.DataFrame) -> pd.DataFrame:
    """Per SKU: total sessions and organic-search sessions for one day."""
    prod = landing.dropna(subset=["SKU"])
    organic = prod["sessionDefaultChannelGroup"].str.lower().eq("organic search")
    out = prod.groupby("SKU").agg(sessions=("sessions", "sum"), engaged=("engagedSessions", "sum"))
    out["organic_sessions"] = prod[organic].groupby("SKU")["sessions"].sum()
    return out.fillna(0).reset_index()
