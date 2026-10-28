"""
Google Search Console through its BigQuery bulk export (project cemaco-content-system,
dataset searchconsole, table searchdata_url_impression; rows since 2026-08-27).

Why not the Search Console API: the API's `page` report returned ~2,300 product SKUs and
63k impressions for 2026-10-10, the export has 19,000 SKUs and 175k impressions for the
same day, plus the query, device and rich-result flags. The export lags ~2 days.

Plain REST (jobs.query) with the service-account token from google-auth: no
google-cloud-bigquery dependency. Every query filters on one `data_date` partition
(≈35–50 MB scanned), and `maximumBytesBilled` stops a runaway query instead of billing it.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

import pandas as pd

from .ga4 import path_to_sku

BQ_URL = "https://bigquery.googleapis.com/bigquery/v2/projects/{project}"
SCOPES = ["https://www.googleapis.com/auth/bigquery.readonly"]
TOP_QUERIES_PER_URL = 25
MAX_BYTES_BILLED = 2_000_000_000
PRODUCT_URL_RE = r"-[0-9]{4,}/p/?$"

PAGES_SQL = """
SELECT url,
       SUM(impressions) AS impressions,
       SUM(clicks) AS clicks,
       SUM(sum_position) AS sum_position,
       SUM(IF(is_merchant_listings, impressions, 0)) AS impressions_ml,
       SUM(IF(is_product_snippets, impressions, 0)) AS impressions_ps,
       SUM(IF(device = 'MOBILE', impressions, 0)) AS impressions_mobile,
       COUNT(DISTINCT IF(is_anonymized_query, NULL, query)) AS consultas
FROM `{table}`
WHERE data_date = @day AND search_type = 'WEB'
GROUP BY url
"""

QUERIES_SQL = """
WITH rows_ AS (
  SELECT url,
         IF(is_anonymized_query, '(anónimas)', query) AS query,
         is_anonymized_query AS anonima,
         SUM(impressions) AS impressions,
         SUM(clicks) AS clicks,
         SUM(sum_position) AS sum_position,
         LOGICAL_OR(is_merchant_listings) AS ml,
         LOGICAL_OR(is_product_snippets) AS ps
  FROM `{table}`
  WHERE data_date = @day AND search_type = 'WEB' AND REGEXP_CONTAINS(url, r'{product_re}')
  GROUP BY url, query, anonima
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY url, anonima ORDER BY impressions DESC, clicks DESC) AS rn
  FROM rows_
)
SELECT url, query, anonima, impressions, clicks, sum_position, ml, ps
FROM ranked
WHERE anonima OR rn <= {top}
"""


class GSCNotReady(Exception):
    """The export has no rows for that day yet (it lands ~2 days late)."""


def rows_to_frame(schema: dict, rows: list[dict]) -> pd.DataFrame:
    """Typed frame from a jobs.query response (`schema.fields` + rows as {"f": [{"v": ...}]})."""
    fields = schema.get("fields", [])
    names = [f["name"] for f in fields]
    records = [[cell.get("v") for cell in row.get("f", [])] for row in rows]
    df = pd.DataFrame(records, columns=names)
    for f in fields:
        t = f.get("type", "STRING").upper()
        c = f["name"]
        if t in ("INTEGER", "INT64"):
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
        elif t in ("FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        elif t in ("BOOLEAN", "BOOL"):
            df[c] = df[c].map(lambda v: None if v is None else str(v).lower() == "true")
    return df


@dataclass
class GSCBigQuery:
    project: str
    dataset: str
    table: str
    credentials_info: dict
    billing_project: str = ""
    last_bytes_processed: int = 0

    @property
    def full_table(self) -> str:
        return f"{self.project}.{self.dataset}.{self.table}"

    def _session(self):
        from google.auth.transport.requests import AuthorizedSession
        from google.oauth2.service_account import Credentials

        creds = Credentials.from_service_account_info(self.credentials_info, scopes=SCOPES)
        return AuthorizedSession(creds)

    def _query(self, sql: str, params: dict[str, str]) -> pd.DataFrame:
        """Run a parameterised query and return every row (follows pageToken; retries 429/5xx)."""
        session = self._session()
        base = BQ_URL.format(project=self.billing_project or self.project)
        body = {
            "query": sql, "useLegacySql": False, "parameterMode": "NAMED", "timeoutMs": 60_000,
            "maxResults": 50_000, "maximumBytesBilled": str(MAX_BYTES_BILLED),
            "queryParameters": [{"name": k, "parameterType": {"type": "STRING"}, "parameterValue": {"value": v}}
                                for k, v in params.items()],
        }
        resp = self._post(session, f"{base}/queries", body)
        job = resp["jobReference"]
        location = job.get("location")
        results_url = f"{base}/queries/{job['jobId']}"
        while not resp.get("jobComplete", True):
            time.sleep(2)
            resp = self._get(session, results_url, {"location": location, "maxResults": 50_000, "timeoutMs": 60_000})
        schema, rows = resp.get("schema", {"fields": []}), list(resp.get("rows", []))
        token = resp.get("pageToken")
        while token:
            page = self._get(session, results_url, {"location": location, "maxResults": 50_000, "pageToken": token})
            rows += page.get("rows", [])
            token = page.get("pageToken")
        self.last_bytes_processed = int(resp.get("totalBytesProcessed", 0) or 0)
        return rows_to_frame(schema, rows)

    @staticmethod
    def _post(session, url: str, body: dict) -> dict:
        return GSCBigQuery._call(lambda: session.post(url, json=body, timeout=120))

    @staticmethod
    def _get(session, url: str, params: dict) -> dict:
        return GSCBigQuery._call(lambda: session.get(url, params={k: v for k, v in params.items() if v is not None},
                                                      timeout=120))

    @staticmethod
    def _call(fn) -> dict:
        for attempt in range(4):
            r = fn()
            if r.status_code in (429, 500, 502, 503, 504) and attempt < 3:
                time.sleep(2 ** attempt)
                continue
            if r.status_code >= 400:
                raise RuntimeError(f"BigQuery {r.status_code}: {r.text[:400]}")
            payload = r.json()
            if "error" in payload:
                raise RuntimeError(f"BigQuery: {payload['error'].get('message', payload['error'])}")
            return payload
        raise RuntimeError("BigQuery: too many retries")  # pragma: no cover

    # ── reports ─────────────────────────────────────────────────────────────
    def pages_report(self, day: str) -> pd.DataFrame:
        """
        Same columns as GSCClient.pages_report (day, page, clicks, impressions, ctr, position, SKU)
        plus impressions_ml / impressions_ps / impressions_mobile / consultas. WEB search only.
        """
        df = self._query(PAGES_SQL.format(table=self.full_table), {"day": day})
        if df.empty:
            raise GSCNotReady(day)
        impressions = df["impressions"].where(df["impressions"] > 0)
        out = pd.DataFrame({
            "day": day, "page": df["url"], "clicks": df["clicks"].astype(int), "impressions": df["impressions"].astype(int),
            "ctr": (df["clicks"] / impressions).fillna(0.0),
            # The export's sum_position is zero-based; the API's position is one-based
            "position": (df["sum_position"] / impressions + 1).round(2),
        })
        out["SKU"] = out["page"].map(path_to_sku)
        for c in ("impressions_ml", "impressions_ps", "impressions_mobile", "consultas"):
            out[c] = df[c].astype(int).values
        return out

    def queries_report(self, day: str) -> pd.DataFrame:
        """Product pages only: top queries per URL (plus one '(anónimas)' row) → day, SKU, query, ..."""
        df = self._query(QUERIES_SQL.format(table=self.full_table, product_re=PRODUCT_URL_RE, top=TOP_QUERIES_PER_URL),
                         {"day": day})
        cols = ["day", "SKU", "query", "impressions", "clicks", "position", "anonima", "ml", "ps"]
        if df.empty:
            return pd.DataFrame(columns=cols)
        df["SKU"] = df["url"].map(path_to_sku)
        df = df.dropna(subset=["SKU"]).copy()
        impressions = df["impressions"].where(df["impressions"] > 0)
        df["position"] = (df["sum_position"] / impressions + 1).round(2)
        df["day"] = day
        for c in ("anonima", "ml", "ps"):
            df[c] = df[c].fillna(False).astype(bool)
        return df[cols].reset_index(drop=True)
