"""
Google search queries over a rolling window, from the daily Search Console query rows
(gsc/<day>/queries.parquet: day, SKU, query, impressions, clicks, position, anonima).

Two small tables the app can read in one go:
  per SKU   the top queries of each SKU (impressions, clicks, average position, days seen)
  site      the top queries across all product pages, with how many SKUs they reach

Anonymised queries ("(anónimas)") are not a query; they are kept only as a per-SKU
total so the share of hidden traffic is known.
"""

from __future__ import annotations

import pandas as pd

TOP_PER_SKU = 15
TOP_SITE = 3000
_KEYS = ["SKU", "query"]


def _partial(day: pd.DataFrame) -> pd.DataFrame:
    """One day reduced to SKU × query sums (impressions-weighted position)."""
    d = day[["SKU", "query", "impressions", "clicks", "position", "anonima"]].copy()
    d["SKU"] = d["SKU"].astype(str)
    d.loc[d["anonima"].fillna(False).astype(bool), "query"] = "(anónimas)"
    d["impressions"] = pd.to_numeric(d["impressions"], errors="coerce").fillna(0)
    d["clicks"] = pd.to_numeric(d["clicks"], errors="coerce").fillna(0)
    d["pos_w"] = pd.to_numeric(d["position"], errors="coerce").fillna(0) * d["impressions"]
    g = d.groupby(_KEYS, as_index=False)[["impressions", "clicks", "pos_w"]].sum()
    g["dias"] = 1
    return g


def _merge(parts: list[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(parts, ignore_index=True).groupby(_KEYS, as_index=False)[
        ["impressions", "clicks", "pos_w", "dias"]].sum()


def aggregate_queries(days) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    `days`: iterable of daily query frames (loaded one at a time, so memory stays at one
    day plus the running total). Returns (per_sku, site).
    """
    acc, parts = None, []
    for day in days:
        if day is None or day.empty:
            continue
        parts.append(_partial(day))
        if len(parts) >= 7:                       # fold weekly to keep the running total small
            acc = _merge([acc, *parts] if acc is not None else parts)
            parts = []
    if parts:
        acc = _merge([acc, *parts] if acc is not None else parts)
    empty_sku = pd.DataFrame(columns=["SKU", "query", "impresiones", "clics", "posicion", "dias", "ctr"])
    empty_site = pd.DataFrame(columns=["query", "impresiones", "clics", "posicion", "skus", "ctr"])
    if acc is None or acc.empty:
        return empty_sku, empty_site

    acc["posicion"] = (acc["pos_w"] / acc["impressions"].where(acc["impressions"] > 0)).round(1)
    acc = acc.rename(columns={"impressions": "impresiones", "clicks": "clics"}).drop(columns="pos_w")
    acc["ctr"] = (acc["clics"] / acc["impresiones"].where(acc["impresiones"] > 0)).round(4)

    anon = acc[acc["query"] == "(anónimas)"]
    named = acc[acc["query"] != "(anónimas)"]
    per_sku = (named.sort_values(["SKU", "impresiones"], ascending=[True, False])
               .groupby("SKU", group_keys=False).head(TOP_PER_SKU))
    # The hidden share per SKU, as one extra row
    per_sku = pd.concat([per_sku, anon], ignore_index=True).sort_values(["SKU", "impresiones"], ascending=[True, False])

    w = named.assign(pos_w=named["posicion"].fillna(0) * named["impresiones"])
    site = w.groupby("query", as_index=False).agg(impresiones=("impresiones", "sum"), clics=("clics", "sum"),
                                                  pos_w=("pos_w", "sum"), skus=("SKU", "nunique"))
    site["posicion"] = (site["pos_w"] / site["impresiones"].where(site["impresiones"] > 0)).round(1)
    site["ctr"] = (site["clics"] / site["impresiones"].where(site["impresiones"] > 0)).round(4)
    site = site.drop(columns="pos_w").sort_values("impresiones", ascending=False).head(TOP_SITE)
    return per_sku.reset_index(drop=True), site.reset_index(drop=True)
