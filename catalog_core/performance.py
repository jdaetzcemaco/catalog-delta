"""Per-SKU performance aggregates from daily GA4 rows (pure; the job feeds and stores them)."""

from __future__ import annotations

import pandas as pd

WINDOWS = (7, 30, 90)
ITEM_COLS = {"itemsViewed": "vistas", "itemsAddedToCart": "carrito", "itemsPurchased": "compras", "itemRevenue": "ingresos"}


def daily_sku_frame(items_by_day: dict[str, pd.DataFrame], landing_by_day: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """One row per (day, SKU) with item metrics and landing sessions, from the daily reports."""
    from pipeline.ga4 import summarize_landing  # pure helper, no I/O

    frames = []
    for day, items in items_by_day.items():
        if items is None or items.empty:
            continue
        d = items.rename(columns=ITEM_COLS).copy()
        d["SKU"] = d["itemId"].astype(str).str.strip()
        d = d.groupby("SKU", as_index=False)[list(ITEM_COLS.values())].sum()
        landing = landing_by_day.get(day)
        if landing is not None and not landing.empty and "SKU" in landing.columns:
            d = d.merge(summarize_landing(landing)[["SKU", "sessions", "organic_sessions"]], on="SKU", how="outer")
        d["day"] = pd.Timestamp(day)
        frames.append(d)
    if not frames:
        return pd.DataFrame(columns=["day", "SKU", *ITEM_COLS.values(), "sessions", "organic_sessions"])
    out = pd.concat(frames, ignore_index=True)
    for c in [*ITEM_COLS.values(), "sessions", "organic_sessions"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0) if c in out.columns else 0.0
    return out


def rolling_aggregates(daily: pd.DataFrame, as_of: str) -> pd.DataFrame:
    """
    Per SKU: sums over the last 7/30/90 days ending at `as_of`, plus the previous-7-day
    window for trend (vistas_7d_prev) and the last day with any view.
    """
    if daily.empty:
        return pd.DataFrame(columns=["SKU"])
    end = pd.Timestamp(as_of)
    metrics = [*ITEM_COLS.values(), "sessions", "organic_sessions"]
    parts = []
    for w in WINDOWS:
        win = daily[(daily["day"] > end - pd.Timedelta(days=w)) & (daily["day"] <= end)]
        agg = win.groupby("SKU")[metrics].sum()
        agg.columns = [f"{m}_{w}d" for m in metrics]
        parts.append(agg)
    prev = daily[(daily["day"] > end - pd.Timedelta(days=14)) & (daily["day"] <= end - pd.Timedelta(days=7))]
    parts.append(prev.groupby("SKU")[["vistas", "organic_sessions"]].sum().rename(
        columns={"vistas": "vistas_7d_prev", "organic_sessions": "organic_sessions_7d_prev"}))
    seen = daily[daily["vistas"] > 0].groupby("SKU")["day"].max().rename("ultima_vista")
    parts.append(seen)
    out = pd.concat(parts, axis=1).fillna(0).reset_index().rename(columns={"index": "SKU"})
    out.loc[out["ultima_vista"] == 0, "ultima_vista"] = pd.NaT
    out["as_of"] = end
    # Conversion helpers (guarded)
    out["tasa_carrito_30d"] = (out["carrito_30d"] / out["vistas_30d"].where(out["vistas_30d"] > 0)).round(4)
    out["tasa_compra_30d"] = (out["compras_30d"] / out["vistas_30d"].where(out["vistas_30d"] > 0)).round(4)
    return out
