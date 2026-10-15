"""
Per-product and per-SKU performance aggregates from the daily GA4 / Search Console rows.

GA4 identifies e-commerce items by PRODUCT ID (verified 2026-10-15: 100% match), so item
metrics (views, cart, purchases, revenue) live at product level and are attributed to
every SKU of the product. Landing-page sessions and Search Console rows carry the SKU in
the URL, so those are per SKU.
"""

from __future__ import annotations

import pandas as pd

WINDOWS = (7, 30, 90)
ITEM_COLS = {"itemsViewed": "vistas", "itemsAddedToCart": "carrito", "itemsPurchased": "compras", "itemRevenue": "ingresos"}
PRODUCT_METRICS = list(ITEM_COLS.values())
LANDING_METRICS = ["sessions", "organic_sessions"]
GSC_METRICS = ["clicks", "impressions"]


def daily_product_frame(items_by_day: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """One row per (day, product_id) with GA4 item metrics."""
    frames = []
    for day, items in items_by_day.items():
        if items is None or items.empty:
            continue
        d = items.rename(columns=ITEM_COLS)
        d = d.assign(product_id=d["itemId"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True))
        d = d.groupby("product_id", as_index=False)[PRODUCT_METRICS].sum()
        d.insert(0, "day", pd.Timestamp(day))
        frames.append(d)
    if not frames:
        return pd.DataFrame(columns=["day", "product_id", *PRODUCT_METRICS])
    return pd.concat(frames, ignore_index=True)


def daily_landing_frame(landing_by_day: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """One row per (day, SKU) with sessions and organic sessions to the product page."""
    from pipeline.ga4 import summarize_landing  # pure helper, no I/O

    frames = []
    for day, landing in landing_by_day.items():
        if landing is None or landing.empty or "SKU" not in landing.columns:
            continue
        d = summarize_landing(landing)[["SKU", *LANDING_METRICS]]
        d.insert(0, "day", pd.Timestamp(day))
        frames.append(d)
    if not frames:
        return pd.DataFrame(columns=["day", "SKU", *LANDING_METRICS])
    return pd.concat(frames, ignore_index=True)


def daily_gsc_frame(pages_by_day: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """One row per (day, SKU) with Search Console clicks, impressions and position."""
    frames = []
    for day, pages in pages_by_day.items():
        if pages is None or pages.empty or "SKU" not in pages.columns:
            continue
        d = pages.dropna(subset=["SKU"]).copy()
        d["pos_x_impr"] = d["position"] * d["impressions"]
        g = d.groupby("SKU", as_index=False).agg(clicks=("clicks", "sum"), impressions=("impressions", "sum"),
                                                 pos_x_impr=("pos_x_impr", "sum"))
        g["position"] = (g["pos_x_impr"] / g["impressions"].where(g["impressions"] > 0)).round(1)
        g = g.drop(columns="pos_x_impr")
        g.insert(0, "day", pd.Timestamp(day))
        frames.append(g)
    if not frames:
        return pd.DataFrame(columns=["day", "SKU", "clicks", "impressions", "position"])
    return pd.concat(frames, ignore_index=True)


def merge_days(existing: pd.DataFrame | None, fresh: pd.DataFrame, keep_days: int, as_of: str) -> pd.DataFrame:
    """Replace the days present in `fresh`, keep the rest, drop rows older than keep_days."""
    if existing is not None and not existing.empty:
        existing = existing.copy()
        existing["day"] = pd.to_datetime(existing["day"])
        out = pd.concat([existing[~existing["day"].isin(pd.to_datetime(fresh["day"]).unique())], fresh],
                        ignore_index=True)
    else:
        out = fresh
    cutoff = pd.Timestamp(as_of) - pd.Timedelta(days=keep_days)
    return out[pd.to_datetime(out["day"]) >= cutoff].reset_index(drop=True)


def rolling_aggregates(daily: pd.DataFrame, key: str, metrics: list[str], as_of: str,
                       trend_metric: str | None = None, activity_metric: str | None = None) -> pd.DataFrame:
    """Sums over the last 7/30/90 days ending at `as_of`, plus previous-week trend and last active day."""
    cols = [key, *[f"{m}_{w}d" for w in WINDOWS for m in metrics]]
    if daily.empty:
        return pd.DataFrame(columns=cols)
    daily = daily.copy()
    daily["day"] = pd.to_datetime(daily["day"])
    end = pd.Timestamp(as_of)
    parts = []
    for w in WINDOWS:
        win = daily[(daily["day"] > end - pd.Timedelta(days=w)) & (daily["day"] <= end)]
        agg = win.groupby(key)[metrics].sum()
        agg.columns = [f"{m}_{w}d" for m in metrics]
        parts.append(agg)
    if trend_metric:
        prev = daily[(daily["day"] > end - pd.Timedelta(days=14)) & (daily["day"] <= end - pd.Timedelta(days=7))]
        parts.append(prev.groupby(key)[trend_metric].sum().rename(f"{trend_metric}_7d_prev"))
    out = pd.concat(parts, axis=1).fillna(0)
    if activity_metric:
        last = daily[daily[activity_metric] > 0].groupby(key)["day"].max().rename(f"ultimo_dia_{activity_metric}")
        out = out.join(last)
    out.index.name = key
    out = out.reset_index()
    out["as_of"] = end
    return out


def product_aggregates(daily_product: pd.DataFrame, as_of: str) -> pd.DataFrame:
    out = rolling_aggregates(daily_product, "product_id", PRODUCT_METRICS, as_of, "vistas", "vistas")
    if not out.empty:
        out["tasa_carrito_30d"] = (out["carrito_30d"] / out["vistas_30d"].where(out["vistas_30d"] > 0)).round(4)
        out["tasa_compra_30d"] = (out["compras_30d"] / out["vistas_30d"].where(out["vistas_30d"] > 0)).round(4)
    return out


def sku_performance(sku_map: pd.DataFrame, product_perf: pd.DataFrame, landing_perf: pd.DataFrame | None,
                    gsc_perf: pd.DataFrame | None) -> pd.DataFrame:
    """
    Per-SKU view the app reads: product metrics attributed to each SKU of the product
    (with `skus_del_producto` so shared numbers are visible), plus SKU-level sessions and
    Search Console. `sku_map` has SKU and PRODUCT ID from the catalog snapshot.
    """
    m = sku_map[["SKU", "PRODUCT ID"]].dropna().copy()
    m["product_id"] = m["PRODUCT ID"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    m["skus_del_producto"] = m.groupby("product_id")["SKU"].transform("size")
    out = m[["SKU", "product_id", "skus_del_producto"]].merge(product_perf, on="product_id", how="left")
    for extra in (landing_perf, gsc_perf):
        if extra is not None and not extra.empty:
            out = out.merge(extra.drop(columns=["as_of"], errors="ignore"), on="SKU", how="left")
    num = [c for c in out.columns if c.endswith(("d", "d_prev")) and c not in ("ultimo_dia_vistas",)]
    out[num] = out[num].fillna(0)
    return out
