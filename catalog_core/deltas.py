"""Day-over-day comparison of two flagged snapshots."""

from __future__ import annotations

import pandas as pd

VIS_COLS = ["SKU", "content_score_today", "is_visible_today", "is_visible_yesterday"]
SNAPSHOT_COLS = ["SKU", "content_score", "is_visible", "has_image", "has_price", "has_stock"]


def compute_deltas(today: pd.DataFrame, yesterday: pd.DataFrame) -> pd.DataFrame:
    """Outer-join today/yesterday flags on SKU and derive change flags."""
    merged = today.merge(yesterday, on="SKU", suffixes=("_today", "_yesterday"), how="outer")

    merged["delta_score"] = merged["content_score_today"].fillna(0) - merged["content_score_yesterday"].fillna(0)

    # Changes only count for SKUs that exist on both days (not new, not removed)
    exists_both = merged["content_score_today"].notna() & merged["content_score_yesterday"].notna()

    vis_today = merged["is_visible_today"].fillna(0).astype(int)
    vis_yesterday = merged["is_visible_yesterday"].fillna(0).astype(int)
    merged["newly_visible"] = exists_both & (vis_today == 1) & (vis_yesterday == 0)
    merged["no_longer_visible"] = exists_both & (vis_today == 0) & (vis_yesterday == 1)

    # -1 for missing so a missing value never equals 0/1
    def changed(flag: str) -> pd.Series:
        t = merged[f"{flag}_today"].fillna(-1).astype(int)
        y = merged[f"{flag}_yesterday"].fillna(-1).astype(int)
        return exists_both & (t != y)

    merged["image_changed"] = changed("has_image")
    merged["price_changed"] = changed("has_price")
    merged["stock_flipped"] = changed("has_stock")
    merged["score_changed"] = exists_both & (merged["delta_score"].abs() >= 10)

    return merged


def sku_changes(today: pd.DataFrame, yesterday: pd.DataFrame) -> dict:
    """New/removed SKU sets and the net change in catalog size."""
    skus_today = set(today["SKU"])
    skus_yesterday = set(yesterday["SKU"])
    return {
        "new": skus_today - skus_yesterday,
        "removed": skus_yesterday - skus_today,
        "net": len(today) - len(yesterday),
    }


def _pick(merged: pd.DataFrame, cond: pd.Series, cols: list[str]) -> pd.DataFrame:
    if cond.any():
        return merged.loc[cond, cols].reset_index(drop=True)
    return pd.DataFrame(columns=cols)


def change_tables(today: pd.DataFrame, yesterday: pd.DataFrame, merged: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """The per-change detail tables, keyed by their sheet name."""
    changes = sku_changes(today, yesterday)
    return {
        "New SKUs": today[today["SKU"].isin(changes["new"])][SNAPSHOT_COLS].reset_index(drop=True),
        "Removed SKUs": yesterday[yesterday["SKU"].isin(changes["removed"])][SNAPSHOT_COLS].reset_index(drop=True),
        "No Longer Visible": _pick(merged, merged["no_longer_visible"], VIS_COLS),
        "Newly Visible": _pick(merged, merged["newly_visible"], VIS_COLS),
        "Image Changes": _pick(merged, merged["image_changed"], ["SKU", "has_image_today", "has_image_yesterday"]),
        "Price Changes": _pick(merged, merged["price_changed"], ["SKU", "has_price_today", "has_price_yesterday"]),
        "Stock Flips": _pick(merged, merged["stock_flipped"], ["SKU", "has_stock_today", "has_stock_yesterday"]),
        "Score Changes": _pick(
            merged, merged["score_changed"],
            ["SKU", "content_score_today", "content_score_yesterday", "delta_score"],
        ),
        # Visible, in stock, but weak content: the 50 worst first
        "Top Priorities": today.loc[
            (today["content_score"] < 80) & (today["is_visible"] == 1) & (today["has_stock"] == 1)
        ].sort_values("content_score").head(50).reset_index(drop=True),
        "Stock Not Visible": today.loc[
            (today["has_stock"] == 1) & (today["is_visible"] == 0)
        ][["SKU", "content_score", "has_image", "has_price", "has_stock"]]
        .sort_values("content_score", ascending=False).reset_index(drop=True),
    }
