"""Per-SKU content flags, the 0–100 content score and the catalog health KPIs."""

from __future__ import annotations

import pandas as pd

from .text import col, first_col, nonempty, yesno

SCORE_WEIGHTS = {
    "has_name": 10,
    "has_desc": 15,
    "has_brand": 5,
    "has_price": 15,
    "has_image": 25,
    "taxonomy_depth": 15,
    "is_visible": 10,
}


def build_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Boolean flags and weighted content score for each SKU.

    The result shares df's index, so it can be used as a mask source for df.
    """
    f = pd.DataFrame(index=df.index)
    f["SKU"] = df["SKU"]

    # Content flags
    f["has_name"] = nonempty(first_col(df, ["NOMBRE DE SKU", "NOMBRE DE PRODUCTO"]))
    f["has_desc"] = nonempty(col(df, "DESCRIPCION ERP"))
    f["has_brand"] = nonempty(col(df, "MARCA"))

    # Price: "TIENE PRECIO" flag or PRECIO value > 0
    has_price_flag = yesno(col(df, "TIENE PRECIO"))
    has_price_value = (pd.to_numeric(col(df, "PRECIO"), errors="coerce").fillna(0) > 0).astype(int)
    f["has_price"] = (has_price_flag | has_price_value).astype(int)

    # Image: any of the image columns
    has_image_flag = yesno(col(df, "TIENE IMAGEN"))
    has_primary_image = yesno(col(df, "IMAGEN PRIMARIA"))
    has_image_url = nonempty(col(df, "URL IMAGEN"))
    f["has_image"] = (has_image_flag | has_primary_image | has_image_url).astype(int)

    # Stock: STOCK value > 0 or "TIENE STOCK" flag
    has_stock_value = (pd.to_numeric(col(df, "STOCK"), errors="coerce").fillna(0) > 0).astype(int)
    has_stock_flag = yesno(col(df, "TIENE STOCK"))
    f["has_stock"] = (has_stock_value | has_stock_flag).astype(int)

    # Visibility and enabled status
    f["is_visible"] = yesno(col(df, "VISIBLE"))
    f["is_enabled"] = col(df, "HABILITADO/DESHABILITADO").astype(str).str.lower().str.startswith("habil").astype(int)

    # Taxonomy depth (levels 1-3)
    f["taxonomy_depth"] = (
        nonempty(col(df, "NIVEL 1")) +
        nonempty(col(df, "NIVEL 2")) +
        nonempty(col(df, "NIVEL 3"))
    )

    w = SCORE_WEIGHTS
    f["taxonomy_points"] = (f["taxonomy_depth"] * (w["taxonomy_depth"] / 3.0)).clip(upper=w["taxonomy_depth"])
    f["content_score"] = (
        f["has_name"] * w["has_name"] +
        f["has_desc"] * w["has_desc"] +
        f["has_brand"] * w["has_brand"] +
        f["has_price"] * w["has_price"] +
        f["has_image"] * w["has_image"] +
        f["taxonomy_points"] +
        f["is_visible"] * w["is_visible"]
    ).round(0).astype(int)

    return f


def build_summary(flags: pd.DataFrame) -> pd.DataFrame:
    """Single-row catalog health KPIs (the 'Catalog Health' sheet and history row)."""
    return pd.DataFrame([{
        "Total SKUs": len(flags),
        "Visible": int(flags["is_visible"].sum()),
        "Visible %": round(flags["is_visible"].mean() * 100, 2),
        "With Image %": round(flags["has_image"].mean() * 100, 2),
        "With Price %": round(flags["has_price"].mean() * 100, 2),
        "With Stock %": round(flags["has_stock"].mean() * 100, 2),
        "Avg Content Score": round(flags["content_score"].mean(), 2),
        "Score = 100": int((flags["content_score"] == 100).sum()),
    }])
