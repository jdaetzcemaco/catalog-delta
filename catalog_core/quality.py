"""
Data-quality checks on the raw export. They never change a rule; they flag values
that make rules unreliable so the app can warn about them.
"""

from __future__ import annotations

import pandas as pd

from .text import col

# From 2026-09-22 the STEP export sets STOCK to 1,000,000 (or 1,000,000 + real stock)
# on most SKUs, with TIENE STOCK = Si. That is not real inventory.
PLACEHOLDER_STOCK = 1_000_000


def quality_checks(df: pd.DataFrame) -> dict:
    stock = pd.to_numeric(col(df, "STOCK"), errors="coerce")
    placeholder = int((stock >= PLACEHOLDER_STOCK).sum())
    corrected = "STOCK REAL" in df.columns
    out = {
        "stock_placeholder": placeholder,
        "stock_placeholder_pct": round(placeholder / len(df) * 100, 2) if len(df) else 0.0,
        # True when has_stock came from the Inventarios sheet (placeholder warehouse excluded)
        "stock_corrected": corrected,
        "stock_source": "inventarios" if corrected else "skus",
    }
    if corrected:
        real = pd.to_numeric(df["STOCK REAL"], errors="coerce").fillna(0)
        out["stock_real_pct"] = round(float((real > 0).mean()) * 100, 2) if len(df) else 0.0
    return out
