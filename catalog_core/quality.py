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
    return {
        "stock_placeholder": placeholder,
        "stock_placeholder_pct": round(placeholder / len(df) * 100, 2) if len(df) else 0.0,
    }
