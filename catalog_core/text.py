"""Column access and value-normalisation helpers shared by every rule."""

from __future__ import annotations

import pandas as pd

# Recognized "yes" values for boolean fields
YES_VALUES = {"si", "sí", "yes", "true", "1"}


def col(df: pd.DataFrame, name: str, default: str = "") -> pd.Series:
    """Column if present, otherwise a Series of `default` aligned to df."""
    if name in df.columns:
        return df[name]
    return pd.Series([default] * len(df), index=df.index)


def first_col(df: pd.DataFrame, names: list[str], default: str = "") -> pd.Series:
    """First column from `names` that exists in df."""
    for name in names:
        if name in df.columns:
            return df[name]
    return pd.Series([default] * len(df), index=df.index)


def yesno(series: pd.Series) -> pd.Series:
    """1 where the value is a recognized "yes" (si, sí, yes, true, 1), else 0."""
    s = series.astype(str).str.strip().str.lower()
    return s.isin(YES_VALUES).astype(int)


def nonempty(series: pd.Series) -> pd.Series:
    """1 where the value is not NaN and not blank, else 0."""
    return (~series.isna() & (series.astype(str).str.strip() != "")).astype(int)


def lower(series: pd.Series) -> pd.Series:
    """Stripped, lower-cased text; NaN stays NaN so it never matches a label."""
    return series.astype("string").str.strip().str.lower()


def select(df: pd.DataFrame, mask: pd.Series, cols: list[str] | None = None) -> pd.DataFrame:
    """Rows matching mask, limited to the columns in `cols` that exist."""
    out = df[mask.fillna(False).astype(bool)]
    if cols is not None:
        out = out[[c for c in cols if c in out.columns]]
    return out.reset_index(drop=True)
