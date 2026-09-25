"""
Readers for the STEP catalog export and the team productivity exports.

The catalog export is a 100+ MB workbook. It is read with the calamine engine (Rust)
and every cell as text, which is ~30x faster than openpyxl and avoids pandas guessing
types differently from one day to the next. Numeric rules call pd.to_numeric
explicitly, exactly as before.
"""

from __future__ import annotations

import io
import os
from typing import BinaryIO, Union

import pandas as pd

Source = Union[str, os.PathLike, bytes, BinaryIO]

CATALOG_SHEET = "SKUs"


def _name_of(source: Source, name: str | None) -> str:
    if name:
        return name
    if isinstance(source, (str, os.PathLike)):
        return os.fspath(source)
    return getattr(source, "name", "") or ""


def _as_readable(source: Source):
    if isinstance(source, (bytes, bytearray)):
        return io.BytesIO(source)
    return source


def load_catalog(source: Source, name: str | None = None) -> pd.DataFrame:
    """
    Load a catalog snapshot (.xlsx, .csv or .parquet).

    Excel files are read from the 'SKUs' sheet when present, else the first sheet.
    Column names are stripped and upper-cased; SKU is stripped text.

    Raises:
        ValueError: unsupported extension or missing SKU column.
    """
    fname = _name_of(source, name)
    ext = os.path.splitext(fname)[1].lower()
    data = _as_readable(source)

    if ext == ".xlsx":
        xl = pd.ExcelFile(data, engine="calamine")
        sheet = CATALOG_SHEET if CATALOG_SHEET in xl.sheet_names else 0
        df = pd.read_excel(xl, sheet_name=sheet, dtype=str)
    elif ext == ".csv":
        df = pd.read_csv(data, dtype=str, encoding="utf-8-sig")
    elif ext == ".parquet":
        df = pd.read_parquet(data)
    else:
        raise ValueError(f"Unsupported file extension: {ext or '(none)'}. Use .xlsx, .csv or .parquet")

    df.columns = [str(c).strip().upper() for c in df.columns]
    if "SKU" not in df.columns:
        raise ValueError(f"'{fname}' must contain a 'SKU' column. Found columns: {list(df.columns)}")
    df["SKU"] = df["SKU"].astype(str).str.strip()
    return df


def load_productivity(source: Source) -> pd.DataFrame:
    """
    Load a Diseño/Edición productivity export.

    Column names are stripped but keep their case (e.g. '<ID>', '<Name>'); <ID> is
    stripped text so it joins against catalog SKUs.
    """
    df = pd.read_excel(_as_readable(source), engine="calamine")
    df.columns = [str(c).strip() for c in df.columns]
    if "<ID>" in df.columns:
        df["<ID>"] = df["<ID>"].astype(str).str.strip()
    return df
