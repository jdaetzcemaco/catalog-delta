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


INVENTORY_SHEET = "Inventarios"
# Virtual warehouse rows carry exactly this value since 2026-09-22 (bodega MDR); it is not stock
STOCK_PLACEHOLDER_VALUE = 1_000_000
STOCK_REAL, STOCK_PLACEHOLDER_COL, STOCK_BODEGAS = "STOCK REAL", "STOCK PLACEHOLDER", "BODEGAS CON STOCK"


def load_inventory_sheet(source: Source, name: str | None = None) -> pd.DataFrame | None:
    """
    Per-SKU real stock from the export's 'Inventarios' sheet (one column per warehouse):
    STOCK REAL = sum of warehouse cells that are not the placeholder value,
    STOCK PLACEHOLDER = sum of placeholder cells, BODEGAS CON STOCK = warehouses with stock.
    None when the workbook has no such sheet (CSV/Parquet sources, old exports).
    """
    fname = _name_of(source, name)
    if os.path.splitext(fname)[1].lower() != ".xlsx":
        return None
    xl = pd.ExcelFile(_as_readable(source), engine="calamine")
    if INVENTORY_SHEET not in xl.sheet_names:
        return None
    inv = pd.read_excel(xl, sheet_name=INVENTORY_SHEET)
    inv.columns = [str(c).strip() for c in inv.columns]
    sku_col = next((c for c in inv.columns if c.lower() == "sku"), None)
    if sku_col is None:
        return None
    warehouses = [c for c in inv.columns if c not in (sku_col, "ProductName", "Total")]
    num = inv[warehouses].apply(pd.to_numeric, errors="coerce").fillna(0)
    placeholder = num == STOCK_PLACEHOLDER_VALUE
    real = num.where(~placeholder, 0)
    out = pd.DataFrame({
        "SKU": inv[sku_col].astype(str).str.strip().str.replace(r"\.0$", "", regex=True),
        STOCK_REAL: real.sum(axis=1).astype(int),
        STOCK_PLACEHOLDER_COL: num.where(placeholder, 0).sum(axis=1).astype(int),
        STOCK_BODEGAS: (real > 0).sum(axis=1).astype(int),
    })
    return out.drop_duplicates("SKU")


def attach_inventory(catalog: pd.DataFrame, inventory: pd.DataFrame | None) -> pd.DataFrame:
    """Add the STOCK REAL columns to the catalog frame (left join on SKU; missing → 0)."""
    if inventory is None:
        return catalog
    out = catalog.merge(inventory, on="SKU", how="left")
    for c in (STOCK_REAL, STOCK_PLACEHOLDER_COL, STOCK_BODEGAS):
        out[c] = out[c].fillna(0).astype(int)
    return out
