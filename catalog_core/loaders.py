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


def sku_from_erp(df: pd.DataFrame) -> pd.DataFrame:
    """
    The SKU of a productivity report is 'Sku ERP' without its leading zeros
    ('000000000001235140' → '1235140'); <ID> now carries another system's ID
    ('catgo-…'). The result goes in <ID>, so every consumer keeps joining on it;
    rows without a Sku ERP keep their <ID>. Reports without the column are unchanged.
    """
    erp = next((c for c in df.columns if str(c).strip().lower() == "sku erp"), None)
    if erp is None or "<ID>" not in df.columns:
        return df
    sku = (df[erp].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
           .str.lstrip("0").where(lambda s: s.str.fullmatch(r"\d+", na=False)))
    df["<ID>"] = sku.fillna(df["<ID>"].astype(str).str.strip())
    return df


def load_productivity(source: Source) -> pd.DataFrame:
    """
    Load a Diseño/Edición productivity export.

    Column names are stripped but keep their case (e.g. '<ID>', '<Name>'); <ID> is
    stripped text so it joins against catalog SKUs, taken from 'Sku ERP' when the
    report has it (see `sku_from_erp`).
    """
    data = _as_readable(source)
    # 'Sku ERP' must be read as text: as a number, 18-digit codes lose their last digit
    header = pd.read_excel(data, engine="calamine", nrows=0).columns
    text = {c: str for c in header if str(c).strip().lower() == "sku erp"}
    if hasattr(data, "seek"):
        data.seek(0)
    df = pd.read_excel(data, engine="calamine", dtype=text or None)
    df.columns = [str(c).strip() for c in df.columns]
    if "<ID>" in df.columns:
        df["<ID>"] = df["<ID>"].astype(str).str.strip()
    return sku_from_erp(df)


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

    The sheet is ~220k rows × 145 warehouses; it is streamed row by row with calamine
    and reduced on the fly instead of materialising a 32M-cell frame (which needs > 4 GB).
    """
    fname = _name_of(source, name)
    if os.path.splitext(fname)[1].lower() != ".xlsx":
        return None
    from python_calamine import CalamineWorkbook

    data = _as_readable(source)
    wb = CalamineWorkbook.from_path(os.fspath(data)) if isinstance(data, (str, os.PathLike)) \
        else CalamineWorkbook.from_filelike(data)
    if INVENTORY_SHEET not in wb.sheet_names:
        return None
    rows = wb.get_sheet_by_name(INVENTORY_SHEET).iter_rows()
    header = [str(c).strip() for c in next(rows, [])]
    if not header:
        return None
    sku_idx = next((i for i, c in enumerate(header) if c.lower() == "sku"), None)
    if sku_idx is None:
        return None
    skip = {sku_idx} | {i for i, c in enumerate(header) if c in ("ProductName", "Total")}
    wh_idx = [i for i in range(len(header)) if i not in skip]

    skus, real, placeholder, bodegas = [], [], [], []
    for row in rows:
        if sku_idx >= len(row) or row[sku_idx] in (None, ""):
            continue
        sku = row[sku_idx]
        sku = str(int(sku)) if isinstance(sku, float) and sku.is_integer() else str(sku).strip()
        r = p = b = 0
        for i in wh_idx:
            v = row[i] if i < len(row) else None
            if not v or isinstance(v, str):
                continue
            if v == STOCK_PLACEHOLDER_VALUE:
                p += int(v)
            else:
                r += int(v)
                if v > 0:
                    b += 1
        skus.append(sku); real.append(r); placeholder.append(p); bodegas.append(b)
    out = pd.DataFrame({"SKU": skus, STOCK_REAL: real, STOCK_PLACEHOLDER_COL: placeholder, STOCK_BODEGAS: bodegas})
    return out.drop_duplicates("SKU").reset_index(drop=True)


def attach_inventory(catalog: pd.DataFrame, inventory: pd.DataFrame | None) -> pd.DataFrame:
    """Add the STOCK REAL columns to the catalog frame (left join on SKU; missing → 0)."""
    if inventory is None:
        return catalog
    out = catalog.merge(inventory, on="SKU", how="left")
    for c in (STOCK_REAL, STOCK_PLACEHOLDER_COL, STOCK_BODEGAS):
        out[c] = out[c].fillna(0).astype(int)
    return out
