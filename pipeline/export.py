"""Excel report rebuilt from stored results (same sheets and order as the v1 download)."""

from __future__ import annotations

from datetime import date

import pandas as pd

from catalog_core import ProductivityReport
from catalog_core.report import INVENTORY_SHEETS, productivity_sheets

from .store import ResultStore


def excel_sheets_from_store(
    store: ResultStore,
    day: str,
    prod: ProductivityReport | None = None,
    flujo_date: date | None = None,
) -> dict[str, pd.DataFrame]:
    manifest = store.load_manifest(day)
    if manifest is None:
        raise FileNotFoundError(f"No results for {day}")
    sheets: dict[str, pd.DataFrame] = {"Catalog Health": pd.DataFrame([manifest["summary"]])}
    for name in manifest["tables"]["changes"]:
        sheets[name] = store.load_table(day, "changes", name)
    if prod is not None:
        sheets.update(productivity_sheets(prod, flujo_date, store.load_sku_info(day)))
    for key, name in INVENTORY_SHEETS.items():
        if key in manifest["tables"]["inventory"]:
            sheets[name] = store.load_table(day, "inventory", key)
    return sheets
