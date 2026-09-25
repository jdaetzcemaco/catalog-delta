"""One call that runs every catalog rule, and the Excel report built from it."""

from __future__ import annotations

import io
from dataclasses import dataclass
from datetime import date

import pandas as pd

from .deltas import change_tables, compute_deltas, sku_changes
from .inventory import InventoryReport, build_inventory
from .productivity import ProductivityReport
from .scoring import build_flags, build_summary

# Inventory section key -> Excel sheet name (v1 names kept; new sheets appended)
INVENTORY_SHEETS = {
    "stock_no_visible": "Stock No Visible",
    "tipo_c_stock": "Tipo C con Stock",
    "no_fisicos": "No Fisicos",
    "long_tail_sin_modal": "Long Tail Sin Modal",
    "sin_imagen_deshabilitado": "Sin Imagen Deshabilitado",
    "url_no_actualizada": "URL No Actualizada",
    "deshabilitados_stock": "Deshabilitados con Stock",
    "tipo_c_graduados": "Tipo C Graduados",
}


@dataclass
class CatalogRun:
    today_raw: pd.DataFrame
    yesterday_raw: pd.DataFrame | None
    today: pd.DataFrame                  # flags
    yesterday: pd.DataFrame | None       # flags
    merged: pd.DataFrame | None
    summary: pd.DataFrame
    sku_changes: dict | None
    changes: dict[str, pd.DataFrame]
    inventory: InventoryReport


def run_catalog(today_raw: pd.DataFrame, yesterday_raw: pd.DataFrame | None) -> CatalogRun:
    """
    Run every catalog rule. Without a previous snapshot (the very first day) only
    today's health and inventory are computed; there are no change tables.
    """
    today = build_flags(today_raw)
    yesterday = merged = changes_ = None
    changes: dict[str, pd.DataFrame] = {}
    if yesterday_raw is not None:
        yesterday = build_flags(yesterday_raw)
        merged = compute_deltas(today, yesterday)
        changes_ = sku_changes(today, yesterday)
        changes = change_tables(today, yesterday, merged)
    return CatalogRun(
        today_raw=today_raw,
        yesterday_raw=yesterday_raw,
        today=today,
        yesterday=yesterday,
        merged=merged,
        summary=build_summary(today),
        sku_changes=changes_,
        changes=changes,
        inventory=build_inventory(today_raw, today, yesterday_raw),
    )


def productivity_sheets(
    prod: ProductivityReport,
    flujo_date: date | None = None,
    catalog: pd.DataFrame | None = None,
) -> dict[str, pd.DataFrame]:
    sheets: dict[str, pd.DataFrame] = {}
    if not prod.teams():
        return sheets
    sheets["SKUs por Usuario"] = prod.skus_by_user()
    if prod.both:
        sheets["SKUs Repetidos"] = prod.repeated()
        sheets["Solo Diseño"] = prod.only_in("Diseño")
        sheets["Solo Edición"] = prod.only_in("Edición")
    # FIX F4: same rows as the screen (chosen date, one row per SKU, with VISIBLE);
    # v1 exported every date with repeats
    day = flujo_date or prod.latest_flujo_date()
    if day is not None:
        sheets["Salieron del Flujo"] = prod.left_workflow(day, catalog)
    sheets["Con Inventario Omnicanal"] = prod.with_omni_stock()
    return sheets


def excel_sheets(
    run: CatalogRun | None,
    prod: ProductivityReport | None = None,
    flujo_date: date | None = None,
) -> dict[str, pd.DataFrame]:
    """All report sheets in download order."""
    sheets: dict[str, pd.DataFrame] = {}
    if run is not None:
        sheets["Catalog Health"] = run.summary
        sheets.update(run.changes)
    if prod is not None:
        sheets.update(productivity_sheets(prod, flujo_date, run.today_raw if run else None))
    if run is not None:
        for key, name in INVENTORY_SHEETS.items():
            if key in run.inventory.sections:
                sheets[name] = run.inventory.sections[key]
    return sheets


def to_excel_bytes(sheets: dict[str, pd.DataFrame]) -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name[:31], index=False)
    return out.getvalue()
