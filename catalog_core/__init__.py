"""
Catalog Delta core logic.

Single source of truth for scoring, day-over-day deltas, inventory QA and team
productivity. Used by the processing job, the Streamlit app and the CLI, so every
surface reports the same numbers.

Rules marked `FIX Fn` intentionally differ from the v1 app (see docs/CHANGES.md).
"""

from .loaders import load_catalog, load_productivity
from .scoring import MAX_SCORE, SCORE_WEIGHTS, build_flags, build_summary
from .deltas import compute_deltas, sku_changes, change_tables
from .inventory import InventoryReport, build_inventory
from .productivity import ProductivityReport, build_productivity
from .history import HISTORY_HEADER, build_history_row
from .report import CatalogRun, run_catalog, excel_sheets, to_excel_bytes

__all__ = [
    "load_catalog", "load_productivity",
    "MAX_SCORE", "SCORE_WEIGHTS", "build_flags", "build_summary",
    "compute_deltas", "sku_changes", "change_tables",
    "InventoryReport", "build_inventory",
    "ProductivityReport", "build_productivity",
    "HISTORY_HEADER", "build_history_row",
    "CatalogRun", "run_catalog", "excel_sheets", "to_excel_bytes",
]
