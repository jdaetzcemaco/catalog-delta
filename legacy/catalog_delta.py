"""
Catalog Delta Report Generator

Compares two catalog snapshots (today vs yesterday) and generates an Excel report
with KPIs, visibility changes, and content score deltas.
"""

import argparse
import logging
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd

# --- CONFIGURATION ---
SCORE_WEIGHTS = {
    "has_name": 10,
    "has_desc": 15,
    "has_brand": 5,
    "has_price": 15,
    "has_image": 25,
    "taxonomy_depth": 15,
    "is_visible": 10,
}

# Recognized "yes" values for boolean fields
YES_VALUES = {"si", "sí", "yes", "true", "1"}

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# --- HELPER FUNCTIONS ---
def safe_get(df: pd.DataFrame, col: str, default: str = "") -> pd.Series:
    """
    Safely get a column from DataFrame, returning a Series of default values if missing.

    Args:
        df: Source DataFrame
        col: Column name to retrieve
        default: Default value to fill if column doesn't exist

    Returns:
        Series with column data or default values
    """
    if col in df.columns:
        return df[col]
    return pd.Series([default] * len(df), index=df.index)


def safe_get_first(df: pd.DataFrame, cols: list[str], default: str = "") -> pd.Series:
    """
    Get the first existing column from a list of column names.

    Args:
        df: Source DataFrame
        cols: List of column names to try in order
        default: Default value if no columns exist

    Returns:
        Series from first matching column or default values
    """
    for col in cols:
        if col in df.columns:
            return df[col]
    return pd.Series([default] * len(df), index=df.index)


def yesno(series: pd.Series) -> pd.Series:
    """
    Convert a series to binary (0/1) based on common "yes" values.

    Recognizes: si, sí, yes, true, 1 (case-insensitive)

    Args:
        series: Input series to convert

    Returns:
        Series of integers (0 or 1)
    """
    s = series.astype(str).str.strip().str.lower()
    return s.isin(YES_VALUES).astype(int)


def nonempty(series: pd.Series) -> pd.Series:
    """
    Check if series values are non-empty (not NaN and not blank string).

    Args:
        series: Input series to check

    Returns:
        Series of integers (0 or 1)
    """
    return (~series.isna() & (series.astype(str).str.strip() != "")).astype(int)


def load_data(path: str) -> pd.DataFrame:
    """
    Load catalog data from Excel or CSV file.

    For Excel files, attempts to read from 'SKUs' sheet if it exists.
    Column names are normalized to uppercase and stripped of whitespace.

    Args:
        path: Path to the input file (.xlsx or .csv)

    Returns:
        DataFrame with normalized column names

    Raises:
        FileNotFoundError: If the file doesn't exist
        ValueError: If file extension is not supported
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input file not found: {path}")

    ext = os.path.splitext(path)[1].lower()

    if ext == ".xlsx":
        excel_file = pd.ExcelFile(path)
        sheet_name = "SKUs" if "SKUs" in excel_file.sheet_names else 0
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        logger.info(f"Loaded {len(df)} rows from '{path}' (sheet: {sheet_name})")
    elif ext == ".csv":
        df = pd.read_csv(path)
        logger.info(f"Loaded {len(df)} rows from '{path}'")
    else:
        raise ValueError(f"Unsupported file extension: {ext}. Use .xlsx or .csv")

    df.columns = [c.strip().upper() for c in df.columns]
    return df


def validate_dataframe(df: pd.DataFrame, source_name: str) -> None:
    """
    Validate that DataFrame has required columns.

    Args:
        df: DataFrame to validate
        source_name: Name of the source file (for error messages)

    Raises:
        ValueError: If required columns are missing
    """
    if "SKU" not in df.columns:
        raise ValueError(f"'{source_name}' must contain a 'SKU' column. Found columns: {list(df.columns)}")


def build_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build boolean flags and content score for each SKU.

    Extracts product attributes and calculates a weighted content score
    based on completeness of: name, description, brand, price, image,
    taxonomy, and visibility.

    Args:
        df: Raw catalog DataFrame with product data

    Returns:
        DataFrame with SKU, boolean flags, and content_score
    """
    f = pd.DataFrame()
    f["SKU"] = df["SKU"]

    # Content flags
    f["has_name"] = nonempty(safe_get_first(df, ["NOMBRE DE SKU", "NOMBRE DE PRODUCTO"]))
    f["has_desc"] = nonempty(safe_get(df, "DESCRIPCION ERP"))
    f["has_brand"] = nonempty(safe_get(df, "MARCA"))

    # Price: check "TIENE PRECIO" flag or actual PRECIO value > 0
    has_price_flag = yesno(safe_get(df, "TIENE PRECIO"))
    has_price_value = (pd.to_numeric(safe_get(df, "PRECIO"), errors="coerce").fillna(0) > 0).astype(int)
    f["has_price"] = (has_price_flag | has_price_value).astype(int)

    # Image: check multiple possible columns
    has_image_flag = yesno(safe_get(df, "TIENE IMAGEN"))
    has_primary_image = yesno(safe_get(df, "IMAGEN PRIMARIA"))
    has_image_url = nonempty(safe_get(df, "URL IMAGEN"))
    f["has_image"] = (has_image_flag | has_primary_image | has_image_url).astype(int)

    # Stock: check STOCK value or "TIENE STOCK" flag
    has_stock_value = (pd.to_numeric(safe_get(df, "STOCK"), errors="coerce").fillna(0) > 0).astype(int)
    has_stock_flag = yesno(safe_get(df, "TIENE STOCK"))
    f["has_stock"] = (has_stock_value | has_stock_flag).astype(int)

    # Visibility and enabled status
    f["is_visible"] = yesno(safe_get(df, "VISIBLE"))
    f["is_enabled"] = safe_get(df, "HABILITADO/DESHABILITADO").astype(str).str.lower().str.startswith("habil").astype(int)

    # Taxonomy depth (levels 1-3)
    f["taxonomy_depth"] = (
        nonempty(safe_get(df, "NIVEL 1")) +
        nonempty(safe_get(df, "NIVEL 2")) +
        nonempty(safe_get(df, "NIVEL 3"))
    )

    # Calculate content score
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


def compute_deltas(today: pd.DataFrame, yesterday: pd.DataFrame) -> pd.DataFrame:
    """
    Merge today and yesterday data and compute delta flags.

    Args:
        today: Today's flags DataFrame
        yesterday: Yesterday's flags DataFrame

    Returns:
        Merged DataFrame with delta columns
    """
    merged = today.merge(yesterday, on="SKU", suffixes=("_today", "_yesterday"), how="outer")

    merged["delta_score"] = merged["content_score_today"].fillna(0) - merged["content_score_yesterday"].fillna(0)

    # SKUs that exist in both files (not new, not removed)
    exists_both = merged["content_score_today"].notna() & merged["content_score_yesterday"].notna()

    # Visibility changes (only for SKUs in both files)
    vis_today = merged["is_visible_today"].fillna(0).astype(int)
    vis_yesterday = merged["is_visible_yesterday"].fillna(0).astype(int)
    merged["newly_visible"] = exists_both & (vis_today == 1) & (vis_yesterday == 0)
    merged["no_longer_visible"] = exists_both & (vis_today == 0) & (vis_yesterday == 1)

    # Attribute changes (only for SKUs in both files)
    # Fill NaN with -1 so NaN != NaN doesn't cause issues, and NaN vs 0/1 is detected
    img_today = merged["has_image_today"].fillna(-1).astype(int)
    img_yesterday = merged["has_image_yesterday"].fillna(-1).astype(int)
    merged["image_changed"] = exists_both & (img_today != img_yesterday)

    price_today = merged["has_price_today"].fillna(-1).astype(int)
    price_yesterday = merged["has_price_yesterday"].fillna(-1).astype(int)
    merged["price_changed"] = exists_both & (price_today != price_yesterday)

    stock_today = merged["has_stock_today"].fillna(-1).astype(int)
    stock_yesterday = merged["has_stock_yesterday"].fillna(-1).astype(int)
    merged["stock_flipped"] = exists_both & (stock_today != stock_yesterday)

    merged["score_changed"] = exists_both & (merged["delta_score"].abs() >= 10)

    return merged


def build_summary(today: pd.DataFrame) -> pd.DataFrame:
    """
    Build KPI summary DataFrame.

    Args:
        today: Today's flags DataFrame

    Returns:
        Single-row DataFrame with KPI metrics
    """
    return pd.DataFrame([{
        "Total SKUs": len(today),
        "Visible": int(today["is_visible"].sum()),
        "Visible %": round(today["is_visible"].mean() * 100, 2),
        "With Image %": round(today["has_image"].mean() * 100, 2),
        "With Price %": round(today["has_price"].mean() * 100, 2),
        "With Stock %": round(today["has_stock"].mean() * 100, 2),
        "Avg Content Score": round(today["content_score"].mean(), 2),
        "Score = 100": int((today["content_score"] == 100).sum()),
    }])


def filter_sheet(merged: pd.DataFrame, cond: pd.Series, cols: list[str]) -> pd.DataFrame:
    """
    Filter merged DataFrame by condition for a specific report sheet.

    Args:
        merged: Merged DataFrame with deltas
        cond: Boolean condition Series
        cols: Columns to include in output

    Returns:
        Filtered DataFrame or empty DataFrame with specified columns
    """
    if cond.any():
        return merged.loc[cond, cols].copy()
    return pd.DataFrame(columns=cols)


def generate_report(
    today: pd.DataFrame,
    yesterday: pd.DataFrame,
    merged: pd.DataFrame,
    output_path: str,
) -> dict[str, int]:
    """
    Generate the Excel report with all sheets.

    Args:
        today: Today's flags DataFrame
        yesterday: Yesterday's flags DataFrame
        merged: Merged DataFrame with deltas
        output_path: Path for the output Excel file

    Returns:
        Dictionary with counts of items in each sheet
    """
    summary = build_summary(today)

    # Build filtered sheets
    sheets = {
        "Catalog Health": summary,
        "No Longer Visible": filter_sheet(
            merged,
            merged["no_longer_visible"],
            ["SKU", "content_score_today", "is_visible_today", "is_visible_yesterday"]
        ),
        "Newly Visible": filter_sheet(
            merged,
            merged["newly_visible"],
            ["SKU", "content_score_today", "is_visible_today", "is_visible_yesterday"]
        ),
        "Image Changes": filter_sheet(
            merged,
            merged["image_changed"],
            ["SKU", "has_image_today", "has_image_yesterday"]
        ),
        "Price Changes": filter_sheet(
            merged,
            merged["price_changed"],
            ["SKU", "has_price_today", "has_price_yesterday"]
        ),
        "Stock Flips": filter_sheet(
            merged,
            merged["stock_flipped"],
            ["SKU", "has_stock_today", "has_stock_yesterday"]
        ),
        "Score Changes": filter_sheet(
            merged,
            merged["score_changed"],
            ["SKU", "content_score_today", "content_score_yesterday", "delta_score"]
        ),
        "Top Priorities": today.loc[
            (today["content_score"] < 80) & (today["is_visible"] == 1) & (today["has_stock"] == 1)
        ].sort_values("content_score").head(50),
    }

    # Write to Excel
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    # Return counts for summary
    return {name: len(df) for name, df in sheets.items()}


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Generate catalog delta report comparing today vs yesterday snapshots.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python catalog_delta.py
  python catalog_delta.py --today catalog_jan16.xlsx --yesterday catalog_jan15.xlsx
  python catalog_delta.py -t today.csv -y yesterday.csv -o report.xlsx
        """,
    )
    parser.add_argument(
        "-t", "--today",
        default="today.xlsx",
        help="Today's catalog file (default: today.xlsx)",
    )
    parser.add_argument(
        "-y", "--yesterday",
        default="yesterday.xlsx",
        help="Yesterday's catalog file (default: yesterday.xlsx)",
    )
    parser.add_argument(
        "-o", "--output",
        default=None,
        help="Output file path (default: Catalog_Delta_YYYYMMDD.xlsx)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose/debug logging",
    )
    return parser.parse_args()


def main() -> int:
    """
    Main entry point for the catalog delta report generator.

    Returns:
        Exit code (0 for success, 1 for error)
    """
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    output_path = args.output or f"Catalog_Delta_{datetime.now().strftime('%Y%m%d')}.xlsx"

    try:
        # Load data
        logger.info("Loading catalog data...")
        today_raw = load_data(args.today)
        yesterday_raw = load_data(args.yesterday)

        # Validate
        validate_dataframe(today_raw, args.today)
        validate_dataframe(yesterday_raw, args.yesterday)

        # Build flags
        logger.info("Building content flags and scores...")
        today = build_flags(today_raw)
        yesterday = build_flags(yesterday_raw)

        # Compute deltas
        logger.info("Computing deltas...")
        merged = compute_deltas(today, yesterday)

        # Generate report
        logger.info("Generating report...")
        counts = generate_report(today, yesterday, merged, output_path)

        # Print summary
        logger.info("=" * 50)
        logger.info("REPORT SUMMARY")
        logger.info("=" * 50)
        for sheet_name, count in counts.items():
            logger.info(f"  {sheet_name}: {count} rows")
        logger.info("=" * 50)
        logger.info(f"Report saved to: {output_path}")

        return 0

    except FileNotFoundError as e:
        logger.error(f"File error: {e}")
        return 1
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        return 1
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
