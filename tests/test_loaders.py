from __future__ import annotations

import io

import pandas as pd
import pytest

from catalog_core import excel_sheets, load_catalog, run_catalog, to_excel_bytes


def _xlsx(sheets: dict[str, pd.DataFrame]) -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        for name, df in sheets.items():
            df.to_excel(w, sheet_name=name, index=False)
    return out.getvalue()


def test_reads_skus_sheet_as_text_with_normalised_columns():
    data = _xlsx({
        "Inventarios": pd.DataFrame({"Sku": [1]}),
        "SKUs": pd.DataFrame({" sku ": [830548, 1144749], "precio": [29.99, 0], "visible": ["Si", None]}),
    })
    df = load_catalog(data, name="catalog-daily.xlsx")
    assert list(df.columns) == ["SKU", "PRECIO", "VISIBLE"]
    assert df["SKU"].tolist() == ["830548", "1144749"]
    assert pd.isna(df.loc[1, "VISIBLE"])


def test_reads_csv_with_bom():
    data = "﻿SKU,VISIBLE\n 001 ,Si\n".encode("utf-8")
    df = load_catalog(data, name="x.csv")
    assert df["SKU"].tolist() == ["001"]


def test_rejects_missing_sku_and_bad_extension():
    with pytest.raises(ValueError, match="SKU"):
        load_catalog(_xlsx({"SKUs": pd.DataFrame({"X": [1]})}), name="a.xlsx")
    with pytest.raises(ValueError, match="Unsupported"):
        load_catalog(b"", name="a.txt")


def test_full_report_round_trips_to_excel(make_sku, make_catalog):
    run = run_catalog(make_catalog(make_sku("1"), make_sku("2", VISIBLE="No")), make_catalog(make_sku("1")))
    sheets = excel_sheets(run)
    assert list(sheets)[:3] == ["Catalog Health", "New SKUs", "Removed SKUs"]
    assert "Stock No Visible" in sheets and "Tipo C Graduados" in sheets
    back = pd.read_excel(io.BytesIO(to_excel_bytes(sheets)), sheet_name=None)
    assert back["New SKUs"]["SKU"].astype(str).tolist() == ["2"]


def _workbook_with_inventory(skus_rows, inv_rows, warehouses):
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        pd.DataFrame(skus_rows).to_excel(w, sheet_name="SKUs", index=False)
        inv = pd.DataFrame(inv_rows, columns=["Sku", "ProductName", "Total", *warehouses])
        inv.to_excel(w, sheet_name="Inventarios", index=False)
    return out.getvalue()


def test_inventory_sheet_excludes_the_placeholder_warehouse(make_sku):
    from catalog_core import build_flags
    from catalog_core.loaders import attach_inventory, load_inventory_sheet
    from catalog_core.quality import quality_checks

    data = _workbook_with_inventory(
        [make_sku("1", STOCK="1000003"), make_sku("2", STOCK="1000000"), make_sku("3", STOCK="7")],
        [[1, "A", 1000003, 1000000, 3, 0], [2, "B", 1000000, 1000000, 0, 0], [3, "C", 7, 0, 5, 2]],
        ["MDR", "T004", "T007"])
    inv = load_inventory_sheet(data, name="catalog-daily.xlsx").set_index("SKU")
    assert inv.loc["1", "STOCK REAL"] == 3 and inv.loc["2", "STOCK REAL"] == 0 and inv.loc["3", "STOCK REAL"] == 7
    assert inv.loc["1", "STOCK PLACEHOLDER"] == 1_000_000 and inv.loc["3", "BODEGAS CON STOCK"] == 2

    cat = attach_inventory(load_catalog(data, name="catalog-daily.xlsx"), inv.reset_index())
    flags = build_flags(cat).set_index("SKU")
    # F9: has_stock from STOCK REAL, so the placeholder-only SKU has no stock
    assert flags.loc["1", "has_stock"] == 1 and flags.loc["2", "has_stock"] == 0 and flags.loc["3", "has_stock"] == 1
    q = quality_checks(cat)
    assert q["stock_corrected"] and q["stock_placeholder"] == 2 and q["stock_real_pct"] == round(2 / 3 * 100, 2)


def test_inventory_sheet_absent():
    from catalog_core.loaders import attach_inventory, load_inventory_sheet

    data = _xlsx({"SKUs": pd.DataFrame({"SKU": [1]})})
    assert load_inventory_sheet(data, name="x.xlsx") is None
    assert load_inventory_sheet(b"", name="x.csv") is None
    df = pd.DataFrame({"SKU": ["1"]})
    assert attach_inventory(df, None) is df


def test_productivity_sku_comes_from_sku_erp_without_leading_zeros():
    from catalog_core.loaders import load_productivity
    buf = io.BytesIO()
    pd.DataFrame({"<ID>": ["1169614", "catgo-730dcc", "catgo-x"],
                  "Sku ERP": ["000000000001169614", "000000000001235140", None],
                  "<Name>": ["A", "B", "C"]}).to_excel(buf, index=False)
    df = load_productivity(buf.getvalue())
    assert df["<ID>"].tolist() == ["1169614", "1235140", "catgo-x"]
    assert df["Sku ERP"].tolist()[0] == "000000000001169614"   # the source column is kept
