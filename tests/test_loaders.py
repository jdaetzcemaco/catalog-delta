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
