from __future__ import annotations

import pandas as pd
import pytest

from pipeline.gsc_bq import GSCBigQuery, GSCNotReady, rows_to_frame


def _resp(fields, rows):
    return {"fields": fields}, [{"f": [{"v": v} for v in r]} for r in rows]


def test_rows_to_frame_types_by_schema():
    schema, rows = _resp([{"name": "url", "type": "STRING"}, {"name": "impressions", "type": "INTEGER"},
                          {"name": "ctr", "type": "FLOAT"}, {"name": "ml", "type": "BOOLEAN"}],
                         [["/a-1001/p", "12", "0.5", "true"], ["/b", None, None, "false"]])
    df = rows_to_frame(schema, rows)
    assert df["impressions"].tolist() == [12, 0] and df["ctr"].iloc[0] == 0.5 and df["ml"].tolist() == [True, False]
    assert rows_to_frame({"fields": []}, []).empty


def _client(monkeypatch, frames: dict[str, pd.DataFrame]) -> GSCBigQuery:
    c = GSCBigQuery("p", "d", "t", {"client_email": "x"})
    calls = []

    def fake_query(sql, params):
        calls.append((sql, params))
        return frames["queries" if "ROW_NUMBER" in sql else "pages"]

    monkeypatch.setattr(c, "_query", fake_query)
    c.calls = calls  # type: ignore[attr-defined]
    return c


def test_pages_report_matches_api_schema_and_one_based_position(monkeypatch):
    pages = pd.DataFrame({"url": ["https://www.cemaco.com/cortina-830548/p", "https://www.cemaco.com/hogar"],
                          "impressions": [100, 50], "clicks": [10, 0], "sum_position": [250, 0],
                          "impressions_ml": [0, 0], "impressions_ps": [40, 0], "impressions_mobile": [70, 20], "consultas": [8, 3]})
    c = _client(monkeypatch, {"pages": pages})
    out = c.pages_report("2026-10-10")
    assert list(out.columns[:7]) == ["day", "page", "clicks", "impressions", "ctr", "position", "SKU"]
    r = out.set_index("page").loc["https://www.cemaco.com/cortina-830548/p"]
    assert r["SKU"] == "830548" and r["position"] == 3.5 and r["ctr"] == 0.1 and r["consultas"] == 8 and r["impressions_ps"] == 40
    assert pd.isna(out.set_index("page").loc["https://www.cemaco.com/hogar", "SKU"])
    assert c.calls[0][1] == {"day": "2026-10-10"} and "data_date = @day" in c.calls[0][0]


def test_pages_report_raises_when_export_not_landed(monkeypatch):
    c = _client(monkeypatch, {"pages": pd.DataFrame(columns=["url", "impressions", "clicks", "sum_position"])})
    with pytest.raises(GSCNotReady):
        c.pages_report("2026-10-25")


def test_queries_report_keeps_product_urls_only(monkeypatch):
    q = pd.DataFrame({"url": ["https://www.cemaco.com/cortina-830548/p", "https://www.cemaco.com/cortina-830548/p", "https://www.cemaco.com/hogar"],
                      "query": ["cortina baño", "(anónimas)", "hogar"], "anonima": [False, True, False],
                      "impressions": [20, 5, 9], "clicks": [2, 0, 1], "sum_position": [40, 10, 0], "ml": [False, False, False],
                      "ps": [True, False, False]})
    c = _client(monkeypatch, {"pages": pd.DataFrame(), "queries": q})
    out = c.queries_report("2026-10-10")
    assert out["SKU"].tolist() == ["830548", "830548"] and out["position"].tolist() == [3.0, 3.0]
    assert out["anonima"].tolist() == [False, True] and out["day"].unique().tolist() == ["2026-10-10"]
    c2 = _client(monkeypatch, {"pages": pd.DataFrame(), "queries": pd.DataFrame(columns=q.columns)})
    assert c2.queries_report("2026-10-11").empty
