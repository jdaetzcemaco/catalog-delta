from __future__ import annotations

import pandas as pd
import pytest

from catalog_core import build_flags
from catalog_core.campaigns import (CHECKS, list_slug, parse_sku_list, readiness, readiness_kpis,
                                    regressions)


def test_parse_sku_list_by_header_and_by_content():
    skus, col = parse_sku_list(pd.DataFrame({"Descripción": ["a", "b"], "SKU": [830548.0, 1144749.0]}))
    assert (skus, col) == (["830548", "1144749"], "SKU")
    skus, col = parse_sku_list(pd.DataFrame({"x": ["Promo A", "Promo B", "Promo A"], "y": ["12345", "67890", "12345"]}))
    assert (skus, col) == (["12345", "67890"], "y")  # de-duplicated, file order
    with pytest.raises(ValueError, match="columna de SKU"):
        parse_sku_list(pd.DataFrame({"a": ["x", "y"], "b": ["p", "q"]}))


def _status(make_catalog, make_sku, skus, rows, **kw):
    raw = make_catalog(*rows)
    return readiness(skus, raw, build_flags(raw), **kw)


def test_readiness_checks(make_catalog, make_sku):
    st = _status(make_catalog, make_sku, ["ok", "hidden", "tipo-c", "weak", "nostock", "missing"], [
        make_sku("ok"),
        make_sku("hidden", VISIBLE="No"),
        make_sku("tipo-c", **{"NIVEL 1": "Catalogo Completo"}),
        make_sku("weak", **{"DESCRIPCION ERP": "", "MARCA": ""}),  # score 75
        make_sku("nostock", STOCK="0", **{"TIENE STOCK": "No"}),
    ]).set_index("SKU")
    assert st.loc["ok", "listo"]
    assert not st.loc["hidden", "listo"] and not st.loc["hidden", "visible"]
    assert not st.loc["tipo-c", "categoria_real"]
    assert not st.loc["weak", "contenido"] and st.loc["weak", "falta_descripcion"]
    assert not st.loc["nostock", "stock"] and st.loc["nostock", "stock_confiable"]
    assert not st.loc["missing", "en_catalogo"] and not st.loc["missing", "listo"]
    assert set(CHECKS) <= set(st.columns)


def test_placeholder_stock_does_not_block_readiness(make_catalog, make_sku):
    st = _status(make_catalog, make_sku, ["p"], [make_sku("p", STOCK="1000000")]).set_index("SKU")
    assert st.loc["p", "stock"] and not st.loc["p", "stock_confiable"] and st.loc["p", "listo"]
    k = readiness_kpis(st.reset_index())
    assert k["stock_no_confiable"] == 1 and k["pct_listos"] == 100.0


def test_history_columns_join(make_catalog, make_sku):
    visible = pd.DataFrame({"SKU": ["a"], "last_visible": [pd.Timestamp("2026-09-20")],
                            "first_seen": [pd.Timestamp("2026-07-02")], "visible_streak": [0]})
    workflow = pd.DataFrame({"SKU": ["a"], "salida_flujo": [pd.Timestamp("2026-09-10")],
                             "fin_dat_log": [pd.Timestamp("2026-09-10 11:16")], "equipo": ["Edición"]})
    st = _status(make_catalog, make_sku, ["a", "b"], [make_sku("a", VISIBLE="No"), make_sku("b")],
                 visible_history=visible, workflow=workflow).set_index("SKU")
    assert st.loc["a", "ultima_vez_visible"] == pd.Timestamp("2026-09-20")
    assert st.loc["a", "equipo"] == "Edición"
    assert pd.isna(st.loc["b", "ultima_vez_visible"])


def test_regressions_name_what_broke(make_catalog, make_sku):
    y = _status(make_catalog, make_sku, ["a", "b"], [make_sku("a"), make_sku("b")])
    t = _status(make_catalog, make_sku, ["a", "b"], [make_sku("a", VISIBLE="No", **{"TIENE IMAGEN": "No",
                                                                "IMAGEN PRIMARIA": "No", "URL IMAGEN": ""}),
                                                     make_sku("b")])
    lost = regressions(t, y)
    assert lost["SKU"].tolist() == ["a"]
    assert "Visible" in lost.loc[0, "perdio"] and "Contenido" in lost.loc[0, "perdio"]
    assert regressions(t, None).empty


def test_list_slug():
    assert list_slug("Navidad 2026 · Hogar & Deco!") == "navidad-2026-hogar-deco"
