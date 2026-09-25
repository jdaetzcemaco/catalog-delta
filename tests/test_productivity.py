from datetime import date

import pandas as pd

from catalog_core import build_productivity

FLUJO = "Fecha de Salida del Flujo de trabajo"


def _team(rows, promoter_col):
    return pd.DataFrame(rows, columns=["<ID>", "<Name>", "Categoría", "Usuario", promoter_col, FLUJO, "Total Omnicanal"])


def _report():
    diseno = _team([
        ["1", "A", "CUADROS", "ana", "ana", "2026-04-22", 10],
        ["2", "B", "CUADROS", "ana", None, "2026-04-21", 0],
        ["3", "C", "MANTAS", "beto", "carla", "2026-04-22", 5],
    ], "Usuario Promueve desde Catalogo")
    edicion = _team([
        ["3", "C", "MANTAS", "dani", "eva", "2026-04-22", 5],
        ["4", "D", "CEPILLOS", "dani", "eva", "2026-04-22", 0],
    ], "Usuario Promueve desde Compras")
    return build_productivity(diseno, edicion)


def test_skus_by_user_counts_workers_and_promoters_once():
    by_user = _report().skus_by_user().set_index("Usuario")["SKUs Únicos"].to_dict()
    assert by_user == {"ana": 2, "beto": 1, "carla": 1, "dani": 2, "eva": 2}


def test_team_overlap():
    p = _report()
    assert p.repeated()["<ID>"].tolist() == ["3"]
    assert p.unique_counts() == {"total": 4, "solo_diseno": 2, "ambos": 1, "solo_edicion": 1}
    assert p.only_in("Diseño")["<ID>"].tolist() == ["1", "2"]
    assert p.only_in_by_category("Diseño").to_dict("records") == [{"Categoría": "CUADROS", "Cantidad": 2}]


def test_omni_stock_dedupes_across_teams():
    omni = _report().with_omni_stock()
    assert omni["<ID>"].tolist() == ["1", "3"]
    assert omni["Team"].tolist() == ["Diseño", "Diseño"]


def test_left_workflow_on_date_with_catalog_visibility():
    p = _report()
    assert p.latest_flujo_date() == date(2026, 4, 22)
    catalog = pd.DataFrame({"SKU": ["1", "3", "4"], "VISIBLE": ["Sí", "No", "Si"]})
    left = p.left_workflow(date(2026, 4, 22), catalog)
    assert sorted(left["<ID>"]) == ["1", "3", "4"]
    # FIX F5: "Sí" counts as visible
    assert p.left_workflow_kpis(left) == {
        "total": 3, "con_inventario": 2, "con_inventario_visibles": 1, "con_inventario_no_visibles": 1,
    }


def test_single_team():
    p = build_productivity(_report().diseno, None)
    assert not p.both
    assert p.repeated().empty
    assert p.unique_counts()["total"] == 3
