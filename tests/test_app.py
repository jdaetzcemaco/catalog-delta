"""Render every app page headlessly against a small processed store; fail on any exception."""

from __future__ import annotations

import io
import os

import pandas as pd
import pytest

from pipeline.config import Settings
from pipeline.process import Job
from pipeline.storage import LocalStorage

AppTest = pytest.importorskip("streamlit.testing.v1").AppTest
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PAGES = ["resumen", "cambios", "inventario", "historial", "productividad", "cargar", "campanas", "alertas", "rendimiento", "cro",
         "preguntar", "seo_impacto"]


def _xlsx(df: pd.DataFrame, sheet: str) -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        df.to_excel(w, sheet_name=sheet, index=False)
    return out.getvalue()


@pytest.fixture
def store_root(tmp_path, make_sku, monkeypatch):
    root = tmp_path / "onedrive"
    incoming = root / "cemaco-reports" / "incoming"
    incoming.mkdir(parents=True)
    tipo_c = {"NIVEL 1": "Catalogo Completo"}
    (incoming / "catalog-daily-2026-04-01.xlsx").write_bytes(_xlsx(pd.DataFrame(
        [make_sku("1"), make_sku("2", **tipo_c), make_sku("3", VISIBLE="No")]), "SKUs"))
    (incoming / "catalog-daily-2026-04-02.xlsx").write_bytes(_xlsx(pd.DataFrame(
        [make_sku("1", VISIBLE="No"), make_sku("2", STOCK="1000000"),
         make_sku("4", **{"HABILITADO/DESHABILITADO": "Deshabilitado"})]),
        "SKUs"))
    prod = pd.DataFrame({
        "<ID>": [1, 2], "<Name>": ["A", "B"], "Categoría": ["X", "Y"], "Usuario": ["ana", "beto"],
        "Usuario Promueve desde Catalogo": ["ana", None], "Fecha de Salida del Flujo de trabajo": ["2026-04-01"] * 2,
        "Total Omnicanal": [3, 0],
    })
    (incoming / "productivity-diseno-2026-04-02.xlsx").write_bytes(_xlsx(prod, "Sheet1"))
    edicion = prod.rename(columns={"Usuario Promueve desde Catalogo": "Usuario Promueve desde Compras"})
    (incoming / "productivity-edicion-2026-04-02.xlsx").write_bytes(_xlsx(edicion, "Sheet1"))
    ingresos = pd.DataFrame({"<ID>": ["catgo-1", "1"], "<Name>": ["X", "A"], "Categoría": [None, "X"],
                             "Completitud Mercadeo (Calculado)": [12, 0], "Estado Imagen Primaria": ["Sin Imagen"] * 2,
                             "Fecha de Creación STEP": ["2026-04-01"] * 2})
    (incoming / "productivity-ingresos-2026-04-02.xlsx").write_bytes(_xlsx(ingresos, "Sheet1"))
    import json
    (root / "cemaco-reports" / "incoming" / "lists").mkdir(parents=True)
    (root / "cemaco-reports" / "incoming" / "lists" / "navidad.json").write_text(json.dumps(
        {"name": "Navidad", "skus": ["1", "2", "9"], "status": "active", "owner": "ana",
         "created_at": "2026-04-02T12:00:00+00:00"}))
    job = Job(Settings(storage="local", local_root=str(root)), LocalStorage(str(root)))
    job.run()
    # A small performance file so Rendimiento and the Campañas traffic block render
    perf = pd.DataFrame({"SKU": ["1", "2", "4"], "product_id": ["100"] * 3})
    for w in ("7d", "30d", "90d"):
        for m in ("vistas", "carrito", "compras", "ingresos", "sessions", "organic_sessions", "clicks", "impressions"):
            perf[f"{m}_{w}"] = [10.0, 0.0, 5.0]
    perf["vistas_7d_prev"] = perf["organic_sessions_7d_prev"] = perf["clicks_7d_prev"] = 1.0
    perf["posicion_30d"] = [3.5, None, 12.0]
    perf["tasa_carrito_30d"] = perf["tasa_compra_30d"] = 0.1
    perf["ultimo_dia_vistas"] = pd.Timestamp("2026-04-02")
    # catalog context the job adds for CRO (same names as the page's own flags)
    perf["visible"], perf["con_stock"], perf["NIVEL 1"] = [True, False, True], [True, False, False], "Hogar"
    job.store.save_perf(perf)
    cro = pd.DataFrame({"SKU": ["1", "4"], "grupo": ["fuga_conversion", "joya_oculta"], "motivo": ["m", "m"],
                        "impacto": [10.0, 2.0], "NIVEL 1": ["Hogar", "Hogar"], "vistas_ventana": [500, 40],
                        "tasa_carrito": [0.01, 0.2], "cat_cart_rate": [0.1, 0.1], "tasa_compra": [0.003, 0.1],
                        "cat_buy_rate": [0.03, 0.03], "content_score": [90, 95]})
    job.store.save_cro(cro, {"as_of": "2026-04-02", "catalog_day": "2026-04-02",
                             "groups": {"fuga_conversion": 1, "joya_oculta": 1},
                             "content_vs_conversion": [{"banda": "90–95", "skus": 2, "tasa_carrito": 0.1, "tasa_compra": 0.03}],
                             "channel_totals": [{"bucket": "organico", "vistas": 100, "carrito": 10, "compras": 3,
                                                 "ingresos": 50.0, "tasa_carrito": 0.1, "tasa_compra": 0.03}]})
    index = job.store.load_index()
    index["perf"] = {"as_of": "2026-04-02", "catalog_day": "2026-04-02", "skus": 3, "skus_with_views_30d": 2}
    # SEO impact frames so the Impacto SEO page renders its KPIs, chart, tables and SKU detail
    cohorts = pd.DataFrame({"cohorte": ["2026-03-30"] * 2, "equipo": ["Todos", "Diseño"], "skus": [2, 2], "completo": [True, True],
                            "con_presencia": [1, 1], "nuevo": [0, 0], "crecio": [1, 1], "bajo": [0, 0], "estable": [0, 0],
                            "sin_presencia": [1, 1], "no_visible": [0, 0], "pendiente": [0, 0], "impr_antes": [100.0, 100.0],
                            "impr_despues": [150.0, 150.0], "clics_antes": [10.0, 10.0], "clics_despues": [15.0, 15.0],
                            "delta_impr_pct": [50.0, 50.0], "delta_clics_pct": [50.0, 50.0], "pos_antes": [4.0, 4.0], "pos_despues": [4.1, 4.1],
                            "tasa_nuevos": [0.0, 0.0], "dias_a_primera_impresion_mediana": [None, None], "ctrl_delta_impr_pct": [10.0, 10.0],
                            "ctrl_delta_clics_pct": [5.0, 5.0], "ctrl_tasa_nuevos": [0.0, 0.0], "uplift_impr_pct": [40.0, 40.0],
                            "uplift_clics_pct": [45.0, 45.0], "uplift_impr_ci_low": [10.0, 10.0], "uplift_impr_ci_high": [70.0, 70.0],
                            "muestra_pequena": [True, True], "as_of": pd.Timestamp("2026-04-02")})
    skus = pd.DataFrame({"SKU": ["1", "2"], "event_day": pd.to_datetime(["2026-04-01"] * 2), "cohorte": pd.to_datetime(["2026-03-30"] * 2),
                         "equipo": ["Diseño"] * 2, "usuarios": ["ana", "beto"], "estado": ["crecio", "sin_presencia"],
                         "impr_antes": [100.0, 0.0], "impr_despues": [150.0, 0.0], "delta_impr_pct": [50.0, None],
                         "uplift_impr_pct": [40.0, None], "clics_antes": [10.0, 0.0], "clics_despues": [15.0, 0.0],
                         "pos_antes": [4.0, None], "pos_despues": [4.1, None], "completo": [True, True],
                         "dias_a_primera_impresion": [None, None], "cambios_ficha": ["+descripción", ""], "score_antes": [80, 95],
                         "score_despues": [95, 95], "consultas_nuevas": [1, 0], "consultas_perdidas": [0, 0], "top_consulta_despues": ["cojín", None]})
    daily = pd.DataFrame({"cohorte": pd.Timestamp("2026-03-30"), "equipo": "Todos", "grupo": ["trabajados"] * 29 + ["control"] * 29,
                          "offset": list(range(-14, 15)) * 2, "impressions": [10.0] * 58, "clicks": [1.0] * 58, "skus": 2, "indice": [100.0] * 58})
    buckets = pd.DataFrame({"cohorte": [pd.Timestamp("2026-03-30")], "equipo": ["Todos"], "bucket": ["100–999"], "n": [1], "impr_antes": [100.0],
                            "impr_despues": [150.0], "delta_impr_pct": [50.0], "clics_antes": [10.0], "clics_despues": [15.0],
                            "delta_clics_pct": [50.0], "ctrl_n": [3], "ctrl_impr_antes": [300.0], "ctrl_impr_despues": [330.0],
                            "ctrl_delta_impr_pct": [10.0], "ctrl_delta_clics_pct": [5.0]})
    queries = pd.DataFrame({"SKU": ["1"], "query": ["cojín"], "impr_antes": [0.0], "impr_despues": [30.0], "clics_antes": [0.0],
                            "clics_despues": [3.0], "pos_antes": [None], "pos_despues": [5.0], "estado": ["nueva"], "anonima": [False],
                            "ml": [False], "ps": [True], "cohorte": [pd.Timestamp("2026-03-30")], "event_day": [pd.Timestamp("2026-04-01")]})
    job.store.save_seo({"cohorts": cohorts, "skus": skus, "daily": daily, "buckets": buckets, "queries": queries})
    index["seo"] = {"as_of": "2026-04-02", "version": 1, "window": 14, "skus": 2, "cohorts": ["2026-03-30"], "complete": ["2026-03-30"]}
    job.store.save_index(index)

    monkeypatch.setenv("STORAGE", "local")
    monkeypatch.setenv("LOCAL_ROOT", str(root))
    monkeypatch.chdir(ROOT)
    return root


@pytest.mark.parametrize("page", PAGES)
def test_page_renders_without_errors(store_root, page):
    at = AppTest.from_file(os.path.join(ROOT, "streamlit_app.py"), default_timeout=60)
    at.session_state["authenticated"] = True
    at.run()
    at.switch_page(f"app/pages/{page}.py").run()
    assert not at.exception, [e.value for e in at.exception]
    assert at.title, "page rendered no title"


def test_resumen_warns_about_placeholder_stock(store_root):
    at = AppTest.from_file(os.path.join(ROOT, "streamlit_app.py"), default_timeout=60)
    at.session_state["authenticated"] = True
    at.run()
    assert any("STOCK = 1,000,000" in w.value for w in at.warning)


def test_preguntar_renders_with_api_key(store_root, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key-not-used")
    at = AppTest.from_file(os.path.join(ROOT, "streamlit_app.py"), default_timeout=60)
    at.session_state["authenticated"] = True
    at.run()
    at.switch_page("app/pages/preguntar.py").run()
    assert not at.exception, [e.value for e in at.exception]
    assert any("Tope diario" in m.label for m in at.metric)


def test_seo_impacto_shows_kpis_and_sku_detail(store_root):
    at = AppTest.from_file(os.path.join(ROOT, "streamlit_app.py"), default_timeout=60)
    at.session_state["authenticated"] = True
    at.run()
    at.switch_page("app/pages/seo_impacto.py").run()
    assert not at.exception, [e.value for e in at.exception]
    metrics = {m.label: m.value for m in at.metric}
    assert metrics["SKUs trabajados"] == "2" and metrics["Δ impresiones vs control"] == "+40%"
    at.text_input(key="seo_sku_q").set_value("1").run()
    assert not at.exception, [e.value for e in at.exception]
    assert any("Creció" in m.value for m in at.markdown)


def test_campanas_has_a_filter_button_per_pending_reason(store_root):
    # Clicking is checked in a browser: AppTest cannot replay a segmented_control whose
    # value was set from a callback ("content: 'P' is not in list").
    at = AppTest.from_file(os.path.join(ROOT, "streamlit_app.py"), default_timeout=60)
    at.session_state["authenticated"] = True
    at.run()
    at.switch_page("app/pages/campanas.py").run()
    assert not at.exception, [e.value for e in at.exception]
    buttons = {b.key: b for b in at.button if (b.key or "").startswith("see_")}
    assert set(buttons) == {"see_solo_inventario", "see_step", "see_fuera_catalogo"}
    assert not buttons["see_fuera_catalogo"].disabled          # SKU "9" of the test list is not in the export
