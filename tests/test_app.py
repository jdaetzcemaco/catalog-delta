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
PAGES = ["resumen", "cambios", "inventario", "historial", "productividad", "cargar"]


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
        [make_sku("1", VISIBLE="No"), make_sku("2"), make_sku("4", **{"HABILITADO/DESHABILITADO": "Deshabilitado"})]),
        "SKUs"))
    prod = pd.DataFrame({
        "<ID>": [1, 2], "<Name>": ["A", "B"], "Categoría": ["X", "Y"], "Usuario": ["ana", "beto"],
        "Usuario Promueve desde Catalogo": ["ana", None], "Fecha de Salida del Flujo de trabajo": ["2026-04-01"] * 2,
        "Total Omnicanal": [3, 0],
    })
    (incoming / "productivity-diseno-2026-04-02.xlsx").write_bytes(_xlsx(prod, "Sheet1"))
    edicion = prod.rename(columns={"Usuario Promueve desde Catalogo": "Usuario Promueve desde Compras"})
    (incoming / "productivity-edicion-2026-04-02.xlsx").write_bytes(_xlsx(edicion, "Sheet1"))
    Job(Settings(storage="local", local_root=str(root)), LocalStorage(str(root))).run()

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
