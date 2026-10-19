from __future__ import annotations

import pandas as pd
import pytest

from catalog_core.sku_history import rebuild_visible_history, update_visible_history, update_workflow_dates


def snap(**vis):
    return pd.DataFrame({"SKU": list(vis), "VISIBLE": ["Si" if v else "No" for v in vis.values()]})


def test_visible_history_incremental():
    h = update_visible_history(None, "2026-09-01", snap(a=True, b=False))
    h = update_visible_history(h, "2026-09-02", snap(a=True, b=True, c=False))
    h = update_visible_history(h, "2026-09-03", snap(a=False, b=True)).set_index("SKU")
    assert h.loc["a", "last_visible"] == pd.Timestamp("2026-09-02") and h.loc["a", "visible_streak"] == 0
    assert h.loc["a", "days_visible"] == 2
    assert h.loc["b", "visible_streak"] == 2 and h.loc["b", "first_seen"] == pd.Timestamp("2026-09-01")
    # c disappeared from the export on the 3rd: last_seen stays, never visible
    assert h.loc["c", "last_seen"] == pd.Timestamp("2026-09-02") and pd.isna(h.loc["c", "last_visible"])


def test_out_of_order_day_requires_rebuild():
    h = update_visible_history(None, "2026-09-02", snap(a=True))
    with pytest.raises(ValueError, match="rebuild"):
        update_visible_history(h, "2026-09-01", snap(a=False))
    r = rebuild_visible_history([("2026-09-02", snap(a=True)), ("2026-09-01", snap(a=False))]).set_index("SKU")
    assert r.loc["a", "first_seen"] == pd.Timestamp("2026-09-01") and r.loc["a", "visible_streak"] == 1


def test_workflow_dates_keep_latest_per_sku():
    rep1 = pd.DataFrame({"<ID>": [1, 2], "Fecha de Salida del Flujo de trabajo": ["2026-09-10", "2026-09-11"],
                         "Fecha_Fin_Dat_Log": ["2026-09-10 11:00", None], "Fecha_Fin_Edi_Mer": [None, None]})
    rep2 = pd.DataFrame({"<ID>": [1], "Fecha de Salida del Flujo de trabajo": ["2026-09-20"],
                         "Fecha_Fin_Dat_Log": ["2026-09-20 09:00"]})
    wf = update_workflow_dates(None, "Edición", rep1)
    wf = update_workflow_dates(wf, "Diseño", rep2).set_index("SKU")
    assert wf.loc["1", "salida_flujo"] == pd.Timestamp("2026-09-20") and wf.loc["1", "equipo"] == "Diseño"
    assert wf.loc["2", "equipo"] == "Edición" and pd.isna(wf.loc["2", "fin_dat_log"])


def test_work_log_keeps_every_worker_and_replaces_resent_reports():
    from catalog_core.sku_history import update_work_log, work_log_rows

    rep = lambda user, promo: pd.DataFrame({
        "<ID>": [756006.0, "830548"], "<Name>": ["Villa", "Silla"], "Usuario": [user, "BETO"],
        "Usuario Promueve desde Compras": [promo, None], "Fecha de Salida del Flujo de trabajo": ["2026-10-01"] * 2,
        "Fecha_Fin_Dat_Log": ["2026-10-06 13:59:52", None], "Fecha de Creación STEP": ["2020-12-07"] * 2})
    log = update_work_log(None, work_log_rows("Edición", "2026-10-02", rep("ACASTILLO - Ashley", "ACASTILLO - Ashley")))
    log = update_work_log(log, work_log_rows("Edición", "2026-10-03", rep("AD_EDICION - Diego", "ACASTILLO - Ashley")))
    r = log[log["SKU"] == "756006"]
    assert r["usuario"].tolist() == ["ACASTILLO - Ashley", "AD_EDICION - Diego"]
    assert r["promovio"].tolist() == ["ACASTILLO - Ashley"] * 2
    assert pd.isna(log[log["SKU"] == "830548"]["promovio"]).all()
    # the same day/team sent again replaces its rows instead of duplicating them
    log = update_work_log(log, work_log_rows("Edición", "2026-10-03", rep("OTRA", "OTRA")))
    assert log[log["SKU"] == "756006"]["usuario"].tolist() == ["ACASTILLO - Ashley", "OTRA"]
