from __future__ import annotations

import numpy as np
import pandas as pd
from pytest import approx

from catalog_core.seo_impact import (build_seo_impact, cohort_summary, control_pool, daily_index, events_from_work_log,
                                     listing_changes, match_controls, query_deltas, query_rollup, sku_status, window_metrics)

AS_OF = "2026-10-31"


def _wl(rows):
    return pd.DataFrame(rows, columns=["SKU", "dia_reporte", "equipo", "usuario", "salida_flujo", "fin_dat_log"])


def _daily(spec: dict[str, dict[str, int]]) -> pd.DataFrame:
    """{sku: {day: impressions}} → daily rows (clicks = impressions // 10, position 5)."""
    rows = [{"day": d, "SKU": s, "impressions": v, "clicks": v // 10, "position": 5.0} for s, days in spec.items() for d, v in days.items()]
    return pd.DataFrame(rows)


def _flat(sku: str, start: str, days: int, value: int) -> dict:
    return {sku: {(pd.Timestamp(start) + pd.Timedelta(days=i)).strftime("%Y-%m-%d"): value for i in range(days)}}


def test_events_latest_exit_wins_fallback_to_dat_log_and_both_teams():
    wl = _wl([["1", "2026-10-02", "Edición", "ana", "2026-10-01", "2026-09-30"],
              ["1", "2026-10-06", "Edición", "beto", "2026-10-05", None],
              ["1", "2026-10-07", "Diseño", "cris", "2026-10-05", None],
              ["2", "2026-10-02", "Diseño", "cris", None, "2026-10-01 14:00"],
              ["3", "2026-10-02", "Edición", "ana", None, None]])
    ev = events_from_work_log(wl).set_index("SKU")
    assert ev.loc["1", "event_day"] == pd.Timestamp("2026-10-05") and ev.loc["1", "equipo"] == "Diseño+Edición"
    assert ev.loc["1", "cohorte"] == pd.Timestamp("2026-10-05") and ev.loc["1", "usuarios"] == "ana, beto, cris"
    assert ev.loc["2", "event_day"] == pd.Timestamp("2026-10-01") and ev.loc["2", "equipo"] == "Diseño"
    assert "3" not in ev.index and events_from_work_log(None).empty


def test_window_metrics_skips_event_day_counts_missing_days_as_zero_and_flags_incomplete():
    ev = pd.DataFrame({"SKU": ["1", "2"], "event_day": pd.to_datetime(["2026-10-01", "2026-10-25"])})
    daily = _daily({"1": {"2026-09-20": 10, "2026-10-01": 999, "2026-10-05": 30}, "2": {"2026-10-20": 5}, "x": {"2026-09-01": 1}})
    m = window_metrics(daily, ev, AS_OF, window=14).set_index("SKU")
    assert m.loc["1", "impr_antes"] == 10 and m.loc["1", "impr_despues"] == 30 and m.loc["1", "dias_despues"] == 1
    assert m.loc["1", "delta_impr_pct"] == 200.0 and bool(m.loc["1", "completo"])
    assert not m.loc["2", "despues_completo"] and pd.isna(m.loc["2", "impr_despues"]) and m.loc["2", "impr_antes"] == 5


def test_sku_status_rules():
    base = {"completo": True, "event_day": pd.Timestamp("2026-10-01"), "primera_impresion": pd.NaT, "last_visible": pd.Timestamp("2026-10-30")}
    rows = pd.DataFrame([
        {**base, "impr_antes": 0, "impr_despues": 0},                                    # sin_presencia
        {**base, "impr_antes": 0, "impr_despues": 8, "primera_impresion": pd.Timestamp("2026-10-03")},   # nuevo
        {**base, "impr_antes": 0, "impr_despues": 8, "primera_impresion": pd.Timestamp("2026-08-03")},   # crecio (had presence long ago)
        {**base, "impr_antes": 100, "impr_despues": 140},                                # crecio
        {**base, "impr_antes": 100, "impr_despues": 60},                                 # bajo
        {**base, "impr_antes": 100, "impr_despues": 105},                                # estable
        {**base, "impr_antes": 3, "impr_despues": 6},                                    # estable (< MIN_IMPR)
        {**base, "impr_antes": 100, "impr_despues": 0},                                  # bajo
        {**base, "impr_antes": 100, "impr_despues": 150, "last_visible": pd.Timestamp("2026-09-20")},   # no_visible
        {**base, "impr_antes": 100, "impr_despues": 150, "completo": False},             # pendiente
    ])
    assert sku_status(rows).tolist() == ["sin_presencia", "nuevo", "crecio", "crecio", "bajo", "estable", "estable", "bajo",
                                         "no_visible", "pendiente"]
    # Without any visibility history the rule is not applied
    assert sku_status(rows.assign(last_visible=pd.NaT)).tolist()[8] == "crecio"


def test_control_pool_excludes_worked_and_not_visible_skus():
    events = pd.DataFrame({"SKU": ["1"], "event_day": pd.to_datetime(["2026-10-01"])})
    wl = _wl([["1", "2026-10-02", "Edición", "ana", "2026-10-01", None], ["9", "2026-10-10", "Edición", "ana", "2026-10-09", None]])
    vh = pd.DataFrame({"SKU": ["1", "5", "6", "9"], "first_seen": pd.to_datetime(["2026-07-01"] * 4),
                       "last_visible": pd.to_datetime(["2026-10-30", "2026-10-30", "2026-10-05", "2026-10-30"])})
    ctx = pd.DataFrame({"SKU": ["5"], "NIVEL 1": ["Hogar"]})
    pool = control_pool(events, wl, vh, ctx, pd.Timestamp("2026-10-01"), 14)
    assert pool["SKU"].tolist() == ["5"] and pool["NIVEL 1"].tolist() == ["Hogar"]


def test_match_controls_stratified_uplift():
    treated = pd.DataFrame({"SKU": ["t1", "t2"], "event_day": pd.to_datetime(["2026-10-01"] * 2), "impr_antes": [100.0, 5.0],
                            "impr_despues": [150.0, 10.0], "clics_antes": [10.0, 1.0], "clics_despues": [15.0, 2.0],
                            "delta_impr_pct": [50.0, 100.0], "delta_clics_pct": [50.0, 100.0], "NIVEL 1": ["Hogar", "Hogar"]})
    controls = pd.DataFrame({"SKU": [f"c{i}" for i in range(4)], "event_day": pd.to_datetime(["2026-10-01"] * 4),
                             "impr_antes": [200.0, 100.0, 4.0, 6.0], "impr_despues": [220.0, 110.0, 4.0, 6.0],
                             "clics_antes": [20.0, 10.0, 1.0, 1.0], "clics_despues": [21.0, 12.0, 1.0, 1.0], "NIVEL 1": ["Hogar"] * 4})
    m = match_controls(treated, controls, min_stratum=20).set_index("SKU")
    assert m.loc["t1", "ctrl_delta_impr_pct"] == approx(10.0) and m.loc["t1", "uplift_impr_pct"] == approx(40.0) and m.loc["t1", "ctrl_n"] == 2
    assert m.loc["t2", "ctrl_delta_impr_pct"] == 0.0 and m.loc["t2", "uplift_impr_pct"] == 100.0
    assert np.isnan(match_controls(treated, None).loc[0, "uplift_impr_pct"])


def test_build_seo_impact_end_to_end_and_pending_cohort():
    wl = _wl([["1", "2026-10-02", "Edición", "ana", "2026-10-01", None],
              ["2", "2026-10-02", "Edición", "ana", "2026-10-01", None],
              ["3", "2026-10-02", "Diseño", "cris", "2026-10-01", None],
              ["7", "2026-10-29", "Edición", "ana", "2026-10-28", None]])
    spec = {}
    spec.update(_flat("1", "2026-09-17", 14, 10)); spec["1"].update(_flat("1", "2026-10-02", 14, 15)["1"])   # +50 %
    spec.update(_flat("2", "2026-10-03", 14, 2))                                                            # nuevo
    for c in ("c1", "c2", "c3"):
        spec.update(_flat(c, "2026-09-10", 50, 10))                                                        # control flat
    daily = _daily(spec)
    vh = pd.DataFrame({"SKU": ["1", "2", "3", "7", "c1", "c2", "c3"], "first_seen": pd.to_datetime(["2026-07-01"] * 7),
                       "last_visible": pd.to_datetime(["2026-10-30"] * 7)})
    ctx = pd.DataFrame({"SKU": ["1", "2", "3", "c1", "c2", "c3"], "NIVEL 1": ["Hogar"] * 6})
    r = build_seo_impact(wl, daily, vh, ctx, AS_OF, window=14, n_boot=20)
    s = r.skus.set_index("SKU")
    assert s.loc["1", "estado"] == "crecio" and s.loc["2", "estado"] == "nuevo" and s.loc["2", "dias_a_primera_impresion"] == 2
    assert s.loc["3", "estado"] == "sin_presencia" and s.loc["7", "estado"] == "pendiente"
    assert s.loc["1", "ctrl_delta_impr_pct"] == 0.0 and s.loc["1", "uplift_impr_pct"] == 50.0
    todos = r.cohorts[(r.cohorts["equipo"] == "Todos")].set_index("cohorte")
    wk = todos.loc[pd.Timestamp("2026-09-28")]
    assert wk["skus"] == 3 and wk["completo"] and wk["nuevo"] == 1 and wk["crecio"] == 1 and wk["sin_presencia"] == 1
    # 140 → 210 (SKU 1) plus 0 → 26 (SKU 2, 13 days × 2) = +68.6 %; the control is flat
    assert wk["delta_impr_pct"] == approx(68.57, abs=0.01) and wk["uplift_impr_pct"] == approx(68.57, abs=0.01) and wk["muestra_pequena"]
    assert not todos.loc[pd.Timestamp("2026-10-26"), "completo"]
    assert set(r.cohorts["equipo"]) == {"Todos", "Edición", "Diseño"}
    b = r.buckets[(r.buckets["equipo"] == "Todos") & (r.buckets["bucket"] == "100–999")].iloc[0]
    assert b["n"] == 1 and b["delta_impr_pct"] == 50.0 and b["ctrl_n"] == 3 and b["ctrl_delta_impr_pct"] == 0.0
    d = r.daily[(r.daily["equipo"] == "Todos") & (r.daily["grupo"] == "trabajados")].set_index("offset")
    assert d.index.min() == -14 and d.index.max() == 14 and abs(d.loc[-14:-1, "indice"].mean() - 100) < 1e-6
    assert d.loc[1, "indice"] > 100 and (r.daily["grupo"] == "control").any()


def test_daily_index_empty_inputs():
    assert daily_index(pd.DataFrame(), None, None).empty
    assert cohort_summary(pd.DataFrame(), AS_OF).empty


def test_query_deltas_and_rollup():
    events = pd.DataFrame({"SKU": ["1"], "event_day": pd.to_datetime(["2026-10-01"]), "cohorte": pd.to_datetime(["2026-09-28"])})
    rows = []
    for d in ("2026-09-25", "2026-09-26"):
        rows += [{"day": d, "SKU": "1", "query": "cortina", "impressions": 20, "clicks": 2, "position": 12.0, "anonima": False, "ml": False, "ps": False},
                 {"day": d, "SKU": "1", "query": "vieja", "impressions": 10, "clicks": 0, "position": 8.0, "anonima": False, "ml": False, "ps": False}]
    for d in ("2026-10-03", "2026-10-04"):
        rows += [{"day": d, "SKU": "1", "query": "cortina", "impressions": 30, "clicks": 5, "position": 6.0, "anonima": False, "ml": False, "ps": True},
                 {"day": d, "SKU": "1", "query": "cortina baño", "impressions": 15, "clicks": 1, "position": 9.0, "anonima": False, "ml": False, "ps": False},
                 {"day": d, "SKU": "1", "query": "(anónimas)", "impressions": 50, "clicks": 1, "position": 9.0, "anonima": True, "ml": False, "ps": False}]
    rows.append({"day": "2026-10-01", "SKU": "1", "query": "evento", "impressions": 1, "clicks": 0, "position": 1.0, "anonima": False, "ml": False, "ps": False})
    q = query_deltas(events, pd.DataFrame(rows), window=14).set_index("query")
    assert q.loc["cortina", "estado"] == "subio" and q.loc["cortina", "pos_antes"] == 12.0 and q.loc["cortina", "pos_despues"] == 6.0
    assert q.loc["cortina baño", "estado"] == "nueva" and q.loc["vieja", "estado"] == "perdida" and q.loc["(anónimas)", "anonima"]
    assert "evento" not in q.index and bool(q.loc["cortina", "ps"])
    roll = query_rollup(q.reset_index()).set_index("SKU").loc["1"]
    assert roll["consultas_nuevas"] == 1 and roll["consultas_perdidas"] == 1 and roll["consultas_subieron"] == 1
    assert roll["top_consulta_despues"] == "cortina"
    assert query_deltas(events, None).empty and query_rollup(None).empty


def test_listing_changes_from_flags(make_catalog, make_sku):
    from catalog_core import build_flags

    before = build_flags(make_catalog(make_sku("1", **{"DESCRIPCION ERP": "", "TIENE IMAGEN": "No", "IMAGEN PRIMARIA": "No",
                                                        "URL IMAGEN": ""}), make_sku("2")))
    after = build_flags(make_catalog(make_sku("1"), make_sku("2", MARCA=""), make_sku("3")))
    lc = listing_changes(before, after, pd.Series(["1", "2", "3", "4"])).set_index("SKU")
    assert lc.loc["1", "cambios_ficha"] == "+descripción, +imagen" and lc.loc["1", "score_antes"] < lc.loc["1", "score_despues"]
    assert lc.loc["2", "cambios_ficha"] == "−marca" and lc.loc["3", "cambios_ficha"] == "nuevo en el catálogo"
    assert "4" not in lc.index and listing_changes(None, after, pd.Series(["1"])).empty
