from __future__ import annotations

import pandas as pd

from catalog_core import build_flags
from catalog_core.alerts import (AlertThresholds, catalog_alerts, export_late_alert, finalize, list_alerts,
                                 new_visible_weak_alert, tipo_c_stuck_alert)
from catalog_core.campaigns import readiness

T = AlertThresholds()


def _manifest(visible=1000, score=86.0, placeholder_pct=0.0, snv=100):
    return {"summary": {"Visible": visible, "Avg Content Score": score},
            "quality": {"stock_placeholder": int(placeholder_pct * 10), "stock_placeholder_pct": placeholder_pct},
            "inventory_kpis": {"stock_no_visible": snv}}


def test_catalog_rules_fire_only_past_thresholds():
    quiet = catalog_alerts("d", _manifest(), _manifest(), T)
    assert quiet == []
    fired = catalog_alerts("d", _manifest(visible=900, score=84.5, placeholder_pct=50, snv=140), _manifest(), T)
    assert {a.rule for a in fired} == {"quality_stock_placeholder", "visible_drop", "score_drop", "stock_no_visible_jump"}
    assert fired[1].count == 100 and fired[1].severity == "alta"
    # No previous day: only the data-quality rule can fire
    assert [a.rule for a in catalog_alerts("d", _manifest(placeholder_pct=50), None, T)] == ["quality_stock_placeholder"]


def test_list_rules(make_catalog, make_sku):
    raw_y = make_catalog(make_sku("a"), make_sku("b"), make_sku("c"))
    raw_t = make_catalog(make_sku("a", VISIBLE="No"), make_sku("b"), make_sku("c", VISIBLE="No"))
    y = readiness(["a", "b", "c"], raw_y, build_flags(raw_y))
    t = readiness(["a", "b", "c"], raw_t, build_flags(raw_t))
    alerts = list_alerts("d", {"nav": {"name": "Navidad"}}, {"nav": t}, {"nav": y}, T)
    rules = {a.rule: a for a in alerts}
    assert rules["list_regression"].skus == ["a", "c"] and "Navidad" in rules["list_regression"].title
    assert rules["list_low_ready"].context["pct"] == 33.3
    # Fully ready list: nothing
    assert list_alerts("d", {}, {"nav": y}, {"nav": None}, T) == []


def test_new_visible_weak_and_tipo_c_stuck():
    new = pd.DataFrame({"SKU": [str(i) for i in range(30)], "is_visible": [1] * 30, "content_score": [70] * 30})
    assert new_visible_weak_alert("d", new, T)[0].count == 30
    assert new_visible_weak_alert("d", new.head(5), T) == []

    tipo_c = pd.DataFrame({"SKU": ["old", "new"]})
    hist = pd.DataFrame({"SKU": ["old", "new"], "first_seen": [pd.Timestamp("2026-08-01"), pd.Timestamp("2026-09-28")]})
    out = tipo_c_stuck_alert("2026-09-29", tipo_c, hist, T)
    assert out[0].skus == ["old"]


def test_export_late():
    assert export_late_alert("2026-09-29", "2026-09-28", T) == []
    assert export_late_alert("2026-09-29", "2026-09-25", T)[0].count == 4


def test_finalize_keeps_first_seen_and_orders_by_severity():
    a = catalog_alerts("2026-09-29", _manifest(visible=900, score=84.5), _manifest(), T)
    prev = [{"id": "catalog:visible_drop", "first_seen": "2026-09-20"}]
    out = finalize(a, prev, "now")
    assert [x["severity"] for x in out] == ["alta", "media"]
    assert out[0]["first_seen"] == "2026-09-20" and out[1]["first_seen"] == "2026-09-29"


def test_traffic_and_organic_drop_rules():
    from catalog_core.alerts import organic_drop_alerts, traffic_alerts

    info = pd.DataFrame({"SKU": [str(i) for i in range(8)], "VISIBLE": ["Si"] * 8,
                         "TIENE STOCK": ["No"] * 6 + ["Si", "No"], "STOCK": ["0"] * 6 + ["5", "1000000"]})
    perf = pd.DataFrame({"SKU": [str(i) for i in range(8)], "vistas_7d": [100] * 8, "carrito_7d": [3] * 8,
                         "organic_sessions_7d": [5] * 8, "organic_sessions_7d_prev": [40] * 8})
    out = traffic_alerts("d", perf, info, T)
    # 6 visible, no stock, reliable; "6" has stock and "7" is a placeholder -> excluded
    assert out[0].count == 6 and "7" not in out[0].skus and out[0].severity == "alta"
    assert traffic_alerts("d", perf.assign(vistas_7d=10), info, T) == []

    status = pd.DataFrame({"SKU": ["0", "1"], "listo": [True, True]})
    drops = organic_drop_alerts("d", {"nav": {"name": "Navidad"}}, {"nav": status}, perf, T)
    assert drops[0].count == 2 and "Navidad" in drops[0].title
    assert organic_drop_alerts("d", {}, {"nav": status}, perf.assign(organic_sessions_7d_prev=10), T) == []
