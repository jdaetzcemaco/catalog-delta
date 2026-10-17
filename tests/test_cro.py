from __future__ import annotations

import pandas as pd

from catalog_core.cro import CROThresholds, classify, content_vs_conversion, window_frame

T = CROThresholds(prior=100)


def _perf(rows):
    base = {"NIVEL 1": "Hogar", "visible": True, "con_stock": True, "vistas_ventana": 0, "cart_rate": 0.0,
            "buy_rate": 0.0, "paid_views": 0.0, "paid_buy_rate": float("nan"), "vistas_7d": 0,
            "organic_sessions_7d": 0, "organic_sessions_7d_prev": 0, "content_score": 90,
            "carrito_30d": 0, "compras_30d": 0, "ingresos_30d": 0.0}
    return pd.DataFrame([{**base, **r} for r in rows])


def _category(n=20, **extra):
    # A healthy category: 10% of viewers add to cart, 3% buy
    return [{"SKU": f"c{i}", "vistas_ventana": 1000, "cart_rate": 0.10, "buy_rate": 0.03, **extra} for i in range(n)]


def groups(df):
    return {(r.SKU, r.grupo) for r in df.itertuples()}


def test_leak_and_cart_abandonment_against_category():
    perf = _perf(_category() + [
        {"SKU": "leak", "vistas_ventana": 2000, "cart_rate": 0.01, "buy_rate": 0.003},
        {"SKU": "abandon", "vistas_ventana": 1000, "cart_rate": 0.20, "buy_rate": 0.004},
        {"SKU": "tiny", "vistas_ventana": 20, "cart_rate": 0.0, "buy_rate": 0.0},
    ])
    out = classify(perf, T)
    g = groups(out)
    assert ("leak", "fuga_conversion") in g and ("abandon", "carrito_sin_compra") in g
    assert not any(s == "tiny" for s, _ in g)
    row = out[out["SKU"] == "leak"].iloc[0]
    assert "carrito" in row["motivo"] and row["impacto"] > 0
    # Rates are per person, so none can exceed 100%
    assert (out["tasa_carrito"] <= 1).all() and (out["compra_por_carrito"] <= 1).all()


def test_shrinkage_protects_small_samples():
    perf = _perf(_category() + [{"SKU": "lucky", "vistas_ventana": 40, "cart_rate": 0.2, "buy_rate": 0.05}])
    assert ("lucky", "joya_oculta") not in groups(classify(perf, T))
    perf = _perf(_category() + [{"SKU": "gem", "vistas_ventana": 300, "cart_rate": 0.3, "buy_rate": 0.15}])
    assert ("gem", "joya_oculta") in groups(classify(perf, T))


def test_paid_organic_stock_and_non_physical():
    perf = _perf(_category(paid_views=500, paid_buy_rate=0.03) + [
        {"SKU": "paid", "vistas_ventana": 1200, "cart_rate": 0.1, "buy_rate": 0.03, "paid_views": 1000, "paid_buy_rate": 0.0},
        {"SKU": "seo", "vistas_ventana": 500, "cart_rate": 0.1, "buy_rate": 0.03,
         "organic_sessions_7d_prev": 100, "organic_sessions_7d": 30},
        {"SKU": "oos", "vistas_ventana": 500, "vistas_7d": 120, "con_stock": False},
        {"SKU": "gift", "NIVEL 1": "Certificados De Regalo", "vistas_ventana": 500, "vistas_7d": 300, "con_stock": False},
        {"SKU": "hidden", "vistas_ventana": 2000, "cart_rate": 0.005, "visible": False},
    ])
    g = groups(classify(perf, T))
    assert ("paid", "pagado_sin_retorno") in g and ("seo", "organico_en_riesgo") in g and ("oos", "trafico_sin_stock") in g
    assert not any(s in ("gift", "hidden") for s, _ in g)


def test_without_window_rates_only_traffic_groups_run():
    perf = _perf([{"SKU": "oos", "vistas_7d": 120, "con_stock": False}]).drop(columns=["cart_rate", "buy_rate"])
    assert groups(classify(perf, T)) == {("oos", "trafico_sin_stock")}


def test_window_frame_combines_paid_channels():
    rates = pd.DataFrame({"SKU": ["1"], "itemsViewed": [400.0], "cartToViewRate": [0.1], "purchaseToViewRate": [0.03]})
    ch = pd.DataFrame({"SKU": ["1", "1", "1"], "bucket": ["organico", "pagado_social", "pagado_google"],
                       "views": [200.0, 100.0, 100.0], "buyers": [10.0, 1.0, 3.0]})
    w = window_frame(rates, ch).set_index("SKU")
    assert w.loc["1", "paid_views"] == 200 and w.loc["1", "paid_buy_rate"] == 0.02


def test_content_vs_conversion_bands():
    perf = _perf([{"SKU": "a", "vistas_ventana": 100, "cart_rate": 0.2, "buy_rate": 0.05, "content_score": 95},
                  {"SKU": "b", "vistas_ventana": 100, "cart_rate": 0.05, "buy_rate": 0.01, "content_score": 50},
                  {"SKU": "c", "vistas_ventana": 10, "cart_rate": 0.5, "buy_rate": 0.5, "content_score": 50}])
    out = content_vs_conversion(perf).set_index("banda")
    assert out.loc["90–95", "tasa_carrito"] == 0.2 and out.loc["< 60", "skus"] == 1
