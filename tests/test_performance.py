from __future__ import annotations

import pandas as pd

from catalog_core.performance import (daily_gsc_frame, daily_landing_frame, daily_product_frame, merge_days,
                                      product_aggregates, rolling_aggregates, sku_performance)


def _items(day, rows):
    return pd.DataFrame([{"day": day, "itemId": pid, "itemName": "x", "itemsViewed": v, "itemsAddedToCart": c,
                          "itemsPurchased": p, "itemRevenue": r} for pid, v, c, p, r in rows])


def _landing(day, rows):
    df = pd.DataFrame([{"day": day, "landingPagePlusQueryString": f"/p-{sku}/p", "sessionDefaultChannelGroup": ch,
                        "sessions": s, "engagedSessions": s} for sku, ch, s in rows])
    df["SKU"] = df["landingPagePlusQueryString"].str.extract(r"-(\d+)/p")[0]
    return df


def test_product_and_sku_aggregates():
    items = {"2026-10-01": _items("2026-10-01", [("103421396", 10, 2, 1, 50.0)]),
             "2026-10-10": _items("2026-10-10", [("103421396", 30, 3, 0, 0.0), ("103542062", 5, 0, 0, 0.0)])}
    landing = {"2026-10-10": _landing("2026-10-10", [("1001", "Organic Search", 7), ("1001", "Direct", 3)])}
    dp, dl = daily_product_frame(items), daily_landing_frame(landing)
    product = product_aggregates(dp, "2026-10-10").set_index("product_id")
    assert product.loc["103421396", "vistas_7d"] == 30 and product.loc["103421396", "vistas_30d"] == 40
    assert product.loc["103421396", "vistas_7d_prev"] == 10 and product.loc["103421396", "ingresos_90d"] == 50.0
    assert product.loc["103421396", "ultimo_dia_vistas"] == pd.Timestamp("2026-10-10")
    assert product.loc["103421396", "tasa_carrito_30d"] == round(5 / 40, 4)

    landing_perf = rolling_aggregates(dl, "SKU", ["sessions", "organic_sessions"], "2026-10-10", "organic_sessions")
    # Two SKUs of one product share its metrics; SKU-level sessions stay per SKU
    sku_map = pd.DataFrame({"SKU": ["1001", "1002", "2001"], "PRODUCT ID": ["103421396.0", "103421396", "103542062"]})
    perf = sku_performance(sku_map, product.reset_index(), landing_perf, None).set_index("SKU")
    assert perf.loc["1001", "vistas_30d"] == 40 and perf.loc["1002", "vistas_30d"] == 40
    assert perf.loc["1001", "skus_del_producto"] == 2 and perf.loc["2001", "skus_del_producto"] == 1
    assert perf.loc["1001", "organic_sessions_7d"] == 7 and perf.loc["1002", "organic_sessions_7d"] == 0


def test_gsc_frame_weights_position_by_impressions():
    pages = pd.DataFrame({"day": ["d"] * 3, "page": ["/a-1001/p", "/a-1001/p?x", "/home"], "SKU": ["1001", "1001", None],
                          "clicks": [5, 1, 100], "impressions": [100, 300, 1000], "ctr": [0, 0, 0], "position": [2.0, 10.0, 1.0]})
    g = daily_gsc_frame({"2026-10-10": pages}).set_index("SKU")
    assert g.loc["1001", "clicks"] == 6 and g.loc["1001", "position"] == 8.0


def test_merge_days_replaces_and_trims():
    old = pd.DataFrame({"day": pd.to_datetime(["2026-01-01", "2026-10-09", "2026-10-10"]), "SKU": ["a", "a", "a"], "x": [1, 1, 1]})
    fresh = pd.DataFrame({"day": pd.to_datetime(["2026-10-10"]), "SKU": ["a"], "x": [9]})
    out = merge_days(old, fresh, keep_days=120, as_of="2026-10-10")
    assert out["x"].tolist() == [1, 9] and out["day"].min() == pd.Timestamp("2026-10-09")


def test_empty_inputs():
    assert daily_product_frame({}).empty and daily_landing_frame({}).empty
    assert product_aggregates(daily_product_frame({}), "2026-10-10").empty
