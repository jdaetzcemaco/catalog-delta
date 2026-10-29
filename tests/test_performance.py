from __future__ import annotations

import pandas as pd

from catalog_core.performance import (daily_gsc_frame, daily_item_frame, daily_landing_frame, item_aggregates,
                                      merge_days, rolling_aggregates, sku_performance)


def _items(day, rows):
    return pd.DataFrame([{"day": day, "itemId": "100", "itemVariant": sku, "SKU": sku, "itemName": "x", "itemsViewed": v,
                          "itemsAddedToCart": c, "itemsPurchased": p, "itemRevenue": r} for sku, v, c, p, r in rows])


def _landing(day, rows):
    df = pd.DataFrame([{"day": day, "landingPagePlusQueryString": f"/p-{sku}/p", "sessionDefaultChannelGroup": ch,
                        "sessions": s, "engagedSessions": s} for sku, ch, s in rows])
    df["SKU"] = df["landingPagePlusQueryString"].str.extract(r"-(\d+)/p")[0]
    return df


def test_item_and_sku_aggregates():
    items = {"2026-10-01": _items("2026-10-01", [("1001", 10, 2, 1, 50.0)]),
             "2026-10-10": _items("2026-10-10", [("1001", 30, 3, 0, 0.0), ("2001", 5, 0, 0, 0.0)])}
    landing = {"2026-10-10": _landing("2026-10-10", [("1001", "Organic Search", 7), ("1001", "Direct", 3)])}
    di, dl = daily_item_frame(items), daily_landing_frame(landing)
    item = item_aggregates(di, "2026-10-10").set_index("SKU")
    assert item.loc["1001", "vistas_7d"] == 30 and item.loc["1001", "vistas_30d"] == 40
    assert item.loc["1001", "vistas_7d_prev"] == 10 and item.loc["1001", "ingresos_90d"] == 50.0
    assert item.loc["1001", "ultimo_dia_vistas"] == pd.Timestamp("2026-10-10")
    assert item.loc["1001", "tasa_carrito_30d"] == round(5 / 40, 4)

    landing_perf = rolling_aggregates(dl, "SKU", ["sessions", "organic_sessions"], "2026-10-10", "organic_sessions")
    sku_map = pd.DataFrame({"SKU": ["1001", "1002", "2001"], "PRODUCT ID": ["100.0", "100", "200"]})
    perf = sku_performance(sku_map, item.reset_index(), landing_perf, None).set_index("SKU")
    assert perf.loc["1001", "vistas_30d"] == 40 and perf.loc["1002", "vistas_30d"] == 0  # per SKU, not shared
    assert perf.loc["1001", "product_id"] == "100" and perf.loc["1002", "product_id"] == "100"
    assert perf.loc["1001", "organic_sessions_7d"] == 7 and perf.loc["2001", "organic_sessions_7d"] == 0


def test_gsc_frame_weights_position_by_impressions():
    pages = pd.DataFrame({"day": ["d"] * 3, "page": ["/a-1001/p", "/a-1001/p?x", "/home"], "SKU": ["1001", "1001", None],
                          "clicks": [5, 1, 100], "impressions": [100, 300, 1000], "ctr": [0, 0, 0], "position": [2.0, 10.0, 1.0]})
    g = daily_gsc_frame({"2026-10-10": pages}).set_index("SKU")
    assert g.loc["1001", "clicks"] == 6 and g.loc["1001", "position"] == 8.0
    assert g.loc["1001", "consultas"] == 0 and g.loc["1001", "impressions_ps"] == 0   # API days carry zeros
    bq = pages.assign(consultas=[3, 2, 9], impressions_ps=[10, 0, 0], impressions_ml=0, impressions_mobile=[50, 100, 0])
    g2 = daily_gsc_frame({"2026-10-11": bq}).set_index("SKU")
    assert g2.loc["1001", "consultas"] == 5 and g2.loc["1001", "impressions_ps"] == 10 and g2.loc["1001", "impressions_mobile"] == 150


def test_merge_days_replaces_and_trims():
    old = pd.DataFrame({"day": pd.to_datetime(["2026-01-01", "2026-10-09", "2026-10-10"]), "SKU": ["a", "a", "a"], "x": [1, 1, 1]})
    fresh = pd.DataFrame({"day": pd.to_datetime(["2026-10-10"]), "SKU": ["a"], "x": [9]})
    out = merge_days(old, fresh, keep_days=120, as_of="2026-10-10")
    assert out["x"].tolist() == [1, 9] and out["day"].min() == pd.Timestamp("2026-10-09")


def test_empty_inputs():
    assert daily_item_frame({}).empty and daily_landing_frame({}).empty
    assert item_aggregates(daily_item_frame({}), "2026-10-10").empty


def test_aggregate_queries_top_per_sku_and_site():
    from catalog_core.search_queries import aggregate_queries

    day = lambda d, rows: pd.DataFrame([{"day": d, "SKU": sku, "query": q, "impressions": i, "clicks": c,
                                          "position": p, "anonima": q == "(anónimas)"} for sku, q, i, c, p in rows])
    days = [day("2026-10-01", [("1", "sarten", 10, 1, 4.0), ("1", "(anónimas)", 3, 0, 9.0), ("2", "sarten", 5, 0, 8.0)]),
            day("2026-10-02", [("1", "sarten", 30, 2, 2.0), ("2", "silla", 7, 1, 3.0)])] + \
        [day(f"2026-10-{d:02d}", [("3", "x", 1, 0, 1.0)]) for d in range(3, 12)]   # forces a weekly fold
    per_sku, site = aggregate_queries(iter(days))
    s1 = per_sku[(per_sku["SKU"] == "1") & (per_sku["query"] == "sarten")].iloc[0]
    assert (s1["impresiones"], s1["clics"], s1["dias"]) == (40, 3, 2) and s1["posicion"] == 2.5   # (10*4+30*2)/40
    assert per_sku[(per_sku["SKU"] == "1") & (per_sku["query"] == "(anónimas)")]["impresiones"].tolist() == [3]
    top = site.set_index("query")
    assert top.loc["sarten", "impresiones"] == 45 and top.loc["sarten", "skus"] == 2
    assert "(anónimas)" not in top.index and top.loc["x", "impresiones"] == 9
    assert aggregate_queries(iter([]))[0].empty
