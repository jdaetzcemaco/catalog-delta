from __future__ import annotations

import pandas as pd

from catalog_core.performance import daily_sku_frame, rolling_aggregates


def _items(day, rows):
    return pd.DataFrame([{"day": day, "itemId": sku, "itemName": "x", "itemsViewed": v, "itemsAddedToCart": c,
                          "itemsPurchased": p, "itemRevenue": r} for sku, v, c, p, r in rows])


def _landing(day, rows):
    df = pd.DataFrame([{"day": day, "landingPagePlusQueryString": f"/p-{sku}/p", "sessionDefaultChannelGroup": ch,
                        "sessions": s, "engagedSessions": s} for sku, ch, s in rows])
    df["SKU"] = df["landingPagePlusQueryString"].str.extract(r"-(\d+)/p")[0]
    return df


def test_daily_frame_and_rolling_windows():
    items = {"2026-10-01": _items("2026-10-01", [("1001", 10, 2, 1, 50.0)]),
             "2026-10-10": _items("2026-10-10", [("1001", 30, 3, 0, 0.0), ("2002", 5, 0, 0, 0.0)])}
    landing = {"2026-10-10": _landing("2026-10-10", [("1001", "Organic Search", 7), ("1001", "Direct", 3)])}
    daily = daily_sku_frame(items, landing)
    assert len(daily) == 3 and daily["organic_sessions"].sum() == 7

    perf = rolling_aggregates(daily, "2026-10-10").set_index("SKU")
    assert perf.loc["1001", "vistas_7d"] == 30 and perf.loc["1001", "vistas_30d"] == 40
    assert perf.loc["1001", "vistas_7d_prev"] == 10          # 10-01 falls in the previous 7-day window
    assert perf.loc["1001", "compras_90d"] == 1 and perf.loc["1001", "ingresos_90d"] == 50.0
    assert perf.loc["1001", "organic_sessions_7d"] == 7 and perf.loc["2002", "sessions_7d"] == 0
    assert perf.loc["1001", "ultima_vista"] == pd.Timestamp("2026-10-10")
    assert perf.loc["1001", "tasa_carrito_30d"] == round(5 / 40, 4)


def test_empty_inputs():
    assert daily_sku_frame({}, {}).empty
    assert rolling_aggregates(daily_sku_frame({}, {}), "2026-10-10").empty
