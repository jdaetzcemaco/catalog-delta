from __future__ import annotations

import pandas as pd

from pipeline.ga4 import path_to_sku, summarize_landing


def test_path_to_sku():
    assert path_to_sku("/papel-higienico-4-rollos-cottonelle-ultra-830548/p") == "830548"
    assert path_to_sku("https://cemaco.com/TELESCOPIO-ASTRONOMICO-2550-1144749b/p") is None  # suffix letter: not a SKU
    assert path_to_sku("/telescopio-1144749/p?utm_source=x#top") == "1144749"
    assert path_to_sku("/juguetes") is None and path_to_sku(None) is None


def test_summarize_landing():
    df = pd.DataFrame({
        "landingPagePlusQueryString": ["/a-1001/p", "/a-1001/p", "/b-2002/p", "/home"],
        "sessionDefaultChannelGroup": ["Organic Search", "Direct", "Paid Search", "Direct"],
        "sessions": [10, 5, 3, 100], "engagedSessions": [8, 2, 1, 50],
    })
    df["SKU"] = df["landingPagePlusQueryString"].map(path_to_sku)
    out = summarize_landing(df).set_index("SKU")
    assert out.loc["1001", "sessions"] == 15 and out.loc["1001", "organic_sessions"] == 10
    assert out.loc["2002", "organic_sessions"] == 0
