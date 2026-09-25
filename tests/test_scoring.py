from catalog_core import SCORE_WEIGHTS, build_flags, build_summary


def test_complete_sku_scores_max(make_sku, make_catalog):
    f = build_flags(make_catalog(make_sku("1")))
    assert f.loc[0, "content_score"] == sum(SCORE_WEIGHTS.values()) == 95


def test_each_missing_attribute_costs_its_weight(make_sku, make_catalog):
    f = build_flags(make_catalog(
        make_sku("no-img", **{"TIENE IMAGEN": "No", "IMAGEN PRIMARIA": "No", "URL IMAGEN": ""}),
        make_sku("no-desc", **{"DESCRIPCION ERP": ""}),
        make_sku("no-price", **{"TIENE PRECIO": "No", "PRECIO": "0"}),
        make_sku("no-brand", MARCA=""),
        make_sku("hidden", VISIBLE="No"),
        make_sku("no-name", **{"NOMBRE DE SKU": "", "NOMBRE DE PRODUCTO": ""}),
        make_sku("one-level", **{"NIVEL 2": "", "NIVEL 3": ""}),
    )).set_index("SKU")["content_score"]
    assert f["no-img"] == 95 - 25
    assert f["no-desc"] == 95 - 15
    assert f["no-price"] == 95 - 15
    assert f["no-brand"] == 95 - 5
    assert f["hidden"] == 95 - 10
    assert f["no-name"] == 95 - 10
    assert f["one-level"] == 95 - 10


def test_any_image_or_price_signal_counts(make_sku, make_catalog):
    f = build_flags(make_catalog(
        make_sku("url-only", **{"TIENE IMAGEN": "No", "IMAGEN PRIMARIA": "No"}),
        make_sku("price-value-only", **{"TIENE PRECIO": "No"}),
        make_sku("stock-flag-only", STOCK="0"),
        make_sku("stock-value-only", **{"TIENE STOCK": "No"}),
    )).set_index("SKU")
    assert f.loc["url-only", "has_image"] == 1
    assert f.loc["price-value-only", "has_price"] == 1
    assert f.loc["stock-flag-only", "has_stock"] == 1
    assert f.loc["stock-value-only", "has_stock"] == 1


def test_yes_values_are_case_and_accent_insensitive(make_sku, make_catalog):
    f = build_flags(make_catalog(*[make_sku(str(i), VISIBLE=v) for i, v in enumerate(["SI", "sí", "Yes", "true", "1", "No", ""])]))
    assert f["is_visible"].tolist() == [1, 1, 1, 1, 1, 0, 0]


def test_summary(make_sku, make_catalog):
    f = build_flags(make_catalog(make_sku("1"), make_sku("2", VISIBLE="No", STOCK="0", **{"TIENE STOCK": "No"})))
    s = build_summary(f).iloc[0]
    assert s["Total SKUs"] == 2
    assert s["Visible"] == 1
    assert s["Visible %"] == 50.0
    assert s["With Stock %"] == 50.0
    assert s["Avg Content Score"] == 90.0
    # Weights sum to 95, so this is always 0 (kept as in v1; see docs/CHANGES.md)
    assert s["Score = 100"] == 0
