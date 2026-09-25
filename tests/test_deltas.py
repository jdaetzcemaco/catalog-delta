from catalog_core import build_flags, change_tables, compute_deltas, sku_changes


def _run(make_catalog, today_rows, yesterday_rows):
    t, y = build_flags(make_catalog(*today_rows)), build_flags(make_catalog(*yesterday_rows))
    return t, y, compute_deltas(t, y)


def test_new_and_removed_skus(make_sku, make_catalog):
    t, y, _ = _run(make_catalog, [make_sku("a"), make_sku("new")], [make_sku("a"), make_sku("gone")])
    c = sku_changes(t, y)
    assert c["new"] == {"new"} and c["removed"] == {"gone"} and c["net"] == 0


def test_visibility_changes_ignore_new_and_removed(make_sku, make_catalog):
    t, y, m = _run(
        make_catalog,
        [make_sku("up"), make_sku("down", VISIBLE="No"), make_sku("new")],
        [make_sku("up", VISIBLE="No"), make_sku("down"), make_sku("gone")],
    )
    tables = change_tables(t, y, m)
    assert tables["Newly Visible"]["SKU"].tolist() == ["up"]
    assert tables["No Longer Visible"]["SKU"].tolist() == ["down"]


def test_attribute_flips_and_big_score_moves(make_sku, make_catalog):
    no_img = {"TIENE IMAGEN": "No", "IMAGEN PRIMARIA": "No", "URL IMAGEN": ""}
    t, y, m = _run(
        make_catalog,
        [make_sku("img", **no_img), make_sku("stock", STOCK="0", **{"TIENE STOCK": "No"}), make_sku("same")],
        [make_sku("img"), make_sku("stock"), make_sku("same")],
    )
    tables = change_tables(t, y, m)
    assert tables["Image Changes"]["SKU"].tolist() == ["img"]
    assert tables["Stock Flips"]["SKU"].tolist() == ["stock"]
    # Losing the image is -25 points (>= 10); stock does not affect the score
    assert tables["Score Changes"]["SKU"].tolist() == ["img"]
    assert tables["Price Changes"].empty


def test_top_priorities_and_stock_not_visible(make_sku, make_catalog):
    t, y, m = _run(
        make_catalog,
        [
            make_sku("weak", **{"DESCRIPCION ERP": "", "MARCA": ""}),  # 75, visible, stock
            make_sku("ok"),
            make_sku("hidden", VISIBLE="No"),
        ],
        [make_sku("weak"), make_sku("ok"), make_sku("hidden")],
    )
    tables = change_tables(t, y, m)
    assert tables["Top Priorities"]["SKU"].tolist() == ["weak"]
    assert tables["Stock Not Visible"]["SKU"].tolist() == ["hidden"]
