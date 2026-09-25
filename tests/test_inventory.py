from catalog_core import build_flags, build_inventory

HIDDEN = {"VISIBLE": "No"}
DISABLED = {"HABILITADO/DESHABILITADO": "Deshabilitado"}
NO_STOCK = {"STOCK": "0", "TIENE STOCK": "No"}
NO_IMAGE = {"TIENE IMAGEN": "No", "IMAGEN PRIMARIA": "No", "URL IMAGEN": ""}


def _inv(make_catalog, rows, yesterday_rows=None):
    today = make_catalog(*rows)
    yesterday = make_catalog(*yesterday_rows) if yesterday_rows else None
    return build_inventory(today, build_flags(today), yesterday)


def skus(inv, key):
    return sorted(inv.sections[key]["SKU"])


def test_stock_not_visible_excludes_skus_owned_by_other_sections(make_sku, make_catalog):
    inv = _inv(make_catalog, [
        make_sku("plain", **HIDDEN),
        make_sku("tipo-c", **HIDDEN, **{"NIVEL 1": "Catalogo Completo"}),
        make_sku("disabled", **HIDDEN, **DISABLED),
        make_sku("gift", **HIDDEN, **{"NIVEL 1": "Mesa de Regalos"}),
        make_sku("no-stock", **HIDDEN, **NO_STOCK),
    ])
    assert skus(inv, "stock_no_visible") == ["plain"]
    assert inv.kpis["stock_no_visible"] == 1


def test_gift_certificates_are_not_physical(make_sku, make_catalog):
    # FIX F7: the export uses the plural "Certificados De Regalo"
    inv = _inv(make_catalog, [
        make_sku("cert", **DISABLED, **{"NIVEL 1": "Certificados De Regalo"}),
        make_sku("cert-singular", **{"NIVEL 1": "Certificado de Regalo"}),
        make_sku("mesa", **{"NIVEL 1": "Mesa de Regalos"}),
        make_sku("real", **DISABLED),
    ])
    assert skus(inv, "no_fisicos") == ["cert", "cert-singular", "mesa"]
    assert skus(inv, "deshabilitados_stock") == ["real"]


def test_tipo_c_long_tail_disabled_and_no_image(make_sku, make_catalog):
    inv = _inv(make_catalog, [
        make_sku("tipo-c", **{"NIVEL 1": " catalogo completo "}),
        make_sku("tipo-c-empty", **NO_STOCK, **{"NIVEL 1": "Catalogo Completo"}),
        make_sku("lt", **{"TEMPORADA ERP": "Long Tail Proveedor"}),
        make_sku("lt-modal", MODAL="M1", **{"TEMPORADA ERP": "Long Tail Proveedor"}),
        make_sku("lt-cemaco", **{"TEMPORADA ERP": "Long Tail Cemaco"}),
        make_sku("dis", **DISABLED),
        make_sku("dis-no-img", **DISABLED, **NO_IMAGE, **NO_STOCK),
    ])
    assert skus(inv, "tipo_c_stock") == ["tipo-c"]
    assert skus(inv, "long_tail_sin_modal") == ["lt"]
    assert skus(inv, "deshabilitados_stock") == ["dis"]
    assert skus(inv, "sin_imagen_deshabilitado") == ["dis-no-img"]
    assert inv.kpis["acciones_urgentes"] == 1 + 1 + 1 + 0  # tipo C + long tail + disabled + stock-no-visible


def test_low_score_kpi(make_sku, make_catalog):
    inv = _inv(make_catalog, [
        make_sku("weak", **{"DESCRIPCION ERP": "", "MARCA": ""}),  # 75
        make_sku("ok"),
    ])
    assert inv.kpis["stock_visible_score_bajo"] == 1


def test_yesterday_sections(make_sku, make_catalog):
    tipo_c = {"NIVEL 1": "Catalogo Completo"}
    inv = _inv(
        make_catalog,
        [
            make_sku("grad-same-url"),
            make_sku("grad-new-url", **{"URL IMAGEN": "https://img/new.jpg"}),
            make_sku("grad-hidden", **HIDDEN),
            make_sku("still-c", **tipo_c),
        ],
        [
            make_sku("grad-same-url", **tipo_c),
            make_sku("grad-new-url", **tipo_c),
            make_sku("grad-hidden", **tipo_c),
            make_sku("still-c", **tipo_c),
        ],
    )
    assert skus(inv, "tipo_c_graduados") == ["grad-hidden", "grad-new-url", "grad-same-url"]
    assert inv.kpis["tipo_c_graduados_visibles"] == 2
    assert skus(inv, "url_no_actualizada") == ["grad-hidden", "grad-same-url"]


def test_without_yesterday_sections_are_absent(make_sku, make_catalog):
    inv = _inv(make_catalog, [make_sku("a")])
    assert "tipo_c_graduados" not in inv.sections and not inv.has_yesterday


def test_display_uses_section_columns(make_sku, make_catalog):
    inv = _inv(make_catalog, [make_sku("p", **HIDDEN)])
    assert list(inv.display("stock_no_visible").columns) == [
        "SKU", "NOMBRE DE PRODUCTO", "NIVEL 1", "TEMPORADA ERP",
        "HABILITADO/DESHABILITADO", "TIENE STOCK", "MODAL",
    ]
    # Export keeps every catalog column
    assert "MARCA" in inv.sections["stock_no_visible"].columns
