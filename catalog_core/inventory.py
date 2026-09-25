"""
Inventario Omnicanal QA: SKUs whose stock is not reaching the online store.

Each section is a named mask over the raw catalog. The app shows DISPLAY_COLS for a
section; the Excel export writes the same rows with every catalog column, so screen
and download always agree (FIX F4).
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from .text import col, lower, nonempty, select, yesno

TIPO_C = "catalogo completo"
LONG_TAIL_PROVEEDOR = "long tail proveedor"
# FIX F7: the export says "Certificados de Regalo" (plural); v1 only matched the singular
NO_FISICOS_PATTERN = r"mesa de regalos|certificados? de regalo"
LOW_SCORE = 80

SECTIONS = {
    "stock_no_visible": "Tiene Stock pero NO Visible",
    "tipo_c_stock": "Tipo C con Inventario",
    "tipo_c_graduados": "Tipo C Graduados",
    "no_fisicos": "Productos No Físicos",
    "long_tail_sin_modal": "Long Tail Proveedor sin Modal",
    "deshabilitados_stock": "Deshabilitados con Inventario",
    "sin_imagen_deshabilitado": "Sin Imagen + Deshabilitado",
    "url_no_actualizada": "URL Imagen no actualizada",
}

DISPLAY_COLS = {
    "stock_no_visible": ["SKU", "NOMBRE DE PRODUCTO", "NIVEL 1", "TEMPORADA ERP",
                         "HABILITADO/DESHABILITADO", "TIENE STOCK", "MODAL"],
    "tipo_c_stock": ["SKU", "NOMBRE DE PRODUCTO", "NIVEL 1", "TIENE STOCK", "VISIBLE",
                     "HABILITADO/DESHABILITADO"],
    "tipo_c_graduados": ["SKU", "NOMBRE DE PRODUCTO", "NIVEL 1", "VISIBLE", "TIENE STOCK"],
    "no_fisicos": ["SKU", "NOMBRE DE PRODUCTO", "NIVEL 1", "TIENE STOCK", "VISIBLE"],
    "long_tail_sin_modal": ["SKU", "NOMBRE DE PRODUCTO", "NIVEL 1", "TEMPORADA ERP",
                            "MODAL", "VISIBLE", "HABILITADO/DESHABILITADO"],
    "deshabilitados_stock": ["SKU", "NOMBRE DE PRODUCTO", "NIVEL 1", "TEMPORADA ERP",
                             "HABILITADO/DESHABILITADO", "TIENE STOCK", "VISIBLE"],
    "sin_imagen_deshabilitado": ["SKU", "NOMBRE DE PRODUCTO", "NIVEL 1", "TEMPORADA ERP",
                                 "TIENE IMAGEN", "URL IMAGEN", "HABILITADO/DESHABILITADO"],
    "url_no_actualizada": ["SKU", "NOMBRE DE PRODUCTO", "NIVEL 1", "NIVEL 1_AYER",
                           "URL IMAGEN", "URL IMAGEN_AYER"],
}


@dataclass
class InventoryReport:
    kpis: dict[str, int]
    # Full catalog rows per section key; sections needing yesterday are absent without it
    sections: dict[str, pd.DataFrame] = field(default_factory=dict)
    has_yesterday: bool = False

    def display(self, key: str) -> pd.DataFrame:
        df = self.sections[key]
        return df[[c for c in DISPLAY_COLS[key] if c in df.columns]]


def _graduados(today_raw: pd.DataFrame, yesterday_raw: pd.DataFrame, tipo_c_today: pd.Series) -> pd.DataFrame:
    """SKUs that were Tipo C yesterday and have a real category today."""
    was_tipo_c = set(yesterday_raw.loc[lower(col(yesterday_raw, "NIVEL 1")) == TIPO_C, "SKU"])
    still_tipo_c = set(today_raw.loc[tipo_c_today.fillna(False).astype(bool), "SKU"])
    return select(today_raw, today_raw["SKU"].isin(was_tipo_c - still_tipo_c))


def _url_no_actualizada(today_raw: pd.DataFrame, yesterday_raw: pd.DataFrame) -> pd.DataFrame:
    """Left Tipo C since yesterday but the image URL is unchanged (image not re-sent)."""
    y_cols = [c for c in ["SKU", "NIVEL 1", "URL IMAGEN"] if c in yesterday_raw.columns]
    merged = today_raw.merge(
        yesterday_raw[y_cols].rename(columns={"NIVEL 1": "NIVEL 1_AYER", "URL IMAGEN": "URL IMAGEN_AYER"}),
        on="SKU", how="inner",
    )
    n1_hoy = lower(col(merged, "NIVEL 1"))
    n1_ayer = lower(col(merged, "NIVEL 1_AYER"))
    # NaN never equals NaN: SKUs without a URL on both days are §6's problem, not this one
    same_url = col(merged, "URL IMAGEN") == col(merged, "URL IMAGEN_AYER")
    return select(merged, (n1_ayer == TIPO_C) & (n1_hoy != TIPO_C) & same_url)


def build_inventory(
    today_raw: pd.DataFrame,
    today_flags: pd.DataFrame,
    yesterday_raw: pd.DataFrame | None = None,
) -> InventoryReport:
    """
    Run all inventory QA sections.

    today_flags must come from build_flags(today_raw) so the indexes align.
    """
    # FIX F3: same stock definition as the content score (STOCK > 0 or TIENE STOCK)
    has_stock = today_flags["has_stock"] == 1
    is_visible = yesno(col(today_raw, "VISIBLE")) == 1
    nivel1 = lower(col(today_raw, "NIVEL 1"))
    temporada = lower(col(today_raw, "TEMPORADA ERP"))
    modal_empty = nonempty(col(today_raw, "MODAL")) == 0
    tiene_imagen = col(today_raw, "TIENE IMAGEN")
    no_image = (nonempty(tiene_imagen) == 0) | (lower(tiene_imagen) == "no")
    disabled = lower(col(today_raw, "HABILITADO/DESHABILITADO")).str.contains("deshab", na=False).astype(bool)

    tipo_c = (nivel1 == TIPO_C).fillna(False).astype(bool)
    no_fisico = nivel1.str.contains(NO_FISICOS_PATTERN, na=False).astype(bool)
    physical = ~no_fisico

    masks = {
        # Tipo C, disabled and non-physical SKUs have their own sections
        "stock_no_visible": has_stock & ~is_visible & physical & ~tipo_c & ~disabled,
        "tipo_c_stock": tipo_c & has_stock & physical,
        "no_fisicos": no_fisico,
        "long_tail_sin_modal": (temporada == LONG_TAIL_PROVEEDOR).fillna(False) & modal_empty & physical,
        "deshabilitados_stock": disabled & has_stock & physical,
        "sin_imagen_deshabilitado": no_image & disabled & physical,
    }
    sections = {key: select(today_raw, mask) for key, mask in masks.items()}

    if yesterday_raw is not None:
        sections["tipo_c_graduados"] = _graduados(today_raw, yesterday_raw, tipo_c)
        sections["url_no_actualizada"] = _url_no_actualizada(today_raw, yesterday_raw)

    low_score = today_flags["content_score"] < LOW_SCORE
    kpis = {
        "stock_no_visible": int(masks["stock_no_visible"].sum()),
        "stock_visible_score_bajo": int((has_stock & is_visible & physical & low_score).sum()),
        "deshabilitados_stock": int(masks["deshabilitados_stock"].sum()),
        "tipo_c_stock": int(masks["tipo_c_stock"].sum()),
        "long_tail_sin_modal": int(masks["long_tail_sin_modal"].sum()),
        "no_fisicos": int(no_fisico.sum()),
    }
    kpis["acciones_urgentes"] = (
        kpis["stock_no_visible"] + kpis["deshabilitados_stock"] +
        kpis["tipo_c_stock"] + kpis["long_tail_sin_modal"]
    )
    if yesterday_raw is not None:
        grad = sections["tipo_c_graduados"]
        kpis["tipo_c_graduados"] = len(grad)
        kpis["tipo_c_graduados_visibles"] = int((yesno(col(grad, "VISIBLE")) == 1).sum())

    return InventoryReport(kpis=kpis, sections=sections, has_yesterday=yesterday_raw is not None)
