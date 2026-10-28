"""Shared presentation helpers: KPI cards, enriched tables with filters, labels."""

from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st

from catalog_core import MAX_SCORE
from catalog_core.text import yesno

MONTHS = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"]

# Readable Spanish headers for the technical column names
LABELS = {
    "content_score": "Score", "content_score_today": "Score hoy", "content_score_yesterday": "Score ayer",
    "delta_score": "Δ Score", "is_visible": "Visible", "is_visible_today": "Visible hoy",
    "is_visible_yesterday": "Visible ayer", "has_image": "Imagen", "has_image_today": "Imagen hoy",
    "has_image_yesterday": "Imagen ayer", "has_price": "Precio", "has_price_today": "Precio hoy",
    "has_price_yesterday": "Precio ayer", "has_stock": "Stock", "has_stock_today": "Stock hoy",
    "has_stock_yesterday": "Stock ayer", "has_name": "Nombre", "has_desc": "Descripción",
    "has_brand": "Marca", "is_enabled": "Habilitado", "taxonomy_depth": "Niveles",
    "taxonomy_points": "Pts. taxonomía",
    # campaign readiness
    "listo": "Listo", "en_catalogo": "En catálogo", "visible": "Visible", "habilitado": "Habilitado",
    "contenido": "Contenido", "categoria_real": "Categoría", "stock": "Stock", "stock_confiable": "Stock confiable",
    "score": "Score", "falta_imagen": "Falta imagen", "falta_precio": "Falta precio",
    "falta_descripcion": "Falta descripción", "ultima_vez_visible": "Última vez visible",
    "primera_vez_visto": "Primera vez visto", "dias_visible_seguidos": "Días visible seguidos",
    "motivo_pendiente": "Motivo", "falta_en_step": "Falta en STEP", "tambien_sin_stock": "También sin stock",
    "salida_flujo": "Salida del flujo", "fin_dat_log": "Fin Dat_Log", "equipo": "Equipo", "perdio": "Perdió",
    # performance (GA4 / Search Console)
    "vistas_7d": "Vistas 7d", "vistas_30d": "Vistas 30d", "vistas_90d": "Vistas 90d", "vistas_7d_prev": "Vistas 7d ant.",
    "carrito_7d": "Carrito 7d", "carrito_30d": "Carrito 30d", "carrito_90d": "Carrito 90d",
    "compras_7d": "Compras 7d", "compras_30d": "Compras 30d", "compras_90d": "Compras 90d",
    "ingresos_7d": "Ingresos 7d", "ingresos_30d": "Ingresos 30d", "ingresos_90d": "Ingresos 90d",
    "sessions_7d": "Sesiones 7d", "sessions_30d": "Sesiones 30d", "sessions_90d": "Sesiones 90d",
    "organic_sessions_7d": "Orgánicas 7d", "organic_sessions_30d": "Orgánicas 30d", "organic_sessions_90d": "Orgánicas 90d",
    "organic_sessions_7d_prev": "Orgánicas 7d ant.", "clicks_7d": "Clics Google 7d", "clicks_30d": "Clics Google 30d",
    "clicks_90d": "Clics Google 90d", "impressions_7d": "Impresiones 7d", "impressions_30d": "Impresiones 30d",
    "impressions_90d": "Impresiones 90d", "clicks_7d_prev": "Clics 7d ant.", "posicion_30d": "Posición Google 30d",
    "tasa_carrito_30d": "Tasa carrito 30d", "tasa_compra_30d": "Tasa compra 30d", "ultimo_dia_vistas": "Última vista",
    "product_id": "Product ID", "STOCK": "Stock", "PRECIO": "Precio", "TIENE STOCK": "Tiene stock",
    "STOCK REAL": "Stock real", "BODEGAS CON STOCK": "Bodegas con stock",
    # CRO
    "motivo": "Motivo", "impacto": "Impacto", "vistas_ventana": "Vistas 30d", "tasa_carrito": "Agrega al carrito",
    "cat_cart_rate": "Categoría: carrito", "tasa_compra": "Compra", "cat_buy_rate": "Categoría: compra",
    "compra_por_carrito": "Compra / carrito", "cat_buy_per_cart": "Categoría: compra / carrito",
    "paid_views": "Vistas pauta 30d", "tasa_compra_pagado": "Compra desde pauta", "cat_paid_buy_rate": "Categoría: compra pauta",
    "content_score": "Score",
    # SEO impact
    "estado": "Estado", "event_day": "Salida del flujo", "cohorte": "Semana", "usuarios": "Usuarios",
    "impr_antes": "Impr. antes", "impr_despues": "Impr. después", "delta_impr_pct": "Δ impresiones",
    "clics_antes": "Clics antes", "clics_despues": "Clics después", "delta_clics_pct": "Δ clics",
    "consultas_antes": "Consultas antes", "consultas_despues": "Consultas después",
    "pos_antes": "Posición antes", "pos_despues": "Posición después", "uplift_impr_pct": "Δ vs control",
    "uplift_clics_pct": "Δ clics vs control", "ctrl_delta_impr_pct": "Control Δ impr.",
    "ctrl_delta_clics_pct": "Control Δ clics", "primera_impresion": "Primera impresión",
    "dias_a_primera_impresion": "Días a 1ª impresión", "consultas_nuevas": "Consultas nuevas",
    "consultas_perdidas": "Consultas perdidas", "consultas_subieron": "Consultas que subieron",
    "top_consulta_despues": "Consulta principal", "impr_ml_despues": "Impr. Merchant",
    "impr_ps_despues": "Impr. snippet producto", "cambios_ficha": "Cambios en la ficha",
    "score_antes": "Score antes", "score_despues": "Score después", "query": "Consulta", "bucket": "Impr. previas",
    "n": "SKUs", "ctrl_n": "SKUs control", "ml": "Merchant", "ps": "Snippet", "anonima": "Anónima",
}
BOOL_COLS = {"listo", "en_catalogo", "visible", "habilitado", "contenido", "categoria_real", "stock",
             "stock_confiable", "falta_imagen", "falta_precio", "falta_descripcion", "tambien_sin_stock",
             "ml", "ps", "anonima", "completo"}
INFO_COLS = {"NOMBRE DE PRODUCTO": "Producto", "NIVEL 1": "Nivel 1", "URL IMAGEN": "Imagen", "URL": "Página"}


def fmt_day(day: str | date) -> str:
    d = pd.Timestamp(day)
    return f"{d.day:02d} {MONTHS[d.month - 1]} {d.year}"


def n(value) -> str:
    return f"{int(value):,}"


def kpi(container, label: str, value, delta=None, *, help: str | None = None,
        inverse: bool = False, chart: list | None = None, suffix: str = "") -> None:
    """Bordered KPI card; delta is the change vs the previous processed day."""
    if isinstance(value, (int, float)) and not suffix and float(value).is_integer():
        shown = n(value)
    else:
        shown = f"{value:,.2f}{suffix}" if isinstance(value, float) else f"{value}{suffix}"
    if delta is not None:
        if isinstance(delta, float) and not float(delta).is_integer():
            delta = f"{delta:+,.2f}{suffix}"
        else:
            delta = f"{int(delta):+,}{suffix}" if delta else None
    container.metric(
        label, shown, delta, help=help, border=True,
        delta_color="inverse" if inverse else "normal",
        chart_data=chart if chart and len(chart) > 1 else None, chart_type="line",
    )


def enrich(df: pd.DataFrame, info: pd.DataFrame | None) -> pd.DataFrame:
    """Add product name, category, image and page link to a SKU table."""
    if info is None or "SKU" not in df.columns:
        return df
    add = [c for c in INFO_COLS if c in info.columns and c not in df.columns]
    if not add:
        return df
    return df.merge(info[["SKU", *add]].drop_duplicates("SKU"), on="SKU", how="left")


def _as_checks(df: pd.DataFrame) -> pd.DataFrame:
    """0/1 flags and Si/No text as booleans so they render as checkmarks."""
    out = df.copy()
    for c in out.columns:
        s = out[c]
        if c in BOOL_COLS or (c in LABELS and c.startswith(("has_", "is_"))):
            out[c] = s.map(lambda v: None if pd.isna(v) else bool(v))
        elif s.dtype == object and c.upper() == c:
            vals = set(s.dropna().astype(str).str.strip().str.lower().unique())
            if vals and vals <= {"si", "sí", "no"}:
                out[c] = pd.Series(yesno(s).astype(bool), index=s.index).where(s.notna(), None)
    return out


def _column_config(df: pd.DataFrame) -> dict:
    cfg: dict = {}
    for c in df.columns:
        label = LABELS.get(c) or INFO_COLS.get(c) or c
        if c == "SKU":
            cfg[c] = st.column_config.TextColumn("SKU", pinned=True)
        elif c == "URL IMAGEN":
            cfg[c] = st.column_config.ImageColumn("Imagen", width="small")
        elif c == "URL":
            cfg[c] = st.column_config.LinkColumn("Página", display_text="Ver ↗", width="small")
        elif c in ("ultima_vez_visible", "primera_vez_visto", "salida_flujo", "event_day", "primera_impresion", "cohorte"):
            cfg[c] = st.column_config.DateColumn(label, format="DD/MM/YYYY")
        elif c.endswith("_pct"):
            cfg[c] = st.column_config.NumberColumn(label, format="%+.0f%%")
        elif c in ("pos_antes", "pos_despues"):
            cfg[c] = st.column_config.NumberColumn(label, format="%.1f")
        elif c in ("score_antes", "score_despues"):
            cfg[c] = st.column_config.ProgressColumn(label, min_value=0, max_value=MAX_SCORE, format="%d")
        elif c == "ultimo_dia_vistas":
            cfg[c] = st.column_config.DateColumn(label, format="DD/MM/YYYY")
        elif c.startswith("ingresos_"):
            cfg[c] = st.column_config.NumberColumn(label, format="Q%,.0f")
        elif c.startswith(("tasa_", "cat_")) or c == "compra_por_carrito":
            cfg[c] = st.column_config.NumberColumn(label, format="%.1%")
        elif c == "posicion_30d":
            cfg[c] = st.column_config.NumberColumn(label, format="%.1f")
        elif c == "fin_dat_log":
            cfg[c] = st.column_config.DatetimeColumn(label, format="DD/MM/YYYY HH:mm")
        elif c in ("score", "content_score"):
            cfg[c] = st.column_config.ProgressColumn(label, min_value=0, max_value=MAX_SCORE, format="%d")
        elif c.startswith("content_score"):
            cfg[c] = st.column_config.ProgressColumn(label, min_value=0, max_value=MAX_SCORE, format="%d")
        elif c == "delta_score":
            cfg[c] = st.column_config.NumberColumn(label, format="%+d")
        elif df[c].dtype == bool or df[c].map(lambda v: isinstance(v, bool)).any():
            cfg[c] = st.column_config.CheckboxColumn(label, width="small")
        elif label != c:
            cfg[c] = st.column_config.Column(label)
    return cfg


@st.cache_data(show_spinner=False, max_entries=30)
def _csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def show_table(df: pd.DataFrame | None, key: str, *, info: pd.DataFrame | None = None,
               download_name: str | None = None, empty: str = "Sin registros.",
               filters: bool = True, height: int | str = "auto") -> None:
    """Table with product details, search, Nivel 1 filter and a CSV download."""
    if df is None or df.empty:
        st.success(empty)
        return
    df = enrich(df, info)
    # Image and product first, right after SKU
    front = [c for c in ["SKU", "URL IMAGEN", "NOMBRE DE PRODUCTO", "NIVEL 1"] if c in df.columns]
    df = df[front + [c for c in df.columns if c not in front]]

    if filters and len(df) > 10:
        f1, f2 = st.columns([2, 3])
        q = f1.text_input("Buscar SKU o producto", key=f"{key}_q", placeholder="Ej. 830548 o cortina")
        levels = sorted(df["NIVEL 1"].dropna().unique()) if "NIVEL 1" in df.columns else []
        chosen = f2.multiselect("Nivel 1", levels, key=f"{key}_n1", placeholder="Todas las categorías") if levels else []
        if q:
            hay = df["SKU"].astype(str)
            if "NOMBRE DE PRODUCTO" in df.columns:
                hay = hay + " " + df["NOMBRE DE PRODUCTO"].fillna("").astype(str)
            df = df[hay.str.contains(q.strip(), case=False, regex=False)]
        if chosen:
            df = df[df["NIVEL 1"].isin(chosen)]

    shown = _as_checks(df)
    st.dataframe(shown, hide_index=True, width="stretch", height=height,
                 column_config=_column_config(shown), key=f"{key}_df",
                 row_height=56 if "URL IMAGEN" in shown.columns else None)
    c1, c2 = st.columns([3, 1])
    c1.caption(f"{n(len(df))} filas")
    if download_name:
        # Cached: the same table keeps the same file id across reruns, and downloading
        # does not rerun the page (a rerun mid-download is what logs "Missing file")
        c2.download_button("Descargar CSV", _csv(df), file_name=f"{download_name}.csv", mime="text/csv",
                           key=f"{key}_dl", width="stretch", on_click="ignore")


def quality_banner(manifest: dict | None) -> None:
    """Warn when the export carries values that make stock numbers unreliable."""
    q = (manifest or {}).get("quality") or {}
    n_placeholder = q.get("stock_placeholder", 0)
    if not n_placeholder:
        return
    if q.get("stock_corrected"):
        st.info(f"El export trae STOCK = 1,000,000 en {n(n_placeholder)} SKUs (bodega virtual MDR). El stock de este "
                f"tablero se toma de la hoja *Inventarios* sin esa bodega: **{q.get('stock_real_pct', 0)}%** de los SKUs "
                "tienen inventario real.", icon="ℹ️")
        return
    st.warning(
        f"**Stock no confiable en este export:** {n(n_placeholder)} SKUs ({q.get('stock_placeholder_pct', 0)}%) "
        "traen **STOCK = 1,000,000** o más, un valor que no es inventario real (aparece en el export de STEP "
        "desde el 22 sep 2026 y está en revisión con el equipo de STEP). Mientras tanto, *Con stock*, "
        "*Stock flips*, *Stock sin visibilidad*, *Deshabilitados con stock*, *Tipo C con inventario* y "
        "*Prioridades* están inflados.",
        icon="⚠️",
    )


def no_data() -> None:
    st.info("Aún no hay resultados procesados. Sube un catálogo en **Cargar archivos** "
            "o espera a que llegue el export diario.", icon="⏳")
