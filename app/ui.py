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
}
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
        if c in LABELS and (c.startswith(("has_", "is_"))):
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
        elif c.startswith("content_score"):
            cfg[c] = st.column_config.ProgressColumn(label, min_value=0, max_value=MAX_SCORE, format="%d")
        elif c == "delta_score":
            cfg[c] = st.column_config.NumberColumn(label, format="%+d")
        elif df[c].dtype == bool or df[c].map(lambda v: isinstance(v, bool)).any():
            cfg[c] = st.column_config.CheckboxColumn(label, width="small")
        elif label != c:
            cfg[c] = st.column_config.Column(label)
    return cfg


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
        c2.download_button("Descargar CSV", df.to_csv(index=False).encode("utf-8-sig"),
                           file_name=f"{download_name}.csv", mime="text/csv",
                           key=f"{key}_dl", width="stretch")


def no_data() -> None:
    st.info("Aún no hay resultados procesados. Sube un catálogo en **Cargar archivos** "
            "o espera a que llegue el export diario.", icon="⏳")
