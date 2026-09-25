from __future__ import annotations

import pandas as pd
import streamlit as st

from app import data, ui
from catalog_core import MAX_SCORE, to_excel_bytes
from pipeline.export import excel_sheets_from_store

if not data.run_days():
    st.title("Resumen del catálogo")
    ui.no_data()
    st.stop()

day = st.session_state.day
m = data.manifest(day)
prev = data.manifest(m["previous_day"]) if m.get("previous_day") else None
s, ps = m["summary"], (prev or {}).get("summary")
hist = data.history()
if not hist.empty:
    hist = hist[hist["Fecha"] <= pd.Timestamp(day)].tail(30)


def delta(key):
    return None if ps is None else round(s[key] - ps[key], 2)


def trend(key):
    return hist[key].tolist() if key in hist else None


st.title("Resumen del catálogo")
st.caption(f"Catálogo del **{ui.fmt_day(day)}** · archivo `{m['source']['source']}`")

st.subheader("Salud del catálogo")
r1 = st.columns(4)
ui.kpi(r1[0], "Total SKUs", s["Total SKUs"], delta("Total SKUs"), chart=trend("Total SKUs"))
ui.kpi(r1[1], "Visibles", s["Visible"], delta("Visible"), chart=trend("Visible"),
       help=f"{s['Visible %']}% del catálogo")
ui.kpi(r1[2], "Score promedio", s["Avg Content Score"], delta("Avg Content Score"),
       chart=trend("Avg Content Score"), help=f"Máximo posible: {MAX_SCORE}")
ui.kpi(r1[3], f"Score perfecto ({MAX_SCORE})", s["Perfect Score"], delta("Perfect Score"),
       chart=trend("Perfect Score"), help="SKUs con todo el contenido completo")
r2 = st.columns(4)
ui.kpi(r2[0], "Visibles %", s["Visible %"], delta("Visible %"), suffix="%", chart=trend("Visible %"))
ui.kpi(r2[1], "Con imagen", s["With Image %"], delta("With Image %"), suffix="%", chart=trend("With Image %"))
ui.kpi(r2[2], "Con precio", s["With Price %"], delta("With Price %"), suffix="%", chart=trend("With Price %"))
ui.kpi(r2[3], "Con stock", s["With Stock %"], delta("With Stock %"), suffix="%", chart=trend("With Stock %"))

left, right = st.columns(2, gap="large")
with left:
    st.subheader("Cambios vs día anterior")
    if m["baseline"]:
        st.info("Primer día procesado: los cambios aparecen desde el siguiente catálogo.")
    else:
        rc, sc = m["row_counts"], m["sku_changes"]
        c = st.columns(2)
        ui.kpi(c[0], "SKUs nuevos", sc["new"])
        ui.kpi(c[1], "SKUs eliminados", sc["removed"], inverse=True)
        ui.kpi(c[0], "Nuevos visibles", rc["changes:Newly Visible"])
        ui.kpi(c[1], "Ya no visibles", rc["changes:No Longer Visible"])
        ui.kpi(c[0], "Cambio de stock", rc["changes:Stock Flips"])
        ui.kpi(c[1], "Score cambió ±10", rc["changes:Score Changes"])
        st.page_link("app/pages/cambios.py", label="Ver detalle de cambios", icon="🔄")

with right:
    st.subheader("Acciones urgentes")
    k = m["inventory_kpis"]
    with st.container(border=True):
        st.markdown(f"### ⚠️ {ui.n(k['acciones_urgentes'])} SKUs")
        st.caption("Inventario que no está llegando a la tienda en línea")
        for label, key in [("🚫 Stock sin visibilidad", "stock_no_visible"),
                           ("⛔ Deshabilitados con stock", "deshabilitados_stock"),
                           ("🔴 Tipo C con inventario", "tipo_c_stock"),
                           ("🚚 Long Tail sin modal", "long_tail_sin_modal")]:
            a, b = st.columns([3, 1])
            a.write(label)
            b.markdown(f"**{ui.n(k[key])}**")
        st.page_link("app/pages/inventario.py", label="Revisar inventario", icon="📦")

st.divider()
st.subheader("Reporte en Excel")
prod_days = data.productivity_days()
st.caption("Todas las hojas del reporte (cambios, inventario y productividad del día más reciente).")


@st.cache_data(ttl=data.TTL, show_spinner="Generando Excel…", max_entries=3)
def excel(day: str, prod_day: str | None) -> bytes:
    prod = data.productivity(prod_day) if prod_day else None
    return to_excel_bytes(excel_sheets_from_store(data.store(), day, prod))


if st.button("Preparar Excel", icon="📥"):
    st.session_state.excel_ready = day
if st.session_state.get("excel_ready") == day:
    st.download_button(
        "Descargar Catalog_Delta.xlsx", excel(day, prod_days[0] if prod_days else None),
        file_name=f"Catalog_Delta_{day.replace('-', '')}.xlsx", type="primary",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
