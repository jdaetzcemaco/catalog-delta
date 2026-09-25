from __future__ import annotations

import streamlit as st

from app import data, ui
from catalog_core.inventory import DISPLAY_COLS

st.title("Inventario Omnicanal")
if not data.run_days():
    ui.no_data()
    st.stop()

day = st.session_state.day
m = data.manifest(day)
k = m["inventory_kpis"]
info = data.sku_info(day)
st.caption("SKUs con inventario que no llega a la tienda en línea. Los productos no físicos "
           f"(Mesa de Regalos, Certificados de Regalo: {ui.n(k['no_fisicos'])}) se excluyen de todas las acciones.")

c = st.columns(5)
ui.kpi(c[0], "🚫 Stock sin visibilidad", k["stock_no_visible"])
ui.kpi(c[1], "⛔ Deshabilitados con stock", k["deshabilitados_stock"])
ui.kpi(c[2], "🔴 Tipo C con inventario", k["tipo_c_stock"])
ui.kpi(c[3], "🚚 Long Tail sin modal", k["long_tail_sin_modal"])
ui.kpi(c[4], "📉 Visibles, score < 80", k["stock_visible_score_bajo"],
       help="Con stock y visibles, pero con contenido incompleto")
st.markdown(f"**Acciones urgentes totales: {ui.n(k['acciones_urgentes'])}** "
            "(stock sin visibilidad + deshabilitados + Tipo C + Long Tail)")


def section(key: str, why: str, empty: str) -> None:
    st.caption(why)
    df = data.table(day, "inventory", key)
    if df is not None:
        df = df[[c for c in DISPLAY_COLS[key] if c in df.columns]]
    ui.show_table(df, key=f"inv_{key}", info=info, download_name=f"{key}_{day}", empty=empty)


tabs = st.tabs([
    f"🚫 Stock sin visibilidad ({ui.n(k['stock_no_visible'])})",
    f"🔴 Tipo C ({ui.n(k['tipo_c_stock'])})",
    f"⛔ Deshabilitados ({ui.n(k['deshabilitados_stock'])})",
    f"🖼️ Sin imagen + deshabilitado ({ui.n(m['row_counts'].get('inventory:sin_imagen_deshabilitado', 0))})",
    f"🚚 Long Tail sin modal ({ui.n(k['long_tail_sin_modal'])})",
    f"🔗 URL no actualizada ({ui.n(m['row_counts'].get('inventory:url_no_actualizada', 0))})",
    f"🎁 No físicos ({ui.n(k['no_fisicos'])})",
])
with tabs[0]:
    section("stock_no_visible",
            "Tienen stock pero no son visibles. Excluye Tipo C, deshabilitados y no físicos (tienen su propia pestaña).",
            "No hay SKUs con stock sin visibilidad.")
with tabs[1]:
    st.caption("Productos sin foto/contenido completo (NIVEL 1 = Catalogo Completo): disponibles en tienda pero no en línea.")
    if "tipo_c_graduados" in k:
        g = st.columns(3)
        ui.kpi(g[0], "Pendientes con stock", k["tipo_c_stock"])
        ui.kpi(g[1], "Graduados hoy", k["tipo_c_graduados"], help="Eran Tipo C el día anterior; hoy tienen categoría")
        ui.kpi(g[2], "Graduados y ya visibles", k["tipo_c_graduados_visibles"])
    st.markdown("**Pendientes**")
    section("tipo_c_stock", "", "No hay SKUs Tipo C con inventario pendientes.")
    if "tipo_c_graduados" in k:
        st.markdown("**Graduados hoy**")
        section("tipo_c_graduados", "Eran Tipo C el día anterior y hoy ya tienen categoría.", "Ningún graduado hoy.")
with tabs[2]:
    section("deshabilitados_stock", "Deshabilitados pero con inventario: ventas bloqueadas.",
            "No hay SKUs deshabilitados con inventario.")
with tabs[3]:
    section("sin_imagen_deshabilitado", "Deshabilitados porque la integración nunca recibió imagen.",
            "No hay SKUs deshabilitados por falta de imagen.")
with tabs[4]:
    section("long_tail_sin_modal",
            "Long Tail Proveedor sin modal asignado. (Long Tail Cemaco se almacena en bodega Cemaco y no requiere modal.)",
            "Todos los SKUs Long Tail Proveedor tienen modal.")
with tabs[5]:
    if m["baseline"]:
        st.info("Necesita un día anterior para comparar.")
    else:
        section("url_no_actualizada",
                "Salieron de Tipo C desde el día anterior pero mantienen la misma URL de imagen (posible imagen no reenviada).",
                "No hay SKUs con URL de imagen desactualizada.")
with tabs[6]:
    section("no_fisicos", "Mesa de Regalos y Certificados de Regalo: se excluyen del análisis de inventario.",
            "No hay productos no físicos.")
