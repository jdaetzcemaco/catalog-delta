from __future__ import annotations

import altair as alt
import streamlit as st

from app import data, ui

st.title("Productividad de equipos")
days = data.productivity_days()
if not days:
    st.info("Aún no hay archivos de productividad de Diseño o Edición.", icon="⏳")
    st.stop()

top = st.columns([1, 1, 2])
pday = top[0].selectbox("Archivo de productividad", days, format_func=ui.fmt_day)
p = data.productivity(pday)
teams = [t for t, _ in p.teams()]
top[2].caption(f"Equipos cargados: **{', '.join(teams)}**" +
               ("" if p.both else " · sube ambos archivos para ver las comparaciones entre equipos"))

run_days = data.run_days()
catalog_day = next((d for d in run_days if d <= pday), run_days[-1] if run_days else None)
info = data.sku_info(catalog_day) if catalog_day else None

u = p.unique_counts()
c = st.columns(4)
ui.kpi(c[0], "SKUs únicos trabajados", u["total"])
if p.both:
    ui.kpi(c[1], "Solo Diseño", u["solo_diseno"])
    ui.kpi(c[2], "Ambos equipos", u["ambos"])
    ui.kpi(c[3], "Solo Edición", u["solo_edicion"])

tab_a, tab_f, tab_d, tab_bc = st.tabs(["A · Por usuario", "F · Salieron del flujo",
                                       "D · Con inventario omnicanal", "B/C · Entre equipos"])

with tab_a:
    by_user = p.skus_by_user()
    st.caption("SKUs únicos por persona: quien trabajó el SKU y quien lo promovió.")
    if not by_user.empty:
        chart = alt.Chart(by_user).mark_bar(cornerRadiusEnd=4).encode(
            x=alt.X("SKUs Únicos:Q", title="SKUs únicos"),
            y=alt.Y("Usuario:N", sort="-x", title=None),
            tooltip=["Usuario", "SKUs Únicos"],
        ).properties(height=max(200, 26 * len(by_user)))
        st.altair_chart(chart, use_container_width=True)
    ui.show_table(by_user, key="prod_users", filters=False, download_name=f"skus_por_usuario_{pday}")

with tab_f:
    default = p.latest_flujo_date()
    on = st.date_input("Fecha de salida del flujo", value=default, format="DD/MM/YYYY",
                       help="Por defecto la fecha más reciente en los archivos.")
    left = p.left_workflow(on, info)
    k = p.left_workflow_kpis(left)
    f = st.columns(4)
    ui.kpi(f[0], "Salieron del flujo", k["total"])
    ui.kpi(f[1], "Con inventario omnicanal", k["con_inventario"])
    ui.kpi(f[2], "✅ Con inventario y visibles", k["con_inventario_visibles"])
    ui.kpi(f[3], "⚠️ Con inventario, no visibles", k["con_inventario_no_visibles"])
    if catalog_day:
        st.caption(f"Visibilidad según el catálogo del {ui.fmt_day(catalog_day)}.")
    ui.show_table(left.rename(columns={"<ID>": "SKU"}), key="prod_flujo", info=info,
                  download_name=f"salieron_del_flujo_{on}", empty=f"Ningún SKU salió del flujo el {ui.fmt_day(on)}." if on else "Elige una fecha.")

with tab_d:
    omni = p.with_omni_stock()
    st.caption("SKUs trabajados que tienen inventario omnicanal (un registro por SKU).")
    ui.show_table(omni.rename(columns={"<ID>": "SKU"}), key="prod_omni", info=info,
                  download_name=f"con_inventario_omnicanal_{pday}", empty="Sin SKUs con inventario omnicanal.")

with tab_bc:
    if not p.both:
        st.info("Sube ambos archivos de productividad para ver esta sección.")
    else:
        st.markdown(f"**B · Repetidos entre equipos ({ui.n(u['ambos'])})**")
        ui.show_table(p.repeated().rename(columns={"<ID>": "SKU"}), key="prod_rep", info=info,
                      download_name=f"skus_repetidos_{pday}", empty="Ningún SKU pasó por ambos equipos.")
        st.markdown("**C · No pasaron por ambos equipos** (por categoría)")
        a, b = st.columns(2)
        with a:
            st.caption(f"Solo Diseño · {ui.n(u['solo_diseno'])} SKUs")
            ui.show_table(p.only_in_by_category("Diseño"), key="prod_solo_d", filters=False)
        with b:
            st.caption(f"Solo Edición · {ui.n(u['solo_edicion'])} SKUs")
            ui.show_table(p.only_in_by_category("Edición"), key="prod_solo_e", filters=False)
