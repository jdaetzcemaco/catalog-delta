from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from app import data, ui
from catalog_core.cro import GROUP_ACTIONS, GROUPS
from pipeline.ga4 import BUCKET_LABELS

st.title("CRO · SKUs en la mira")
cro, summary = data.cro()
if cro is None:
    st.info("Aún no hay clasificación CRO. El job la calcula cuando tiene datos de GA4 (una vez al día).", icon="⏳")
    st.stop()

st.caption(f"Últimos 30 días de GA4 hasta el **{ui.fmt_day(summary['as_of'])}**, catálogo del {ui.fmt_day(summary['catalog_day'])}. "
           "Cada SKU se compara contra su **Nivel 1**. Las tasas son **por persona** (quienes lo vieron y lo agregaron o "
           "compraron) y se ajustan por volumen: un SKU con pocas vistas no puede aparecer por casualidad. "
           "Solo SKUs físicos; *fuga*, *carrito*, *pauta* y *joyas* exigen que estén visibles y con stock.")

counts = cro["grupo"].value_counts().to_dict()
cols = st.columns(len(GROUPS))
for col, (key, label) in zip(cols, GROUPS.items()):
    ui.kpi(col, label, counts.get(key, 0))

info = data.sku_info(st.session_state.day)
tabs = st.tabs([f"{label} ({counts.get(k, 0):,})" for k, label in GROUPS.items()] + ["📐 Contenido vs conversión", "📡 Canales"])

VIEW_COLS = {
    "fuga_conversion": ["SKU", "motivo", "impacto", "vistas_ventana", "tasa_carrito", "cat_cart_rate", "content_score"],
    "carrito_sin_compra": ["SKU", "motivo", "impacto", "compra_por_carrito", "cat_buy_per_cart", "carrito_30d", "compras_30d"],
    "pagado_sin_retorno": ["SKU", "motivo", "impacto", "paid_views", "tasa_compra_pagado", "cat_paid_buy_rate", "ingresos_30d"],
    "joya_oculta": ["SKU", "motivo", "impacto", "vistas_ventana", "tasa_compra", "cat_buy_rate", "ingresos_30d"],
    "organico_en_riesgo": ["SKU", "motivo", "impacto", "organic_sessions_7d_prev", "organic_sessions_7d"],
    "trafico_sin_stock": ["SKU", "motivo", "impacto", "vistas_7d"],
}

for tab, (key, label) in zip(tabs, GROUPS.items()):
    with tab:
        st.markdown(f"**Qué hacer:** {GROUP_ACTIONS[key]}")
        rows = cro[cro["grupo"] == key]
        if rows.empty:
            st.success("Ningún SKU en este grupo.", icon="✅")
            continue
        levels = sorted(rows["NIVEL 1"].dropna().unique())
        chosen = st.multiselect("Nivel 1", levels, key=f"cro_n1_{key}", placeholder="Todas las categorías")
        if chosen:
            rows = rows[rows["NIVEL 1"].isin(chosen)]
        rows = rows.sort_values("impacto", ascending=False)
        ui.show_table(rows[[c for c in VIEW_COLS[key] if c in rows.columns]].head(300), key=f"cro_{key}", info=info,
                      download_name=f"cro_{key}_{summary['as_of']}", filters=False)
        if len(rows) > 300:
            st.caption(f"Se muestran los 300 de mayor impacto de {len(rows):,}.")
        with st.form(f"pin_{key}"):
            pick = st.multiselect("Fijar en «En la mira» (se sigue a diario en Campañas)",
                                  rows["SKU"].head(300).tolist(), key=f"cro_pick_{key}")
            if st.form_submit_button("📌 Fijar seleccionados", disabled=False) and pick:
                total = data.add_to_watchlist(pick)
                st.success(f"{len(pick)} SKUs agregados a «En la mira» ({total} en total). El job los evalúa en su "
                           "próxima corrida.", icon="📌")

with tabs[len(GROUPS)]:
    st.caption("¿Convierten más los SKUs con contenido completo? Tasas por persona ponderadas por vistas, SKUs con ≥ 50 "
               "vistas en 30 días, agrupados por score de contenido (máximo 95).")
    cvc = pd.DataFrame(summary.get("content_vs_conversion") or [])
    if cvc.empty or cvc["skus"].sum() == 0:
        st.info("Sin datos suficientes todavía.")
    else:
        cvc = cvc[cvc["skus"] > 0]
        long = cvc.melt(id_vars=["banda", "skus"], value_vars=["tasa_carrito", "tasa_compra"], var_name="tasa", value_name="valor")
        long["tasa"] = long["tasa"].map({"tasa_carrito": "Agrega al carrito", "tasa_compra": "Compra"})
        chart = alt.Chart(long).mark_bar().encode(
            x=alt.X("banda:N", title="Score de contenido", sort=["< 60", "60–79", "80–89", "90–95"]),
            y=alt.Y("valor:Q", title=None, axis=alt.Axis(format="%")),
            xOffset="tasa:N", color=alt.Color("tasa:N", title=None),
            tooltip=["banda", "tasa", alt.Tooltip("valor:Q", format=".2%"), "skus"]).properties(height=280)
        st.altair_chart(chart, use_container_width=True)
        st.dataframe(cvc.rename(columns={"banda": "Score", "skus": "SKUs", "tasa_carrito": "Agrega al carrito",
                                         "tasa_compra": "Compra"}), hide_index=True, width="stretch",
                     column_config={"Agrega al carrito": st.column_config.NumberColumn(format="%.2f%%"),
                                    "Compra": st.column_config.NumberColumn(format="%.2f%%")})

with tabs[len(GROUPS) + 1]:
    st.caption("Embudo por canal en los últimos 30 días (unidades de GA4: vistas, agregados al carrito y compras de "
               "productos). Muestra de dónde viene la gente que compra y dónde la pauta trae vistas que no convierten.")
    ct = pd.DataFrame(summary.get("channel_totals") or [])
    if ct.empty:
        st.info("El histórico por canal se completa en las próximas corridas del job.")
    else:
        ct["Canal"] = ct["bucket"].map(BUCKET_LABELS).fillna(ct["bucket"])
        st.dataframe(ct[["Canal", "vistas", "carrito", "compras", "ingresos", "tasa_carrito", "tasa_compra"]],
                     hide_index=True, width="stretch",
                     column_config={"vistas": st.column_config.NumberColumn("Vistas", format="%,d"),
                                    "carrito": st.column_config.NumberColumn("Carrito", format="%,d"),
                                    "compras": st.column_config.NumberColumn("Compras", format="%,d"),
                                    "ingresos": st.column_config.NumberColumn("Ingresos", format="Q%,.0f"),
                                    "tasa_carrito": st.column_config.NumberColumn("Carrito / vistas", format="%.1f%%"),
                                    "tasa_compra": st.column_config.NumberColumn("Compras / vistas", format="%.2f%%")})
