from __future__ import annotations

import altair as alt
import streamlit as st

from app import data, ui

st.title("Historial")
hist = data.history()
if hist.empty:
    ui.no_data()
    st.stop()

st.caption(f"{len(hist)} días procesados, del {ui.fmt_day(hist['Fecha'].min())} al {ui.fmt_day(hist['Fecha'].max())}.")

METRICS = {
    "Score promedio": "Avg Content Score",
    "Visibles %": "Visible %",
    "Con imagen %": "With Image %",
    "Con precio %": "With Price %",
    "Con stock %": "With Stock %",
    "Total SKUs": "Total SKUs",
    "Visibles": "Visible",
    "Score perfecto": "Perfect Score",
    "Acciones urgentes": "inv:acciones_urgentes",
    "Stock sin visibilidad": "inv:stock_no_visible",
    "Deshabilitados con stock": "inv:deshabilitados_stock",
    "Tipo C con inventario": "inv:tipo_c_stock",
}
chosen = st.multiselect("Métricas", list(METRICS), default=["Score promedio", "Visibles %", "Con imagen %"])


def line(label: str) -> alt.Chart:
    col = METRICS[label]
    return alt.Chart(hist).mark_line(point=True).encode(
        x=alt.X("Fecha:T", title=None, axis=alt.Axis(format="%d %b")),
        y=alt.Y(f"{col}:Q", title=label, scale=alt.Scale(zero=False)),
        tooltip=[alt.Tooltip("Fecha:T", format="%d/%m/%Y"), alt.Tooltip(f"{col}:Q", title=label, format=",.2f")],
    ).properties(height=220, title=label)


cols = st.columns(2)
for i, label in enumerate(chosen):
    with cols[i % 2], st.container(border=True):
        st.altair_chart(line(label), use_container_width=True)

with st.expander("Tabla"):
    table = hist.rename(columns={v: k for k, v in METRICS.items()})
    st.dataframe(table[["Fecha", *[k for k in METRICS if k in table.columns]]].sort_values("Fecha", ascending=False),
                 hide_index=True, width="stretch",
                 column_config={"Fecha": st.column_config.DateColumn(format="DD/MM/YYYY")})
