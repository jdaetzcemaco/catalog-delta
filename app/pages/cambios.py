from __future__ import annotations

import streamlit as st

from app import data, ui

st.title("Cambios del día")
if not data.run_days():
    ui.no_data()
    st.stop()

day = st.session_state.day
m = data.manifest(day)
if m["baseline"]:
    st.info("Este es el primer día procesado: no hay un día anterior con el cual comparar.")
    st.stop()

st.caption(f"{ui.fmt_day(day)} comparado con {ui.fmt_day(m['previous_day'])}. "
           "Solo cuentan SKUs que existen ambos días, excepto Nuevos y Eliminados.")

# (sheet name, label, explanation)
VIEWS = [
    ("New SKUs", "Nuevos", "Existen hoy y no existían en el día anterior."),
    ("Removed SKUs", "Eliminados", "Existían en el día anterior y hoy ya no están."),
    ("Newly Visible", "Nuevos visibles", "Pasaron de no visibles a visibles."),
    ("No Longer Visible", "Ya no visibles", "Eran visibles y hoy no lo son."),
    ("Image Changes", "Imagen", "Ganaron o perdieron imagen."),
    ("Price Changes", "Precio", "Ganaron o perdieron precio."),
    ("Stock Flips", "Stock", "Pasaron de tener stock a no tener, o al revés."),
    ("Score Changes", "Score ±10", "El score de contenido subió o bajó 10 puntos o más."),
    ("Top Priorities", "Prioridades", "Visibles y con stock pero con score menor a 80: los 50 peores."),
    ("Stock Not Visible", "Stock no visible", "Tienen stock pero no son visibles (todas las categorías)."),
]
counts = m["row_counts"]
labels = [f"{label} · {ui.n(counts.get(f'changes:{name}', 0))}" for name, label, _ in VIEWS]
choice = st.segmented_control("Tipo de cambio", labels, default=labels[0], key="cambios_view",
                              label_visibility="collapsed")
name, label, why = VIEWS[labels.index(choice or labels[0])]

st.subheader(label)
st.caption(why)
ui.show_table(data.table(day, "changes", name), key=f"chg_{name}", info=data.sku_info(day),
              download_name=f"{name.replace(' ', '_')}_{day}", empty=f"Sin cambios de este tipo.")
