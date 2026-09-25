from __future__ import annotations

import re
from datetime import datetime

import pandas as pd
import streamlit as st

from app import data, ui
from pipeline.process import latest_per_day, is_new
from pipeline.storage import _join
from pipeline.trigger import trigger_processing

try:
    from zoneinfo import ZoneInfo
    TODAY = datetime.now(ZoneInfo("America/Guatemala")).date()
except Exception:  # pragma: no cover
    TODAY = datetime.now().date()

st.title("Cargar archivos")
st.caption("Normalmente no hace falta: el export diario llega a OneDrive por correo y se procesa solo "
           "(cada 30 minutos). Usa esto cuando el archivo no llegó o hay que reemplazarlo.")

s = data.settings()
storage = data.store().storage
KINDS = {
    "Catálogo diario (STEP)": ("catalog-daily-{day}.xlsx", "catalog_pattern"),
    "Productividad Diseño": ("productivity-diseno-{day}.xlsx", "diseno_pattern"),
    "Productividad Edición": ("productivity-edicion-{day}.xlsx", "edicion_pattern"),
}


def guess_day(name: str):
    m = re.search(r"(\d{4}-\d{2}-\d{2})", name)
    return pd.Timestamp(m.group(1)).date() if m else TODAY


with st.container(border=True):
    kind = st.segmented_control("Tipo de archivo", list(KINDS), default=list(KINDS)[0], key="up_kind")
    kind = kind or list(KINDS)[0]
    up = st.file_uploader("Archivo .xlsx", type=["xlsx"], key=f"up_{kind}")
    if up is not None:
        day = st.date_input("Fecha del archivo", value=guess_day(up.name), format="DD/MM/YYYY",
                            help="Fecha del catálogo que contiene el archivo.")
        target = KINDS[kind][0].format(day=day.isoformat())
        existing = {f.name for f in storage.list(s.incoming_dir, target)}
        replace_ok = True
        if existing:
            replace_ok = st.checkbox(f"Ya existe **{target}** en OneDrive. Reemplazarlo.", key="up_replace")
        st.caption(f"Se guardará como `{_join(s.incoming_dir, target)}` · {up.size / 1e6:,.1f} MB")
        if st.button("Subir y procesar", type="primary", disabled=not replace_ok, icon="📤"):
            with st.spinner("Subiendo a OneDrive…"):
                storage.write(_join(s.incoming_dir, target), up.getvalue())
            try:
                with st.spinner("Procesando…"):
                    outcome = trigger_processing(s)
            except Exception as exc:
                st.error(f"El archivo se subió, pero no se pudo iniciar el procesamiento: {exc}")
            else:
                data.refresh()
                if outcome == "processed":
                    st.success("Listo: archivo procesado. Los resultados ya están disponibles.", icon="✅")
                elif outcome == "triggered":
                    st.success("Archivo subido. El procesamiento empezó y tarda alrededor de un minuto; "
                               "luego pulsa **↻ Actualizar datos**.", icon="✅")
                else:
                    st.success("Archivo subido. Se procesará en la próxima corrida (máximo 30 minutos).", icon="✅")

st.subheader("Estado del procesamiento")
index = data.index()
files = latest_per_day(storage.list(s.incoming_dir, s.catalog_pattern))
pending = [f"{ui.fmt_day(d)} · {f.name}" for d, f in files.items() if is_new(index["catalog"].get(d), f)]
if pending:
    st.warning("Pendientes de procesar: " + ", ".join(pending), icon="⏳")
else:
    st.caption("No hay catálogos pendientes en OneDrive.")

rows = [
    {"Día": pd.Timestamp(d), "Archivo": e.get("source"), "Comparado con": e.get("previous_day"),
     "Procesado": pd.Timestamp(e["processed_at"]).tz_convert("America/Guatemala").tz_localize(None)
     if e.get("processed_at") else None}
    for d, e in index["catalog"].items()
]
if rows:
    st.dataframe(pd.DataFrame(rows).sort_values("Día", ascending=False).head(15), hide_index=True, width="stretch",
                 column_config={"Día": st.column_config.DateColumn(format="DD/MM/YYYY"),
                                "Procesado": st.column_config.DatetimeColumn(format="DD/MM/YYYY HH:mm")})
