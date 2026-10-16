from __future__ import annotations

import pandas as pd
import streamlit as st

from app import data, ui

SEV = {"alta": ("🔴", "Alta"), "media": ("🟠", "Media"), "baja": ("🔵", "Baja")}
PAGE_LINK = {"campanas": ("app/pages/campanas.py", "Campañas"), "cambios": ("app/pages/cambios.py", "Cambios del día"),
             "inventario": ("app/pages/inventario.py", "Inventario Omnicanal"), "cargar": ("app/pages/cargar.py", "Cargar archivos"),
             "rendimiento": ("app/pages/rendimiento.py", "Rendimiento")}

st.title("Alertas")
days = data.alert_days()
if not days:
    st.info("Aún no hay alertas calculadas; el job las genera en cada corrida.", icon="⏳")
    st.stop()

top = st.columns([1, 1, 2])
day = top[0].selectbox("Día", days, format_func=ui.fmt_day)
show_acked = top[1].toggle("Mostrar revisadas", value=False)
all_alerts = data.alerts(day)
acked = data.acks()
shown = all_alerts if show_acked else [a for a in all_alerts if a["id"] not in acked]
counts = {s: sum(1 for a in shown if a["severity"] == s) for s in SEV}
top[2].caption(f"Generadas {pd.Timestamp(all_alerts[0]['generated_at']).tz_convert('America/Guatemala'):%d/%m %H:%M} · "
               f"{len(all_alerts)} alertas, {len(all_alerts) - len([a for a in all_alerts if a['id'] not in acked])} revisadas"
               if all_alerts else "Sin alertas ese día.")

c = st.columns(3)
for col_, (sev, (icon, label)) in zip(c, SEV.items()):
    ui.kpi(col_, f"{icon} {label}", counts[sev], inverse=True)

if not shown:
    st.success("No hay alertas pendientes para este día.", icon="✅")

info = data.sku_info(day)
for a in shown:
    icon, label = SEV.get(a["severity"], ("⚪", a["severity"]))
    since = f" · desde el {ui.fmt_day(a['first_seen'])}" if a.get("first_seen") and a["first_seen"] != a["day"] else ""
    reviewed = a["id"] in acked
    with st.expander(f"{icon} {a['title']}{since}" + (" · ✔ revisada" if reviewed else ""), expanded=not reviewed):
        st.write(a["detail"])
        b1, b2, _ = st.columns([1, 1, 3])
        page, page_title = PAGE_LINK.get(a["page"], (None, None))
        if page:
            b1.page_link(page, label=f"Ir a {page_title}", icon="↗")
        if reviewed:
            if b2.button("Reabrir", key=f"un_{a['id']}", width="stretch"):
                data.unacknowledge(a["id"])
                st.rerun()
        elif b2.button("Marcar revisada", key=f"ack_{a['id']}", width="stretch", type="primary"):
            data.acknowledge(a["id"])
            st.rerun()
        if a.get("skus"):
            df = pd.DataFrame({"SKU": a["skus"]})
            ui.show_table(df, key=f"al_{a['id']}", info=info, download_name=f"alerta_{a['rule']}_{day}",
                          filters=len(df) > 10)
            if a.get("count", 0) > len(a["skus"]):
                st.caption(f"Se muestran {len(a['skus'])} de {a['count']:,} SKUs; la tabla completa está en {page_title}.")

with st.expander("Historial de alertas por día"):
    rows = [{"Día": pd.Timestamp(d), **{SEV[s][1]: sum(1 for a in data.alerts(d) if a["severity"] == s) for s in SEV}}
            for d in sorted(days)]
    st.dataframe(pd.DataFrame(rows).sort_values("Día", ascending=False), hide_index=True, width="stretch",
                 column_config={"Día": st.column_config.DateColumn(format="DD/MM/YYYY")})
