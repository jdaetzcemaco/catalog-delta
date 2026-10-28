from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from app import data, ui
from catalog_core.seo_impact import ALL_TEAMS, BUCKET_LABELS, QUERY_STATUS, STATUS

st.title("Impacto SEO del trabajo de catálogo")
meta = data.seo_meta()
cohorts, skus = data.seo("cohorts"), data.seo("skus")
if not meta or cohorts is None or cohorts.empty or skus is None or skus.empty:
    st.info("Aún no hay medición. El job la arma cuando tiene el historial de productividad (work log) y Search "
            "Console por SKU; tarda una corrida después de la primera descarga.", icon="⏳")
    st.stop()

W = int(meta.get("window", 14))
st.caption(
    f"Cada SKU que **salió del flujo de STEP** (reportes de productividad) se mide **{W} días antes y {W} después** "
    f"de esa fecha en Google (búsqueda web, página de producto), contra un **control** de SKUs no trabajados con el "
    f"mismo tramo de impresiones previas y las mismas fechas. Search Console hasta el **{ui.fmt_day(meta['as_of'])}**; "
    f"una semana queda completa ~{W + 3} días después de su último evento. Se lee aparte la *indexación* "
    "(aparecer por primera vez), la *cobertura* (consultas, impresiones, clics) y el *ranking* (posición)."
)

cohorts = cohorts.copy()
cohorts["cohorte"] = pd.to_datetime(cohorts["cohorte"])
skus = skus.copy()
skus["cohorte"] = pd.to_datetime(skus["cohorte"])
skus["event_day"] = pd.to_datetime(skus["event_day"])

# ── selectors ───────────────────────────────────────────────────────────────
weeks = sorted(cohorts["cohorte"].unique(), reverse=True)
tot = cohorts[cohorts["equipo"] == ALL_TEAMS].set_index("cohorte")


def week_label(w) -> str:
    r = tot.loc[w] if w in tot.index else None
    state = "" if r is None else (" · completa" if bool(r["completo"]) else " · pendiente")
    n = "" if r is None else f" · {ui.n(r['skus'])} SKUs"
    return f"Semana del {ui.fmt_day(w)}{n}{state}"


complete_weeks = [w for w in weeks if w in tot.index and bool(tot.loc[w, "completo"])]
wanted = st.session_state.pop("seo_cohort", None)
default = next((w for w in weeks if wanted and w.strftime("%Y-%m-%d") == wanted), complete_weeks[0] if complete_weeks else weeks[0])
c1, c2 = st.columns([3, 2])
week = c1.selectbox("Semana de salida del flujo", weeks, index=weeks.index(default), format_func=week_label, key="seo_week")
teams = [t for t in [ALL_TEAMS, "Edición", "Diseño", "Diseño+Edición"] if t in set(cohorts.loc[cohorts["cohorte"] == week, "equipo"])]
team = c2.radio("Equipo", teams, horizontal=True, key="seo_team",
                help="«Diseño+Edición»: el SKU aparece en los reportes de ambos equipos con menos de 7 días de diferencia.") or ALL_TEAMS

row = cohorts[(cohorts["cohorte"] == week) & (cohorts["equipo"] == team)]
if row.empty:
    st.warning("Sin datos para esa combinación.")
    st.stop()
r = row.iloc[0]
sel = skus[(skus["cohorte"] == week) & ((skus["equipo"] == team) if team != ALL_TEAMS else True)]
info = data.sku_info(st.session_state.day) if data.run_days() else None

if not bool(r["completo"]):
    st.warning(f"Ventana incompleta: faltan días de Search Console para cerrar los {W} días posteriores. Los KPIs "
               "solo cuentan los SKUs ya completos.", icon="⏳")
elif bool(r.get("muestra_pequena", False)):
    st.info(f"Muestra pequeña: solo {int(r['con_presencia'])} SKUs con presencia en Google. Lee el Δ vs control como "
            "orientación, no como medida; varias semanas juntas pesan más.", icon="ℹ️")


# ── KPIs ────────────────────────────────────────────────────────────────────
def pct(v) -> str:
    return "—" if v is None or pd.isna(v) else f"{v:+.0f}%"


ci = ""
if pd.notna(r.get("uplift_impr_ci_low")):
    ci = f"IC 95 %: {r['uplift_impr_ci_low']:+.0f}% a {r['uplift_impr_ci_high']:+.0f}% (bootstrap sobre SKUs). "
k = st.columns(5)
ui.kpi(k[0], "SKUs trabajados", int(r["skus"]), help="Salieron del flujo esa semana (cada SKU una vez).")
ui.kpi(k[1], "Con presencia en Google", int(r["con_presencia"]), help="Con al menos una impresión antes o después.")
ui.kpi(k[2], "Nuevos en Google", int(r["nuevo"]),
       help=("Sin impresiones antes y con impresiones después (primera vez). Mediana de días hasta la primera impresión: "
             f"{r['dias_a_primera_impresion_mediana']:.0f}" if pd.notna(r.get("dias_a_primera_impresion_mediana")) else
             "Sin impresiones antes y con impresiones después (primera vez)."))
ui.kpi(k[3], "Δ impresiones vs control", pct(r.get("uplift_impr_pct")),
       help=f"Trabajados {pct(r.get('delta_impr_pct'))} · control {pct(r.get('ctrl_delta_impr_pct'))}. {ci}"
            "Impresiones de la página de producto en búsqueda web.")
ui.kpi(k[4], "Δ clics vs control", pct(r.get("uplift_clics_pct")),
       help=f"Trabajados {pct(r.get('delta_clics_pct'))} · control {pct(r.get('ctrl_delta_clics_pct'))}.")
k = st.columns(6)
ui.kpi(k[0], "Crecieron", int(r["crecio"]), help="≥ +20 % y ≥ 10 impresiones más.")
ui.kpi(k[1], "Bajaron", int(r["bajo"]), inverse=True, help="≤ −20 % y ≥ 10 impresiones menos, o perdieron toda presencia.")
ui.kpi(k[2], "Estables", int(r["estable"]))
ui.kpi(k[3], "Sin presencia", int(r["sin_presencia"]), inverse=True,
       help="Visibles en el sitio y sin una sola impresión en Google antes ni después: revisar indexación.")
ui.kpi(k[4], "No visibles", int(r["no_visible"]), inverse=True, help="No estaban visibles en el sitio después del trabajo.")
pos = "—" if pd.isna(r.get("pos_antes")) else f"{r['pos_antes']:.1f} → {r['pos_despues']:.1f}"
ui.kpi(k[5], "Posición (antes → después)", pos, help="Ponderada por impresiones, SKUs con presencia en ambas ventanas.")

tabs = st.tabs(["📈 Antes y después", "🧱 Por tramo", "📋 SKUs", "🔍 Ficha por SKU"])

# ── chart ───────────────────────────────────────────────────────────────────
with tabs[0]:
    daily = data.seo("daily")
    if daily is None or daily.empty:
        st.info("Sin serie diaria todavía.")
    else:
        daily = daily.copy()
        daily["cohorte"] = pd.to_datetime(daily["cohorte"])
        d = daily[(daily["cohorte"] == week) & (daily["equipo"] == team)]
        if d.empty or d["indice"].isna().all():
            st.info("La serie se dibuja cuando la semana está completa y tiene SKUs con presencia.")
        else:
            d = d.assign(grupo=d["grupo"].map({"trabajados": "Trabajados", "control": "Control"}))
            base = alt.Chart(d).encode(x=alt.X("offset:Q", title="Días desde la salida del flujo"),
                                       y=alt.Y("indice:Q", title="Impresiones (promedio previo = 100)"),
                                       color=alt.Color("grupo:N", title=None),
                                       tooltip=["grupo", "offset", alt.Tooltip("impressions:Q", format=",.0f", title="Impresiones"),
                                                alt.Tooltip("clicks:Q", format=",.0f", title="Clics"), alt.Tooltip("indice:Q", format=".0f")])
            rule = alt.Chart(d).mark_rule(strokeDash=[4, 4], color="#888").encode(x=alt.datum(0))
            st.altair_chart((base.mark_line(point=True) + rule).properties(height=320), use_container_width=True)
            st.caption("Índice 100 = promedio diario de impresiones en los 14 días previos de cada grupo. El control son los "
                       "SKUs no trabajados, alineados a la fecha mediana de la semana.")
    st.markdown("**Cómo leerlo**")
    st.markdown(
        "- **Nuevos en Google** es indexación: la ficha empezó a aparecer. Si la mediana de días es baja, Google la tomó rápido.\n"
        "- **Δ vs control** es cobertura: cuánto más (o menos) crecieron las impresiones de los trabajados frente a SKUs "
        "parecidos no trabajados en las mismas fechas. Quita el efecto de temporada y de campañas.\n"
        "- **Posición** casi no se mueve con este trabajo: las fichas ganan consultas nuevas, no suben en las que ya tenían.\n"
        "- **Sin presencia** es la lista accionable: visibles y que Google no muestra; conviene revisar sitemap, enlazado interno "
        "y que la página responda."
    )

# ── buckets ─────────────────────────────────────────────────────────────────
with tabs[1]:
    buckets = data.seo("buckets")
    st.caption("Mismo tramo de impresiones previas, trabajados vs control. Un SKU con 5 impresiones puede «crecer» 300 %; "
               "por eso se compara dentro de cada tramo.")
    if buckets is None or buckets.empty:
        st.info("Sin tramos todavía.")
    else:
        b = buckets.copy()
        b["cohorte"] = pd.to_datetime(b["cohorte"])
        b = b[(b["cohorte"] == week) & (b["equipo"] == team)]
        b["bucket"] = pd.Categorical(b["bucket"], BUCKET_LABELS, ordered=True)
        b = b.sort_values("bucket")
        cols = ["bucket", "n", "impr_antes", "impr_despues", "delta_impr_pct", "delta_clics_pct", "ctrl_n", "ctrl_delta_impr_pct",
                "ctrl_delta_clics_pct"]
        st.dataframe(b[cols], hide_index=True, width="stretch", column_config=ui._column_config(b[cols]))

# ── SKU table ───────────────────────────────────────────────────────────────
with tabs[2]:
    states = [s for s in STATUS if s in set(sel["estado"])]
    chosen = st.multiselect("Estado", states, default=[], format_func=lambda s: STATUS[s], key="seo_estado",
                            placeholder="Todos los estados")
    t = sel[sel["estado"].isin(chosen)] if chosen else sel
    t = t.assign(estado=t["estado"].map(STATUS))
    cols = [c for c in ["SKU", "estado", "equipo", "usuarios", "event_day", "impr_antes", "impr_despues", "delta_impr_pct",
                        "uplift_impr_pct", "clics_antes", "clics_despues", "pos_antes", "pos_despues", "dias_a_primera_impresion",
                        "consultas_nuevas", "consultas_perdidas", "top_consulta_despues", "cambios_ficha", "score_antes",
                        "score_despues", "impr_ml_despues", "impr_ps_despues"] if c in t.columns]
    ui.show_table(t[cols].sort_values("impr_despues", ascending=False, na_position="last"), key="seo_skus", info=info,
                  download_name=f"impacto_seo_{week:%Y-%m-%d}_{team}")
    missing = sel[sel["estado"] == "sin_presencia"]
    if not missing.empty:
        st.markdown(f"**Visibles y sin presencia en Google a los {W} días ({ui.n(len(missing))})** — para revisar indexación.")
        mcols = [c for c in ["SKU", "equipo", "usuarios", "event_day", "cambios_ficha", "score_despues"] if c in missing.columns]
        ui.show_table(missing[mcols], key="seo_missing", info=info, download_name=f"sin_presencia_{week:%Y-%m-%d}_{team}",
                      filters=False, height=300)

# ── SKU detail ──────────────────────────────────────────────────────────────
with tabs[3]:
    q = st.text_input("SKU", placeholder="Ej. 1080127", key="seo_sku_q")
    if q.strip():
        rowd = skus[skus["SKU"].astype(str) == q.strip()]
        if rowd.empty:
            st.warning("Ese SKU no aparece en el work log de productividad.")
        else:
            rd = rowd.iloc[0]
            if info is not None:
                i = info[info["SKU"].astype(str) == q.strip()]
                if len(i):
                    ir = i.iloc[0]
                    cimg, ctxt = st.columns([1, 4])
                    if isinstance(ir.get("URL IMAGEN"), str):
                        cimg.image(ir["URL IMAGEN"], width=140)
                    ctxt.markdown(f"**{ir.get('NOMBRE DE PRODUCTO', '')}**  \n{ir.get('NIVEL 1', '')} · {ir.get('MARCA', '')}"
                                  + (f"  \n[Ver en cemaco.com]({ir['URL']})" if isinstance(ir.get("URL"), str) else ""))
            st.markdown(f"Salió del flujo el **{ui.fmt_day(rd['event_day'])}** ({rd['equipo']}; {rd.get('usuarios') or 'sin usuario'}) · "
                        f"estado: **{STATUS.get(rd['estado'], rd['estado'])}**"
                        + (f" · cambios en la ficha: {rd['cambios_ficha']}" if isinstance(rd.get("cambios_ficha"), str) and rd["cambios_ficha"] else ""))
            m = st.columns(4)
            ui.kpi(m[0], "Impresiones", f"{rd['impr_antes']:,.0f} → {rd['impr_despues']:,.0f}" if pd.notna(rd["impr_despues"]) else f"{rd['impr_antes']:,.0f} → …")
            ui.kpi(m[1], "Clics", f"{rd['clics_antes']:,.0f} → {rd['clics_despues']:,.0f}" if pd.notna(rd["clics_despues"]) else f"{rd['clics_antes']:,.0f} → …")
            ui.kpi(m[2], "Posición", f"{rd['pos_antes']:.1f} → {rd['pos_despues']:.1f}" if pd.notna(rd.get("pos_antes")) and pd.notna(rd.get("pos_despues")) else "—")
            ui.kpi(m[3], "Δ vs control", pct(rd.get("uplift_impr_pct")))
            queries = data.seo("queries")
            qs = queries[queries["SKU"].astype(str) == q.strip()] if queries is not None and not queries.empty else pd.DataFrame()
            if qs.empty:
                st.caption("Sin consultas guardadas para este SKU (se calculan cuando la semana está completa y los días "
                           "vienen del export de BigQuery).")
            else:
                qs = qs[~qs["anonima"].fillna(False).astype(bool)].assign(estado=lambda d: d["estado"].map(QUERY_STATUS))
                qcols = ["query", "estado", "impr_antes", "impr_despues", "clics_antes", "clics_despues", "pos_antes", "pos_despues", "ml", "ps"]
                a, b = st.columns(2)
                a.markdown("**Ganadas o que subieron**")
                ui.show_table(qs[qs["estado"].isin(["Nueva", "Subió"])][qcols].sort_values("impr_despues", ascending=False),
                              key="seo_q_up", filters=False, download_name=f"consultas_ganadas_{q.strip()}", empty="Ninguna.")
                b.markdown("**Perdidas o que bajaron**")
                ui.show_table(qs[qs["estado"].isin(["Perdida", "Bajó"])][qcols].sort_values("impr_antes", ascending=False),
                              key="seo_q_down", filters=False, download_name=f"consultas_perdidas_{q.strip()}", empty="Ninguna.")
                st.markdown("**Todas las consultas**")
                ui.show_table(qs[qcols].sort_values("impr_despues", ascending=False), key="seo_q_all", filters=False,
                              download_name=f"consultas_{q.strip()}")
