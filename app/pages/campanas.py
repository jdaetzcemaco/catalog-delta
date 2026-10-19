from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from app import data, ui
from catalog_core.campaigns import (CHECKS, PENDING_REASONS, list_slug, parse_sku_list, pending_breakdown,
                                    pending_kpis, readiness_kpis, regressions)

st.title("Campañas")
st.caption("Listas de SKUs que van a salir en promociones o temporadas. Cada día el tablero revisa si cada "
           "SKU está listo para venderse en línea, hasta que la lista se archive.")

lists = data.lists()


# ── Nueva lista ──────────────────────────────────────────────────────────────
def _new_list_form() -> None:
    with st.expander("➕ Nueva lista", expanded=not lists):
        name = st.text_input("Nombre de la campaña", placeholder="Ej. Navidad 2026 · Decoración")
        c1, c2 = st.columns(2)
        owner = c1.text_input("Responsable", placeholder="Opcional")
        notes = c2.text_input("Notas", placeholder="Opcional")
        up = st.file_uploader("Excel con los SKUs (cualquier hoja con una columna SKU / Código / ID)",
                              type=["xlsx", "csv"], key="list_upload")
        parsed = None
        if up is not None:
            try:
                raw = pd.read_csv(up, dtype=str) if up.name.lower().endswith(".csv") else \
                    pd.read_excel(up, dtype=str, engine="calamine")
                skus, column = parse_sku_list(raw)
                parsed = skus
                info = data.sku_info(st.session_state.day) if data.run_days() else None
                known = set(info["SKU"]) if info is not None else set()
                missing = [s for s in skus if known and s not in known]
                st.caption(f"Columna **{column}** · {len(skus):,} SKUs únicos" +
                           (f" · **{len(missing)} no están en el catálogo del {ui.fmt_day(st.session_state.day)}**"
                            if missing else ""))
                if missing:
                    st.code(", ".join(missing[:50]) + (" …" if len(missing) > 50 else ""), language=None)
            except Exception as exc:
                st.error(f"No se pudo leer el archivo: {exc}")
        slug = list_slug(name) if name else ""
        clash = slug in lists
        if clash:
            st.warning(f"Ya existe una lista con el id `{slug}`. Cambia el nombre o archívala primero.")
        if st.button("Crear lista", type="primary", disabled=not (name and parsed and not clash), icon="🎯"):
            data.write_list({
                "id": slug, "name": name.strip(), "owner": owner.strip(), "notes": notes.strip(),
                "skus": parsed, "status": "active", "source_file": up.name,
                "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            })
            data.refresh()
            st.success(f"Lista **{name}** guardada con {len(parsed):,} SKUs. El job la evalúa en su próxima "
                       "corrida (máx. 30 min); luego pulsa **↻ Actualizar datos**.", icon="✅")


_new_list_form()

if not lists:
    st.info("Aún no hay listas. Crea la primera con el Excel de SKUs de la próxima campaña.", icon="🎯")
    st.stop()

# ── Selector ─────────────────────────────────────────────────────────────────
active = {k: v for k, v in lists.items() if v.get("status", "active") == "active"}
archived = {k: v for k, v in lists.items() if k not in active}
show_archived = st.toggle("Mostrar archivadas", value=False) if archived else False
options = list(lists if show_archived else active) or list(lists)


def _label(lid: str) -> str:
    e = lists[lid]
    k = e.get("last_kpis") or {}
    tag = "" if e.get("status", "active") == "active" else " · archivada"
    return f"{e.get('name') or lid} ({k.get('listos', '–')}/{k.get('total', e.get('sku_count', '–'))} listos){tag}"


lid = st.selectbox("Lista", options, format_func=_label)
entry, meta = lists[lid], data.list_meta(lid) or {}
is_active = entry.get("status", "active") == "active"
days = entry.get("status_days") or []

head = st.columns([3, 1])
head[0].caption(" · ".join(filter(None, [
    f"Responsable: **{meta.get('owner')}**" if meta.get("owner") else "",
    f"Creada {ui.fmt_day(meta['created_at'][:10])}" if meta.get("created_at") else "",
    f"Archivo `{meta.get('source_file')}`" if meta.get("source_file") else "",
    meta.get("notes") or "",
])))
with head[1]:
    if is_active and st.button("Archivar lista", width="stretch", icon="📦"):
        data.write_list({**meta, "status": "archived",
                         "archived_at": datetime.now(timezone.utc).isoformat(timespec="seconds")})
        data.refresh()
        st.toast("Lista archivada; el job deja de evaluarla en su próxima corrida.")
    elif not is_active and st.button("Reactivar lista", width="stretch", icon="🔄"):
        data.write_list({**meta, "status": "active"})
        data.refresh()
        st.toast("Lista reactivada.")

if not days:
    st.info("La lista ya está guardada; el job la evalúa en su próxima corrida (máximo 30 minutos).", icon="⏳")
    st.stop()

# ── Estado del día ───────────────────────────────────────────────────────────
day = st.selectbox("Evaluación del", sorted(days, reverse=True), format_func=ui.fmt_day) if len(days) > 1 else days[-1]
status = data.list_status(lid, day)
if status is None:
    st.warning("No se encontró la evaluación de ese día.")
    st.stop()
prev_day = max((d for d in days if d < day), default=None)
prev = data.list_status(lid, prev_day) if prev_day else None
k = readiness_kpis(status)
pk = readiness_kpis(prev) if prev is not None else None
ui.quality_banner(data.manifest(day))

c = st.columns(5)
ui.kpi(c[0], "SKUs en la lista", k["total"])
ui.kpi(c[1], "Listos", k["listos"], (k["listos"] - pk["listos"]) if pk else None)
ui.kpi(c[2], "% listos", k["pct_listos"], round(k["pct_listos"] - pk["pct_listos"], 1) if pk else None, suffix="%")
lost = regressions(status, prev)
ui.kpi(c[3], "Dejaron de estar listos", len(lost), inverse=True,
       help=f"Listos el {ui.fmt_day(prev_day)} y no hoy" if prev_day else "Necesita una evaluación anterior")
ui.kpi(c[4], "Stock no confiable", k["stock_no_confiable"],
       help="STOCK = 1,000,000 en el export: no bloquea 'listo', pero no se puede confirmar el inventario")

st.subheader("Qué falta")
fail = {label: k[f"faltan_{key}"] for key, label in CHECKS.items() if k[f"faltan_{key}"]}
if fail:
    cols = st.columns(len(fail))
    for col_, (label, n) in zip(cols, fail.items()):
        ui.kpi(col_, label, n, inverse=True)
else:
    st.success("Todos los SKUs de la lista están listos.", icon="✅")

if k["total"] - k["listos"]:
    pend = pending_kpis(status)
    st.subheader("Pendientes por motivo")
    st.caption("**Solo falta inventario**: todo lo demás está bien; no está visible solo porque no tiene stock "
               "(en el export ningún SKU queda visible sin stock). **Pendiente en STEP**: falla habilitado, contenido "
               "o categoría, o tiene stock y aun así no está visible; se corrige en STEP. **Aún no llega al export de "
               "STEP**: el código nunca apareció en un export; suele ser un SKU nuevo que todavía no se crea o no "
               "sale del flujo.")
    pc = st.columns(3)
    ui.kpi(pc[0], PENDING_REASONS["solo_inventario"], pend["solo_inventario"], inverse=True)
    ui.kpi(pc[1], PENDING_REASONS["step"], pend["step"], inverse=True,
           help=f"De estos, {pend['step_sin_stock']:,} además no tienen stock. Por criterio: "
                + ", ".join(f"{CHECKS[c].split(' (')[0]} {n:,}" for c, n in pend["step_por_check"].items() if n))
    ui.kpi(pc[2], PENDING_REASONS["fuera_catalogo"], pend["fuera_catalogo"], inverse=True)

if len(lost):
    st.subheader(f"Dejaron de estar listos desde el {ui.fmt_day(prev_day)}")
    ui.show_table(lost[["SKU", "perdio", "score", "ultima_vez_visible"]], key="camp_lost",
                  info=data.sku_info(day), download_name=f"{lid}_regresiones_{day}", filters=False)

perf = data.perf()
if perf is not None and not perf.empty:
    pcols = ["SKU", "vistas_30d", "carrito_30d", "compras_30d", "organic_sessions_30d", "clicks_30d", "posicion_30d"]
    lp = perf[[c for c in pcols if c in perf.columns]]
    status = status.merge(lp, on="SKU", how="left")
    lost = lost.merge(lp, on="SKU", how="left") if len(lost) else lost
    st.subheader("Tráfico de la lista (30 días)")
    pk_ = st.columns(4)
    ui.kpi(pk_[0], "Vistas", int(status["vistas_30d"].fillna(0).sum()))
    ui.kpi(pk_[1], "Agregados al carrito", int(status["carrito_30d"].fillna(0).sum()))
    ui.kpi(pk_[2], "Sesiones orgánicas", int(status["organic_sessions_30d"].fillna(0).sum()))
    ui.kpi(pk_[3], "SKUs sin una vista", int((status["vistas_30d"].fillna(0) == 0).sum()), inverse=True,
           help="SKUs de la lista sin vistas en 30 días (visibles o no)")

st.subheader("SKUs")
view = st.segmented_control("Ver", ["Pendientes", "Listos", "Todos"], default="Pendientes", key="camp_view",
                            label_visibility="collapsed") or "Pendientes"
shown = status if view == "Todos" else status[status["listo"] == (view == "Listos")]
if view == "Pendientes" and len(shown):
    shown = pending_breakdown(status)
    labels = {"todos": "Todos los motivos", **PENDING_REASONS}
    reason = st.radio("Motivo", list(labels), format_func=labels.get, horizontal=True, key="camp_reason",
                      label_visibility="collapsed")
    if reason != "todos":
        shown = shown[shown["motivo_pendiente"] == reason]
    shown = shown.assign(motivo_pendiente=shown["motivo_pendiente"].map(PENDING_REASONS))
cols = ["SKU", "motivo_pendiente", "falta_en_step", "tambien_sin_stock", "listo", "visible", "habilitado", "contenido", "categoria_real", "stock", "stock_confiable",
        "score", "vistas_30d", "carrito_30d", "organic_sessions_30d", "clicks_30d", "posicion_30d",
        "ultima_vez_visible", "dias_visible_seguidos", "salida_flujo", "fin_dat_log", "equipo", "en_catalogo"]
ui.show_table(shown[[c for c in cols if c in shown.columns]], key=f"camp_{view}", info=data.sku_info(day),
              download_name=f"{lid}_{view.lower()}_{day}",
              empty="Ningún SKU en esta vista." if view != "Pendientes" else "No hay pendientes: todo listo.")
