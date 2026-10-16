from __future__ import annotations

import pandas as pd
import streamlit as st

from app import data, ui
from catalog_core.quality import PLACEHOLDER_STOCK
from catalog_core.text import yesno

st.title("Rendimiento")
perf, meta = data.perf(), data.perf_meta()
if perf is None or perf.empty:
    st.info("Aún no hay datos de GA4 / Search Console. El job los baja en cuanto tiene las credenciales de Google "
            "(30 días por corrida).", icon="⏳")
    st.stop()

day = st.session_state.day
info = data.sku_info(day)
st.caption(f"GA4 y Search Console hasta el **{ui.fmt_day(meta['as_of'])}** · catálogo del {ui.fmt_day(meta['catalog_day'])}. "
           "Vistas, carrito, compras e ingresos vienen de GA4 por SKU; sesiones orgánicas de la página del producto "
           "y clics/impresiones/posición de Google por SKU. GA4 omite filas de muy bajo volumen: cero no es un dato duro.")

window = st.segmented_control("Ventana", ["7d", "30d", "90d"], default="30d", key="perf_window",
                              label_visibility="collapsed") or "30d"
W = window
num = lambda c: perf[c].sum() if c in perf.columns else 0  # noqa: E731

c = st.columns(5)
ui.kpi(c[0], f"SKUs con vistas {W}", int((perf[f"vistas_{W}"] > 0).sum()))
ui.kpi(c[1], f"Vistas {W}", int(num(f"vistas_{W}")))
ui.kpi(c[2], f"Agregados al carrito {W}", int(num(f"carrito_{W}")))
ui.kpi(c[3], f"Compras {W}", int(num(f"compras_{W}")))
ui.kpi(c[4], f"Ingresos {W}", f"Q{num(f'ingresos_{W}'):,.0f}")
c = st.columns(3)
ui.kpi(c[0], f"Sesiones orgánicas {W}", int(num(f"organic_sessions_{W}")), help="Sesiones que entraron por Google a una página de producto")
ui.kpi(c[1], f"Clics en Google {W}", int(num(f"clicks_{W}")))
ui.kpi(c[2], f"Impresiones en Google {W}", int(num(f"impressions_{W}")))

# Catalog flags for the diagnostic views
flags = None
if info is not None and "TIENE STOCK" in info.columns:
    flags = info[["SKU", "VISIBLE", "TIENE STOCK", "STOCK"]].drop_duplicates("SKU").copy()
    stock_val = pd.to_numeric(flags["STOCK"], errors="coerce")
    flags["visible"] = yesno(flags["VISIBLE"]) == 1
    flags["con_stock"] = (yesno(flags["TIENE STOCK"]) == 1) | (stock_val.fillna(0) > 0)
    flags["stock_confiable"] = ~(stock_val >= PLACEHOLDER_STOCK)
    flags = flags[["SKU", "visible", "con_stock", "stock_confiable"]]
joined = perf.merge(flags, on="SKU", how="left") if flags is not None else perf

base_cols = ["SKU", f"vistas_{W}", f"carrito_{W}", f"compras_{W}", f"ingresos_{W}", f"organic_sessions_{W}",
             f"clicks_{W}", "posicion_30d", "tasa_carrito_30d", "tasa_compra_30d", "ultimo_dia_vistas"]
base_cols = [col for col in base_cols if col in joined.columns]

tabs = st.tabs(["🏆 Top productos", "🚫 Tráfico sin stock", "😴 Stock sin tráfico", "🔎 SEO", "🔍 Buscar SKU"])

with tabs[0]:
    metric = st.selectbox("Ordenar por", [f"vistas_{W}", f"ingresos_{W}", f"carrito_{W}", f"compras_{W}", f"organic_sessions_{W}"],
                          format_func=lambda c: ui.LABELS.get(c, c), key="perf_top_metric")
    top = joined.sort_values(metric, ascending=False).head(200)
    ui.show_table(top[base_cols], key="perf_top", info=info, download_name=f"top_{metric}_{meta['as_of']}")

with tabs[1]:
    st.caption("Visibles, con vistas en la ventana y **sin stock**: el cliente llega y no puede comprar. "
               "Excluye SKUs con stock no confiable (placeholder 1,000,000).")
    if flags is None:
        st.info("El catálogo de este día no guarda la columna de stock; se completa en la próxima corrida.")
    else:
        hot = joined[joined["visible"].fillna(False) & ~joined["con_stock"].fillna(True) & joined["stock_confiable"].fillna(True)
                     & (joined[f"vistas_{W}"] > 0)].sort_values(f"vistas_{W}", ascending=False)
        ui.kpi(st.columns(3)[0], "SKUs", len(hot), inverse=True)
        ui.show_table(hot[base_cols], key="perf_nostock", info=info, download_name=f"trafico_sin_stock_{meta['as_of']}",
                      empty="Ningún SKU visible con tráfico está sin stock.")

with tabs[2]:
    st.caption(f"Visibles, **con stock** y sin una sola vista en {W}: nadie los encuentra. Candidatos a revisar "
               "nombre, categoría, imagen y SEO.")
    if flags is None:
        st.info("El catálogo de este día no guarda la columna de stock; se completa en la próxima corrida.")
    else:
        cold = joined[joined["visible"].fillna(False) & joined["con_stock"].fillna(False) & joined["stock_confiable"].fillna(True)
                      & (joined[f"vistas_{W}"] == 0)]
        ui.kpi(st.columns(3)[0], "SKUs", len(cold), inverse=True)
        cols = ["SKU", f"impressions_{W}", f"clicks_{W}", "posicion_30d", "ultimo_dia_vistas"]
        ui.show_table(cold[[col for col in cols if col in cold.columns]].head(500), key="perf_cold", info=info,
                      download_name=f"stock_sin_trafico_{meta['as_of']}", empty="Todos los SKUs visibles con stock tuvieron vistas.")
        if len(cold) > 500:
            st.caption(f"Se muestran 500 de {len(cold):,}; usa el filtro de Nivel 1 para acotar la lista.")

with tabs[3]:
    st.caption("Search Console por página de producto. **Oportunidad**: muchas impresiones y posición 4–20 "
               "(Google ya lo muestra, pero bajo); mejorar título/descripción suele subirlo.")
    seo_cols = ["SKU", f"impressions_{W}", f"clicks_{W}", "posicion_30d", f"organic_sessions_{W}", f"vistas_{W}"]
    seo_cols = [col for col in seo_cols if col in joined.columns]
    has_pos = joined["posicion_30d"].notna() if "posicion_30d" in joined.columns else pd.Series(False, index=joined.index)
    opp = joined[has_pos & joined["posicion_30d"].between(4, 20) & (joined[f"impressions_{W}"] >= 100)] \
        .sort_values(f"impressions_{W}", ascending=False)
    k = st.columns(3)
    ui.kpi(k[0], "SKUs con impresiones", int((joined[f"impressions_{W}"] > 0).sum()))
    ui.kpi(k[1], "Posición promedio (30d)", round(float(joined.loc[has_pos, "posicion_30d"].mean()), 1) if has_pos.any() else "—")
    ui.kpi(k[2], "Oportunidades (pos. 4–20, ≥100 impr.)", len(opp))
    ui.show_table(opp[seo_cols].head(300), key="perf_seo", info=info, download_name=f"seo_oportunidades_{meta['as_of']}",
                  empty="Sin oportunidades con esos umbrales.")

with tabs[4]:
    q = st.text_input("SKU", placeholder="Ej. 887607", key="perf_sku_q")
    if q.strip():
        row = joined[joined["SKU"].astype(str) == q.strip()]
        if row.empty:
            st.warning("Ese SKU no está en el catálogo del día.")
        else:
            r = row.iloc[0]
            if info is not None:
                i = info[info["SKU"].astype(str) == q.strip()]
                if len(i):
                    ir = i.iloc[0]
                    cimg, ctxt = st.columns([1, 4])
                    if isinstance(ir.get("URL IMAGEN"), str):
                        cimg.image(ir["URL IMAGEN"], width=140)
                    ctxt.markdown(f"**{ir.get('NOMBRE DE PRODUCTO', '')}**  \n{ir.get('NIVEL 1', '')} · {ir.get('MARCA', '')}"
                                  + (f"  \n[Ver en cemaco.com]({ir['URL']})" if isinstance(ir.get("URL"), str) else ""))
            rows = []
            for w in ("7d", "30d", "90d"):
                rows.append({"Ventana": w, "Vistas": r.get(f"vistas_{w}", 0), "Carrito": r.get(f"carrito_{w}", 0),
                             "Compras": r.get(f"compras_{w}", 0), "Ingresos": r.get(f"ingresos_{w}", 0),
                             "Sesiones": r.get(f"sessions_{w}", 0), "Orgánicas": r.get(f"organic_sessions_{w}", 0),
                             "Clics Google": r.get(f"clicks_{w}", 0), "Impresiones": r.get(f"impressions_{w}", 0)})
            st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch",
                         column_config={"Ingresos": st.column_config.NumberColumn(format="Q%,.0f")})
            extra = {"Posición Google 30d": r.get("posicion_30d"), "Tasa carrito 30d": r.get("tasa_carrito_30d"),
                     "Tasa compra 30d": r.get("tasa_compra_30d"), "Última vista": r.get("ultimo_dia_vistas")}
            st.caption(" · ".join(f"{k}: **{v if not pd.isna(v) else '—'}**" for k, v in extra.items()))
