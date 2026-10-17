"""
CRO watchlist: which SKUs deserve attention, and why, from 30 days of GA4 per SKU
compared against their own category (NIVEL 1).

Rates are shrunk toward the category rate (empirical Bayes: `prior` pseudo-views at the
category rate), so a SKU with 3 views and 1 purchase does not look like 33% conversion.
Every flagged SKU carries a reason in plain Spanish and an estimated impact used to sort.
Pure: the job feeds it the per-SKU performance frame with catalog context columns.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

GROUPS = {
    "fuga_conversion": "🔥 Fuga de conversión",
    "carrito_sin_compra": "🛒 Carrito sin compra",
    "pagado_sin_retorno": "💸 Pagado sin retorno",
    "joya_oculta": "💎 Joya oculta",
    "organico_en_riesgo": "📉 Orgánico en riesgo",
    "trafico_sin_stock": "🚫 Tráfico sin stock",
}
GROUP_ACTIONS = {
    "fuga_conversion": "Revisar precio vs competencia, fotos, descripción, reseñas y disponibilidad de variantes.",
    "carrito_sin_compra": "Revisar envío y costo final, stock real al pagar, cupones y restricciones de pago.",
    "pagado_sin_retorno": "Revisar la pauta del SKU: audiencia, landing y oferta; pausar si no mejora.",
    "joya_oculta": "Darle exposición: home, categorías, colecciones y pauta; convierte mejor que su categoría.",
    "organico_en_riesgo": "SEO: título, descripción, contenido y disponibilidad; ver posición en Google.",
    "trafico_sin_stock": "Reponer o despublicar; el cliente llega y no puede comprar.",
}
PAID = ("pagado_google", "pagado_social")


@dataclass
class CROThresholds:
    min_views: int = 300            # views in 30d to judge the cart rate
    min_carts: int = 30             # carts in 30d to judge cart → purchase
    min_paid_views: int = 300       # paid views in 30d to judge paid conversion
    gem_min_views: int = 30         # a hidden gem still needs some evidence
    prior: int = 100                # pseudo-views at the category rate (shrinkage strength)
    low_ratio: float = 0.5          # "far below its category" = ≤ 50% of the category rate
    high_ratio: float = 2.0         # "far above" = ≥ 2× the category rate
    organic_min_prev: int = 20      # previous-week organic sessions needed
    organic_drop_pct: float = 40.0
    no_stock_min_views: int = 50    # views in 7d, visible and no stock


def _shrunk(rate: pd.Series, n: pd.Series, prior_rate: pd.Series, prior: int) -> pd.Series:
    """Empirical-Bayes estimate: `prior` pseudo-observations at the category rate."""
    return (rate.fillna(0) * n + prior * prior_rate) / (n + prior)


def _wmean(rate: pd.Series, w: pd.Series) -> float:
    w = w.where(rate.notna(), 0)
    return float((rate.fillna(0) * w).sum() / w.sum()) if w.sum() > 0 else float("nan")


def _category_rates(p: pd.DataFrame) -> pd.DataFrame:
    """Views-weighted rates per NIVEL 1 over SKUs with some evidence; site rate as fallback."""
    base = p[p["vistas_ventana"] >= 30]
    rows = []
    for cat, g in base.groupby("NIVEL 1"):
        rows.append({"NIVEL 1": cat, "cat_cart_rate": _wmean(g["cart_rate"], g["vistas_ventana"]),
                     "cat_buy_rate": _wmean(g["buy_rate"], g["vistas_ventana"]),
                     "cat_paid_buy_rate": _wmean(g["paid_buy_rate"], g["paid_views"]),
                     "med_v": float(g["vistas_ventana"].median())})
    cats = pd.DataFrame(rows, columns=["NIVEL 1", "cat_cart_rate", "cat_buy_rate", "cat_paid_buy_rate", "med_v"])
    site = {"cat_cart_rate": _wmean(base["cart_rate"], base["vistas_ventana"]),
            "cat_buy_rate": _wmean(base["buy_rate"], base["vistas_ventana"]),
            "cat_paid_buy_rate": _wmean(base["paid_buy_rate"], base["paid_views"])}
    for c, v in site.items():
        cats[c] = cats[c].fillna(v)
    cats["cat_buy_per_cart"] = (cats["cat_buy_rate"] / cats["cat_cart_rate"].where(cats["cat_cart_rate"] > 0)).clip(0, 1)
    return cats


def classify(perf: pd.DataFrame, t: CROThresholds | None = None) -> pd.DataFrame:
    """
    One row per (SKU, group) flagged, with metrics, category benchmark, reason and impact.

    `perf` needs: SKU, NIVEL 1, visible, con_stock; the 30-day window columns from GA4
    `vistas_ventana`, `cart_rate`, `buy_rate` (user-based), `paid_views`, `paid_buy_rate`;
    plus `vistas_7d`, `organic_sessions_7d(_prev)` for the organic and stock groups.
    Units (carrito_30d, compras_30d, ingresos_30d) are carried for display only.
    """
    from .inventory import NO_FISICOS_PATTERN

    t = t or CROThresholds()
    empty = pd.DataFrame(columns=["SKU", "grupo", "motivo", "impacto"])
    if perf is None or perf.empty:
        return empty
    p = perf.copy()
    for c in ("vistas_ventana", "cart_rate", "buy_rate", "paid_views", "paid_buy_rate", "vistas_7d",
              "organic_sessions_7d", "organic_sessions_7d_prev"):
        if c not in p.columns:
            p[c] = float("nan") if c.endswith("rate") else 0.0
    p["vistas_ventana"] = p["vistas_ventana"].fillna(0)
    p["paid_views"] = p["paid_views"].fillna(0)
    p["NIVEL 1"] = p["NIVEL 1"].fillna("(sin categoría)")
    physical = ~p["NIVEL 1"].astype(str).str.lower().str.contains(NO_FISICOS_PATTERN, na=False)
    p = p[physical].merge(_category_rates(p[physical]), on="NIVEL 1", how="left")
    for c in ("cat_cart_rate", "cat_buy_rate", "cat_paid_buy_rate", "cat_buy_per_cart"):
        p[c] = p[c].fillna(p[c].median())
    has_rates = p["cart_rate"].notna().any()
    n = p["vistas_ventana"]
    p["tasa_carrito"] = _shrunk(p["cart_rate"], n, p["cat_cart_rate"], t.prior)
    p["tasa_compra"] = _shrunk(p["buy_rate"], n, p["cat_buy_rate"], t.prior)
    cart_users = (p["cart_rate"].fillna(0) * n)
    raw_bpc = (p["buy_rate"] / p["cart_rate"].where(p["cart_rate"] > 0)).clip(0, 1)
    p["compra_por_carrito"] = _shrunk(raw_bpc, cart_users, p["cat_buy_per_cart"], t.prior // 4)
    p["tasa_compra_pagado"] = _shrunk(p["paid_buy_rate"], p["paid_views"], p["cat_paid_buy_rate"], t.prior)
    visible = p["visible"].fillna(False).astype(bool)
    live = visible & p["con_stock"].fillna(False).astype(bool)

    out = []

    def add(mask, group, motivo, impacto):
        sel = p[mask].copy()
        if sel.empty:
            return
        sel["grupo"], sel["motivo"], sel["impacto"] = group, motivo(sel), impacto(sel)
        out.append(sel)

    pct = lambda x: (x * 100).round(1).astype(str) + "%"  # noqa: E731
    vv = lambda s: s["vistas_ventana"].astype(int).astype(str)  # noqa: E731
    if has_rates:
        add(live & (n >= t.min_views) & (p["tasa_carrito"] <= t.low_ratio * p["cat_cart_rate"]),
            "fuga_conversion",
            lambda s: "Agrega al carrito " + pct(s["tasa_carrito"]) + " de quienes lo ven vs " + pct(s["cat_cart_rate"])
                      + " en su categoría (" + vv(s) + " vistas)",
            lambda s: ((s["cat_cart_rate"] - s["tasa_carrito"]) * s["vistas_ventana"]).round(0))
        add(live & (cart_users >= t.min_carts) & (p["compra_por_carrito"] <= t.low_ratio * p["cat_buy_per_cart"]),
            "carrito_sin_compra",
            lambda s: "De quienes lo agregan, compra " + pct(s["compra_por_carrito"]) + " vs " + pct(s["cat_buy_per_cart"])
                      + " en su categoría (" + (s["cart_rate"].fillna(0) * s["vistas_ventana"]).round(0).astype(int).astype(str)
                      + " personas agregaron)",
            lambda s: ((s["cat_buy_per_cart"] - s["compra_por_carrito"]) * s["cart_rate"].fillna(0) * s["vistas_ventana"]).round(0))
        add(live & (p["paid_views"] >= t.min_paid_views) & (p["tasa_compra_pagado"] <= t.low_ratio * p["cat_paid_buy_rate"]),
            "pagado_sin_retorno",
            lambda s: s["paid_views"].astype(int).astype(str) + " vistas desde pauta, compra " + pct(s["tasa_compra_pagado"])
                      + " vs " + pct(s["cat_paid_buy_rate"]) + " del tráfico pagado de su categoría",
            lambda s: ((s["cat_paid_buy_rate"] - s["tasa_compra_pagado"]) * s["paid_views"]).round(0))
        add(live & (n >= t.gem_min_views) & (n < p["med_v"].fillna(0)) & (p["tasa_compra"] >= t.high_ratio * p["cat_buy_rate"]),
            "joya_oculta",
            lambda s: "Compra " + pct(s["tasa_compra"]) + " de quienes lo ven vs " + pct(s["cat_buy_rate"])
                      + " en su categoría, con solo " + vv(s) + " vistas",
            lambda s: ((s["tasa_compra"] - s["cat_buy_rate"]) * s["med_v"].fillna(0)).round(1))
    prev, now = p["organic_sessions_7d_prev"].fillna(0), p["organic_sessions_7d"].fillna(0)
    drop = (prev - now) / prev.where(prev > 0)
    add(visible & (prev >= t.organic_min_prev) & (drop * 100 >= t.organic_drop_pct),
        "organico_en_riesgo",
        lambda s: "Sesiones orgánicas " + s["organic_sessions_7d_prev"].astype(int).astype(str) + " → "
                  + s["organic_sessions_7d"].astype(int).astype(str) + " (7 días vs 7 anteriores)",
        lambda s: (s["organic_sessions_7d_prev"] - s["organic_sessions_7d"]).round(0))
    add(visible & ~p["con_stock"].fillna(True).astype(bool) & (p["vistas_7d"].fillna(0) >= t.no_stock_min_views),
        "trafico_sin_stock",
        lambda s: s["vistas_7d"].astype(int).astype(str) + " vistas en 7 días sin stock",
        lambda s: s["vistas_7d"].round(0))

    if not out:
        return empty
    res = pd.concat(out, ignore_index=True)
    keep = ["SKU", "grupo", "motivo", "impacto", "NIVEL 1", "vistas_ventana", "tasa_carrito", "cat_cart_rate",
            "tasa_compra", "cat_buy_rate", "compra_por_carrito", "cat_buy_per_cart", "paid_views", "tasa_compra_pagado",
            "cat_paid_buy_rate", "carrito_30d", "compras_30d", "ingresos_30d", "vistas_7d", "organic_sessions_7d",
            "organic_sessions_7d_prev", "content_score"]
    res = res[[c for c in keep if c in res.columns]]
    return res.sort_values(["grupo", "impacto"], ascending=[True, False]).reset_index(drop=True)


def window_frame(rates: pd.DataFrame | None, channel_rates: pd.DataFrame | None) -> pd.DataFrame:
    """Columns classify() needs from the two GA4 30-day window reports."""
    if rates is None or rates.empty:
        return pd.DataFrame(columns=["SKU", "vistas_ventana", "cart_rate", "buy_rate", "paid_views", "paid_buy_rate"])
    out = rates.rename(columns={"itemsViewed": "vistas_ventana", "cartToViewRate": "cart_rate",
                                "purchaseToViewRate": "buy_rate"})[["SKU", "vistas_ventana", "cart_rate", "buy_rate"]]
    if channel_rates is not None and not channel_rates.empty:
        paid = channel_rates[channel_rates["bucket"].isin(PAID)].groupby("SKU", as_index=False)[["views", "buyers"]].sum()
        paid["paid_buy_rate"] = (paid["buyers"] / paid["views"].where(paid["views"] > 0)).clip(0, 1)
        out = out.merge(paid.rename(columns={"views": "paid_views"})[["SKU", "paid_views", "paid_buy_rate"]],
                        on="SKU", how="left")
    else:
        out["paid_views"], out["paid_buy_rate"] = 0.0, float("nan")
    out["paid_views"] = out["paid_views"].fillna(0)
    return out


def content_vs_conversion(perf: pd.DataFrame, min_views: int = 50) -> pd.DataFrame:
    """Views-weighted user cart and purchase rates by content-score band (SKUs with enough views)."""
    cols = ["banda", "skus", "tasa_carrito", "tasa_compra"]
    if perf is None or perf.empty or "content_score" not in perf.columns or "cart_rate" not in perf.columns:
        return pd.DataFrame(columns=cols)
    p = perf[(perf["vistas_ventana"].fillna(0) >= min_views) & perf["content_score"].notna() & perf["cart_rate"].notna()].copy()
    p["banda"] = pd.cut(p["content_score"].astype(float), bins=[-1, 59, 79, 89, 100], labels=["< 60", "60–79", "80–89", "90–95"])
    rows = []
    for band, g in p.groupby("banda", observed=False):
        rows.append({"banda": str(band), "skus": int(len(g)),
                     "tasa_carrito": round(_wmean(g["cart_rate"], g["vistas_ventana"]), 4) if len(g) else None,
                     "tasa_compra": round(_wmean(g["buy_rate"], g["vistas_ventana"]), 4) if len(g) else None})
    return pd.DataFrame(rows, columns=cols)
