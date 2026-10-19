"""
Listas de campaña: SKUs the business will push (promotions, seasons) checked every day
for readiness until the list is archived. Launch dates are secret, so there is no
countdown; the list is a watchlist.
"""

from __future__ import annotations

import re
import unicodedata

import pandas as pd

from .inventory import TIPO_C
from .quality import PLACEHOLDER_STOCK
from .text import col, lower

MIN_SCORE = 80
SKU_COLUMN_HINTS = ("sku", "codigo", "código", "<id>", "id", "item", "articulo", "artículo", "producto")

CHECKS = {
    "en_catalogo": "En el catálogo",
    "visible": "Visible",
    "habilitado": "Habilitado",
    "contenido": f"Contenido mínimo (score ≥ {MIN_SCORE}, imagen, precio, nombre, descripción)",
    "categoria_real": "Categoría real (no Tipo C)",
    "stock": "Con stock",
}


def _norm(text: str) -> str:
    text = unicodedata.normalize("NFKD", str(text))
    return "".join(c for c in text if not unicodedata.combining(c)).strip().lower()


def parse_sku_list(df: pd.DataFrame) -> tuple[list[str], str]:
    """
    Pick the SKU column of an uploaded workbook: by a familiar header first, else the
    column whose values look most like SKUs (digits). Returns unique SKUs in file order.
    """
    if df.empty:
        raise ValueError("El archivo no tiene filas")
    by_name = [c for c in df.columns if _norm(c) in SKU_COLUMN_HINTS]
    if by_name:
        column = by_name[0]
    else:
        scores = {c: df[c].astype(str).str.strip().str.fullmatch(r"\d{4,}").fillna(False).mean() for c in df.columns}
        column, best = max(scores.items(), key=lambda kv: kv[1])
        if best < 0.5:
            raise ValueError("No se encontró una columna de SKU (encabezado SKU/Código/ID o valores numéricos)")
    return clean_skus(df[column].dropna().astype(str).tolist()), str(column)


# Bump when readiness changes for existing lists, so the job re-evaluates the latest day
READINESS_VERSION = 2


def clean_skus(values: list) -> list[str]:
    """
    Strip, drop Excel's ".0", blanks and repeats (file order kept). When most values are
    numeric codes, values without a single digit are labels that slipped into the column
    (a repeated header "SKU", "NUEVO", "NEW") and are dropped.
    """
    out = [str(v).strip() for v in values if v is not None and not (isinstance(v, float) and v != v)]
    out = [v[:-2] if v.endswith(".0") and v[:-2].isdigit() else v for v in out]   # "830548.0"
    out = [v for v in out if v and v.lower() not in ("nan", "none")]
    if out and sum(v.isdigit() for v in out) / len(out) >= 0.5:
        out = [v for v in out if any(ch.isdigit() for ch in v)]
    return list(dict.fromkeys(out))


def readiness(
    skus: list[str],
    today_raw: pd.DataFrame,
    flags: pd.DataFrame,
    visible_history: pd.DataFrame | None = None,
    workflow: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    One row per SKU of the list with a boolean per check plus `listo`.

    Stock from the export is sometimes a placeholder (STOCK >= 1,000,000); then
    `stock_confiable` is False and stock does not block `listo`, so a data problem
    never hides a content problem.
    """
    base = pd.DataFrame({"SKU": skus})
    cat = pd.DataFrame({
        "SKU": today_raw["SKU"].values,
        "visible": (flags["is_visible"] == 1).values,
        "habilitado": (flags["is_enabled"] == 1).values,
        "contenido": ((flags["content_score"] >= MIN_SCORE) & (flags["has_image"] == 1) & (flags["has_price"] == 1)
                      & (flags["has_name"] == 1) & (flags["has_desc"] == 1)).values,
        "categoria_real": (lower(col(today_raw, "NIVEL 1")) != TIPO_C).fillna(True).astype(bool).values,
        "stock": (flags["has_stock"] == 1).values,
        "stock_confiable": (pd.Series(True, index=today_raw.index) if "STOCK REAL" in today_raw.columns
                            else ~(pd.to_numeric(col(today_raw, "STOCK"), errors="coerce") >= PLACEHOLDER_STOCK)).values,
        "score": flags["content_score"].values,
        "falta_imagen": (flags["has_image"] == 0).values,
        "falta_precio": (flags["has_price"] == 0).values,
        "falta_descripcion": (flags["has_desc"] == 0).values,
    }).drop_duplicates("SKU")
    out = base.merge(cat, on="SKU", how="left")
    out["en_catalogo"] = out["visible"].notna()
    for c in ["visible", "habilitado", "contenido", "categoria_real", "stock", "falta_imagen",
              "falta_precio", "falta_descripcion"]:
        out[c] = out[c].fillna(False).astype(bool)
    out["stock_confiable"] = out["stock_confiable"].fillna(True).astype(bool)
    out["score"] = out["score"].astype("Int64")

    stock_ok = out["stock"] | ~out["stock_confiable"]
    out["listo"] = (out["en_catalogo"] & out["visible"] & out["habilitado"] & out["contenido"]
                    & out["categoria_real"] & stock_ok)

    if visible_history is not None and not visible_history.empty:
        hv = visible_history[["SKU", "last_visible", "first_seen", "visible_streak"]].rename(columns={
            "last_visible": "ultima_vez_visible", "first_seen": "primera_vez_visto", "visible_streak": "dias_visible_seguidos"})
        out = out.merge(hv, on="SKU", how="left")
    else:
        out["ultima_vez_visible"] = pd.NaT
        out["primera_vez_visto"] = pd.NaT
        out["dias_visible_seguidos"] = pd.NA
    if workflow is not None and not workflow.empty:
        wf = workflow[["SKU", "salida_flujo", "fin_dat_log", "equipo"]]
        out = out.merge(wf, on="SKU", how="left")
    else:
        out["salida_flujo"] = pd.NaT
        out["fin_dat_log"] = pd.NaT
        out["equipo"] = None
    return out


def readiness_kpis(status: pd.DataFrame) -> dict:
    n = len(status)
    kpis = {
        "total": n,
        "listos": int(status["listo"].sum()) if n else 0,
        "pct_listos": round(float(status["listo"].mean()) * 100, 1) if n else 0.0,
        "stock_no_confiable": int((~status["stock_confiable"]).sum()) if n else 0,
    }
    for key in CHECKS:
        kpis[f"faltan_{key}"] = int((~status[key]).sum()) if n else 0
    return kpis


# Why a pending SKU is pending, in three exclusive buckets. The export has no "published
# to VTEX" field, so this follows the data: a SKU is never visible without stock
# (2026-10-19: 142 of 42,395 visible SKUs had no real stock), so "not visible" on a SKU
# without stock is a consequence of inventory, not a STEP problem. The content score
# also loses its 10 visibility points then, which alone can push it under MIN_SCORE.
PENDING_REASONS = {
    "solo_inventario": "Solo falta inventario",
    "step": "Pendiente en STEP",
    "fuera_catalogo": "Aún no llega al export de STEP",
}
STEP_CHECKS = ["visible", "habilitado", "contenido", "categoria_real"]
VISIBLE_POINTS = 10   # SCORE_WEIGHTS["is_visible"]


def pending_breakdown(status: pd.DataFrame) -> pd.DataFrame:
    """
    The pending rows (listo = False) with:
      motivo_pendiente  solo_inventario | step | fuera_catalogo
      falta_en_step     STEP checks it fails on their own, e.g. "Habilitado, Contenido mínimo"
      tambien_sin_stock True when a STEP-pending SKU also lacks stock
    """
    pending = status[~status["listo"].astype(bool)].copy()
    if pending.empty:
        for c in ("motivo_pendiente", "falta_en_step", "tambien_sin_stock"):
            pending[c] = pd.Series(dtype=object if c != "tambien_sin_stock" else bool)
        return pending
    b = lambda c: pending[c].astype(bool) if c in pending.columns else pd.Series(False, index=pending.index)
    has_stock = b("stock") | ~pending.get("stock_confiable", pd.Series(True, index=pending.index)).astype(bool)
    visible = b("visible")
    # Content that only fails because the score lost the visibility points
    score = pd.to_numeric(pending["score"], errors="coerce").fillna(0) if "score" in pending.columns \
        else pd.Series(0, index=pending.index)
    content_ok = b("contenido") | (~visible & (score + VISIBLE_POINTS >= MIN_SCORE)
                                   & ~b("falta_imagen") & ~b("falta_precio") & ~b("falta_descripcion"))
    step_fail = pd.DataFrame({
        "visible": ~visible & has_stock,          # with stock and still hidden: a publishing problem
        "habilitado": ~b("habilitado"),
        "contenido": ~content_ok,
        "categoria_real": ~b("categoria_real"),
    })
    pending["motivo_pendiente"] = "solo_inventario"
    pending.loc[step_fail.any(axis=1), "motivo_pendiente"] = "step"
    pending.loc[~b("en_catalogo"), "motivo_pendiente"] = "fuera_catalogo"
    is_step = pending["motivo_pendiente"] == "step"
    pending["falta_en_step"] = [", ".join(CHECKS[c].split(" (")[0] for c in STEP_CHECKS if row[c]) if s else ""
                                for (_, row), s in zip(step_fail.iterrows(), is_step)]
    pending["tambien_sin_stock"] = is_step & ~has_stock
    for c in STEP_CHECKS:
        pending[f"_step_{c}"] = step_fail[c] & is_step
    return pending


def pending_kpis(status: pd.DataFrame) -> dict:
    p = pending_breakdown(status)
    counts = p["motivo_pendiente"].value_counts()
    out = {k: int(counts.get(k, 0)) for k in PENDING_REASONS}
    out["step_sin_stock"] = int(p["tambien_sin_stock"].sum())
    out["step_por_check"] = {c: int(p[f"_step_{c}"].sum()) if len(p) else 0 for c in STEP_CHECKS}
    return out


def regressions(today: pd.DataFrame, yesterday: pd.DataFrame | None) -> pd.DataFrame:
    """SKUs that were ready on the previous status day and are not today, with what broke."""
    if yesterday is None or yesterday.empty:
        return today.iloc[0:0]
    was_ready = set(yesterday.loc[yesterday["listo"], "SKU"])
    lost = today[today["SKU"].isin(was_ready) & ~today["listo"]].copy()
    lost["perdio"] = lost.apply(lambda r: ", ".join(CHECKS[k] for k in CHECKS if not r[k]), axis=1)
    return lost.reset_index(drop=True)


def list_slug(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", _norm(name)).strip("-")
    return slug[:60] or "lista"
