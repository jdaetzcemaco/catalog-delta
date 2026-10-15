"""
Deterministic alerts over results the job already computed. No rule reads the raw
export; every input is a manifest, a stored table or a list status, so alerts are
cheap, reproducible and auditable. Thresholds come from `AlertThresholds`.

Severities: "alta" (acts on sales today), "media" (needs attention this week),
"baja" (informative).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field

import pandas as pd

MAX_SKUS = 200  # SKUs stored per alert; the full table lives in its source page
# How a failed check reads in a sentence ("27 no visibles")
MISSING = {"en_catalogo": "fuera del catálogo", "visible": "no visibles", "habilitado": "deshabilitados",
           "contenido": "sin contenido mínimo", "categoria_real": "en Tipo C", "stock": "sin stock"}


@dataclass
class AlertThresholds:
    list_min_ready_pct: float = 80.0      # list below this % ready
    visible_drop_pct: float = 5.0         # visible SKUs fell more than this vs previous day
    score_drop_points: float = 1.0        # average content score fell more than this
    stock_no_visible_jump_pct: float = 25.0  # "stock sin visibilidad" grew more than this
    stock_placeholder_pct: float = 1.0    # share of SKUs with STOCK >= 1,000,000
    tipo_c_min_days: int = 14             # Tipo C with stock older than this (days in catalog)
    export_late_days: int = 1             # no catalog export for more than this many days
    new_visible_weak_min: int = 20        # new visible SKUs with score < 80 to alert


@dataclass
class Alert:
    id: str
    rule: str
    severity: str
    title: str
    detail: str
    day: str
    page: str                                   # app page that explains it
    count: int = 0
    skus: list[str] = field(default_factory=list)
    context: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["skus"] = d["skus"][:MAX_SKUS]
        return d


def _pct(part: float, whole: float) -> float:
    return round(part / whole * 100, 1) if whole else 0.0


# ── rules ───────────────────────────────────────────────────────────────────
def list_alerts(day: str, lists: dict[str, dict], statuses: dict[str, pd.DataFrame],
                previous: dict[str, pd.DataFrame | None], t: AlertThresholds) -> list[Alert]:
    """Regressions and low readiness per active campaign list."""
    from .campaigns import CHECKS, readiness_kpis, regressions

    out: list[Alert] = []
    for lid, status in statuses.items():
        name = lists.get(lid, {}).get("name") or lid
        lost = regressions(status, previous.get(lid))
        if len(lost):
            reasons = lost["perdio"].value_counts().head(3)
            out.append(Alert(
                id=f"list_regression:{lid}", rule="list_regression", severity="alta",
                title=f"{len(lost)} SKUs de «{name}» dejaron de estar listos",
                detail="; ".join(f"{n} perdieron {r}" for r, n in reasons.items()),
                day=day, page="campanas", count=len(lost), skus=lost["SKU"].tolist(), context={"list": lid}))
        k = readiness_kpis(status)
        if k["total"] and k["pct_listos"] < t.list_min_ready_pct:
            missing = {MISSING[c]: k[f"faltan_{c}"] for c in CHECKS if k[f"faltan_{c}"]}
            top = sorted(missing.items(), key=lambda kv: -kv[1])[:3]
            out.append(Alert(
                id=f"list_low_ready:{lid}", rule="list_low_ready", severity="media",
                title=f"«{name}» está al {k['pct_listos']}% de listos ({k['listos']}/{k['total']})",
                detail="Lo que más falta: " + ", ".join(f"{n} {lbl}" for lbl, n in top),
                day=day, page="campanas", count=k["total"] - k["listos"],
                skus=status.loc[~status["listo"], "SKU"].tolist(), context={"list": lid, "pct": k["pct_listos"]}))
    return out


def catalog_alerts(day: str, manifest: dict, previous: dict | None, t: AlertThresholds) -> list[Alert]:
    """Day-over-day movements in the catalog health KPIs and data-quality flags."""
    out: list[Alert] = []
    s, ps = manifest["summary"], (previous or {}).get("summary")
    q = manifest.get("quality") or {}
    if q.get("stock_placeholder_pct", 0) > t.stock_placeholder_pct:
        out.append(Alert(
            id="quality:stock_placeholder", rule="quality_stock_placeholder", severity="alta",
            title=f"Stock no confiable: {q['stock_placeholder']:,} SKUs ({q['stock_placeholder_pct']}%) con STOCK ≥ 1,000,000",
            detail="Todos los indicadores de stock están inflados hasta que STEP corrija el export.",
            day=day, page="inventario", count=int(q["stock_placeholder"]), context=q))
    if ps:
        drop = _pct(ps["Visible"] - s["Visible"], ps["Visible"])
        if drop > t.visible_drop_pct:
            out.append(Alert(
                id="catalog:visible_drop", rule="visible_drop", severity="alta",
                title=f"Los SKUs visibles cayeron {drop}% ({ps['Visible']:,} → {s['Visible']:,})",
                detail="Revisa «Ya no visibles» en Cambios del día.", day=day, page="cambios",
                count=int(ps["Visible"] - s["Visible"]), context={"before": ps["Visible"], "after": s["Visible"]}))
        delta_score = round(ps["Avg Content Score"] - s["Avg Content Score"], 2)
        if delta_score > t.score_drop_points:
            out.append(Alert(
                id="catalog:score_drop", rule="score_drop", severity="media",
                title=f"El score promedio bajó {delta_score} puntos ({ps['Avg Content Score']} → {s['Avg Content Score']})",
                detail="Suele venir de SKUs nuevos sin contenido o de imágenes perdidas.", day=day, page="cambios",
                context={"before": ps["Avg Content Score"], "after": s["Avg Content Score"]}))
        inv, pinv = manifest.get("inventory_kpis", {}), (previous or {}).get("inventory_kpis", {})
        before, after = pinv.get("stock_no_visible", 0), inv.get("stock_no_visible", 0)
        if before and _pct(after - before, before) > t.stock_no_visible_jump_pct and after - before >= 10:
            out.append(Alert(
                id="inventory:stock_no_visible_jump", rule="stock_no_visible_jump", severity="media",
                title=f"Stock sin visibilidad subió de {before:,} a {after:,} SKUs",
                detail="Inventario que no llega a la tienda en línea.", day=day, page="inventario",
                count=after - before, context={"before": before, "after": after}))
    return out


def new_visible_weak_alert(day: str, new_skus: pd.DataFrame | None, t: AlertThresholds) -> list[Alert]:
    """New SKUs published visible with weak content."""
    if new_skus is None or new_skus.empty or "content_score" not in new_skus.columns:
        return []
    weak = new_skus[(new_skus["is_visible"].astype(int) == 1) & (new_skus["content_score"] < 80)]
    if len(weak) < t.new_visible_weak_min:
        return []
    return [Alert(
        id="catalog:new_visible_weak", rule="new_visible_weak", severity="media",
        title=f"{len(weak)} SKUs nuevos salieron visibles con score < 80",
        detail="Están en línea con contenido incompleto (imagen, descripción o precio).",
        day=day, page="cambios", count=len(weak), skus=weak["SKU"].astype(str).tolist())]


def tipo_c_stuck_alert(day: str, tipo_c: pd.DataFrame | None, visible_history: pd.DataFrame | None,
                       t: AlertThresholds) -> list[Alert]:
    """Tipo C SKUs with stock that have been in the catalog for a while without a real category."""
    if tipo_c is None or tipo_c.empty or visible_history is None or visible_history.empty:
        return []
    age = tipo_c[["SKU"]].merge(visible_history[["SKU", "first_seen"]], on="SKU", how="left")
    days_in = (pd.Timestamp(day) - pd.to_datetime(age["first_seen"])).dt.days
    stuck = age[days_in >= t.tipo_c_min_days]
    if stuck.empty:
        return []
    return [Alert(
        id="inventory:tipo_c_stuck", rule="tipo_c_stuck", severity="media",
        title=f"{len(stuck)} SKUs Tipo C con inventario llevan ≥ {t.tipo_c_min_days} días sin categoría",
        detail="Disponibles en tienda pero no en línea; necesitan foto y clasificación.",
        day=day, page="inventario", count=len(stuck), skus=stuck["SKU"].astype(str).tolist())]


def export_late_alert(today: str, latest_day: str | None, t: AlertThresholds) -> list[Alert]:
    """The daily STEP export has not arrived."""
    if latest_day is None:
        return []
    late = (pd.Timestamp(today) - pd.Timestamp(latest_day)).days
    if late <= t.export_late_days:
        return []
    return [Alert(
        id="data:export_late", rule="export_late", severity="media",
        title=f"No ha llegado el export del catálogo: el último es del {latest_day} ({late} días)",
        detail="Revisa el correo de STEP y la carpeta incoming de OneDrive.", day=today, page="cargar",
        count=late, context={"latest_day": latest_day})]


# ── assembly ────────────────────────────────────────────────────────────────
SEVERITY_ORDER = {"alta": 0, "media": 1, "baja": 2}


def finalize(alerts: list[Alert], previous_alerts: list[dict] | None, generated_at: str) -> list[dict]:
    """Serialize, sorted by severity, keeping `first_seen` from the previous day's alerts."""
    prev = {a["id"]: a for a in (previous_alerts or [])}
    out = []
    for a in sorted(alerts, key=lambda a: (SEVERITY_ORDER.get(a.severity, 9), a.title)):
        d = a.to_dict()
        d["first_seen"] = prev.get(a.id, {}).get("first_seen", a.day)
        d["generated_at"] = generated_at
        out.append(d)
    return out
