"""
Per-SKU facts accumulated across days, which no single export can tell:
when a SKU was last visible, and when it last left the STEP workflows.
"""

from __future__ import annotations

import pandas as pd

from .text import yesno

VISIBLE_COLS = ["SKU", "first_seen", "last_seen", "last_visible", "visible_streak", "days_visible"]
WORKFLOW_COLS = ["SKU", "salida_flujo", "fin_dat_log", "fin_edi_mer", "equipo"]


def _day_visible(snapshot: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({"SKU": snapshot["SKU"].astype(str).values,
                         "vis": (yesno(snapshot["VISIBLE"]) == 1).values}).drop_duplicates("SKU")


def update_visible_history(history: pd.DataFrame | None, day: str, snapshot: pd.DataFrame) -> pd.DataFrame:
    """
    Fold one day's snapshot (SKU, VISIBLE) into the history. Days must arrive in order;
    for an older day use `rebuild_visible_history`.
    """
    d = pd.Timestamp(day)
    today = _day_visible(snapshot)
    if history is None or history.empty:
        out = pd.DataFrame({
            "SKU": today["SKU"], "first_seen": d, "last_seen": d,
            "last_visible": today["vis"].map({True: d, False: pd.NaT}),
            "visible_streak": today["vis"].astype(int), "days_visible": today["vis"].astype(int),
        })
        return out[VISIBLE_COLS]
    if (history["last_seen"] >= d).any():
        raise ValueError(f"{day} is not after the history's last day; rebuild instead")
    m = history.merge(today, on="SKU", how="outer")
    seen_today = m["vis"].notna()
    vis = m["vis"].fillna(False).astype(bool)
    m["first_seen"] = m["first_seen"].fillna(d)
    m["last_seen"] = m["last_seen"].where(~seen_today, d)
    m["last_visible"] = m["last_visible"].where(~vis, d)
    m["visible_streak"] = (m["visible_streak"].fillna(0).astype(int) + 1).where(vis, 0)
    m["days_visible"] = m["days_visible"].fillna(0).astype(int) + vis.astype(int)
    return m[VISIBLE_COLS].reset_index(drop=True)


def rebuild_visible_history(days: list[tuple[str, pd.DataFrame]]) -> pd.DataFrame:
    history = None
    for day, snapshot in sorted(days, key=lambda t: t[0]):
        history = update_visible_history(history, day, snapshot)
    return history if history is not None else pd.DataFrame(columns=VISIBLE_COLS)


def update_workflow_dates(existing: pd.DataFrame | None, team: str, report: pd.DataFrame) -> pd.DataFrame:
    """Latest workflow exit / Dat_Log / Edi_Mer date per SKU across productivity reports."""
    cols = {"Fecha de Salida del Flujo de trabajo": "salida_flujo", "Fecha_Fin_Dat_Log": "fin_dat_log",
            "Fecha_Fin_Edi_Mer": "fin_edi_mer"}
    new = pd.DataFrame({"SKU": report["<ID>"].astype(str).str.strip().values})
    for src, dst in cols.items():
        new[dst] = pd.to_datetime(report[src], errors="coerce").values if src in report.columns else pd.NaT
    new["equipo"] = team
    new = new.sort_values("salida_flujo").drop_duplicates("SKU", keep="last")
    if existing is None or existing.empty:
        return new[WORKFLOW_COLS].reset_index(drop=True)
    both = pd.concat([existing[WORKFLOW_COLS], new[WORKFLOW_COLS]])
    agg = both.groupby("SKU", as_index=False).agg(
        salida_flujo=("salida_flujo", "max"), fin_dat_log=("fin_dat_log", "max"), fin_edi_mer=("fin_edi_mer", "max"))
    # Team of the most recent exit
    latest = both.sort_values("salida_flujo").drop_duplicates("SKU", keep="last")[["SKU", "equipo"]]
    return agg.merge(latest, on="SKU", how="left")[WORKFLOW_COLS].reset_index(drop=True)
