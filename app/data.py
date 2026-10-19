"""Cached reads of the processed results. The app never opens the raw Excel exports."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from catalog_core import ProductivityReport, build_productivity
from pipeline.config import Settings
from pipeline.storage import make_storage
from pipeline.store import ResultStore

TTL = 300  # results change at most every job run (30 min)


@st.cache_resource
def settings() -> Settings:
    return Settings.from_env()


@st.cache_resource
def store() -> ResultStore:
    s = settings()
    return ResultStore(make_storage(s), s.processed_dir)


@st.cache_data(ttl=TTL, show_spinner=False)
def index() -> dict:
    return store().load_index()


def run_days() -> list[str]:
    return sorted(index()["catalog"], reverse=True)


def productivity_days() -> list[str]:
    teams = index()["productivity"]
    return sorted({d for t in teams.values() for d in t}, reverse=True)


@st.cache_data(ttl=TTL, show_spinner=False)
def manifest(day: str) -> dict | None:
    return store().load_manifest(day)


@st.cache_data(ttl=TTL, show_spinner="Cargando tabla…")
def table(day: str, group: str, key: str) -> pd.DataFrame | None:
    return store().load_table(day, group, key)


@st.cache_data(ttl=TTL, show_spinner=False)
def sku_info(day: str) -> pd.DataFrame | None:
    return store().load_sku_info(day)


@st.cache_data(ttl=TTL, show_spinner=False)
def _productivity(day: str, team: str) -> pd.DataFrame | None:
    # The index is the source of truth: a day the job rejected has no entry
    if day not in index()["productivity"].get(team, {}):
        return None
    return store().load_productivity(day, team)


def productivity(day: str) -> ProductivityReport:
    return build_productivity(_productivity(day, "diseno"), _productivity(day, "edicion"),
                              _productivity(day, "ingresos"))


@st.cache_data(ttl=TTL, show_spinner=False)
def history() -> pd.DataFrame:
    """Daily KPIs from every stored run, oldest first."""
    rows = []
    for day in sorted(index()["catalog"]):
        m = manifest(day)
        if m:
            rows.append({"Fecha": pd.Timestamp(day), **m["summary"],
                         **{f"inv:{k}": v for k, v in m["inventory_kpis"].items()},
                         **{f"quality:{k}": v for k, v in (m.get("quality") or {}).items()}})
    return pd.DataFrame(rows)


# ── campaign lists ──────────────────────────────────────────────────────────
def lists() -> dict:
    """{list_id: index entry} for every ingested list, newest first."""
    entries = index()["lists"]
    return dict(sorted(entries.items(), key=lambda kv: kv[1].get("created_at") or "", reverse=True))


@st.cache_data(ttl=TTL, show_spinner=False)
def list_meta(list_id: str) -> dict | None:
    return store().load_list(list_id)


@st.cache_data(ttl=TTL, show_spinner="Cargando lista…")
def list_status(list_id: str, day: str) -> pd.DataFrame | None:
    return store().load_list_status(list_id, day)


def write_list(meta: dict) -> None:
    """Hand a list (new or updated) to the job via incoming/lists; the job owns processed/."""
    import json

    from pipeline.storage import _join

    s = settings()
    store().storage.write(_join(s.lists_dir, f"{meta['id']}.json"),
                          json.dumps(meta, ensure_ascii=False, indent=2).encode())


# ── performance (GA4 + Search Console) ─────────────────────────────────────
@st.cache_data(ttl=TTL, show_spinner="Cargando rendimiento…")
def perf() -> pd.DataFrame | None:
    return store().load_perf() if index().get("perf") else None


def perf_meta() -> dict:
    return index().get("perf") or {}


@st.cache_data(ttl=TTL, show_spinner="Cargando CRO…")
def cro() -> tuple[pd.DataFrame | None, dict]:
    return store().load_cro() if index().get("perf") else (None, {})


WATCHLIST_ID = "en-la-mira"


def add_to_watchlist(skus: list[str], user: str = "") -> int:
    """Append SKUs to the 'En la mira' campaign list (created on first use). Returns the new size."""
    from datetime import datetime, timezone

    meta = list_meta(WATCHLIST_ID) or {
        "id": WATCHLIST_ID, "name": "En la mira (CRO)", "owner": user, "status": "active",
        "notes": "SKUs fijados desde la página CRO", "skus": [],
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    merged = list(dict.fromkeys([*meta.get("skus", []), *[str(x) for x in skus]]))
    write_list({**meta, "skus": merged, "status": "active"})
    list_meta.clear()
    return len(merged)


@st.cache_data(ttl=TTL, show_spinner=False)
def visible_history() -> pd.DataFrame | None:
    return store().load_visible_history()


@st.cache_data(ttl=TTL, show_spinner=False)
def work_log() -> pd.DataFrame | None:
    """Who worked / promoted each SKU, from every stored Diseño and Edición report."""
    return store().load_work_log()


# ── agent ("Preguntar") ─────────────────────────────────────────────────────
def agent_budget():
    """Shared daily spend cap for the agent (AGENT_DAILY_BUDGET_USD, default US$5)."""
    import os

    from app.agent import DailyBudget

    limit = float(os.environ.get("AGENT_DAILY_BUDGET_USD", "5") or 5)
    return DailyBudget(limit, store().load_agent_usage, store().save_agent_usage)


# ── alerts ──────────────────────────────────────────────────────────────────
def alert_days() -> list[str]:
    return sorted(index()["alerts"].get("days", []), reverse=True)


@st.cache_data(ttl=TTL, show_spinner=False)
def alerts(day: str) -> list[dict]:
    return (store().load_alerts(day) or {}).get("alerts", [])


@st.cache_data(ttl=60, show_spinner=False)
def acks() -> dict:
    return store().load_acks()


def acknowledge(alert_id: str, user: str = "") -> None:
    current = store().load_acks()
    current[alert_id] = {"at": pd.Timestamp.utcnow().isoformat(timespec="seconds"), "by": user}
    store().save_acks(current)
    acks.clear()


def unacknowledge(alert_id: str) -> None:
    current = store().load_acks()
    current.pop(alert_id, None)
    store().save_acks(current)
    acks.clear()


def open_alerts(day: str) -> list[dict]:
    """Alerts of the day not yet marked as reviewed."""
    done = acks()
    return [a for a in alerts(day) if a["id"] not in done]


def refresh() -> None:
    st.cache_data.clear()
