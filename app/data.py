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
    return store().load_productivity(day, team)


def productivity(day: str) -> ProductivityReport:
    return build_productivity(_productivity(day, "diseno"), _productivity(day, "edicion"))


@st.cache_data(ttl=TTL, show_spinner=False)
def history() -> pd.DataFrame:
    """Daily KPIs from every stored run, oldest first."""
    rows = []
    for day in sorted(index()["catalog"]):
        m = manifest(day)
        if m:
            rows.append({"Fecha": pd.Timestamp(day), **m["summary"],
                         **{f"inv:{k}": v for k, v in m["inventory_kpis"].items()}})
    return pd.DataFrame(rows)


def refresh() -> None:
    st.cache_data.clear()
