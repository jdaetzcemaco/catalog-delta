"""
Layout of the processed/ folder, shared by the job (writes) and the app (reads).

Everything is Parquet plus small JSON files, so the app can load one day's results
in about a second without touching the 100+ MB Excel exports.
"""

from __future__ import annotations

import io
import json
import re
from datetime import datetime, timezone

import pandas as pd

from catalog_core import CatalogRun

from .storage import FileInfo, Storage, _join

INDEX_VERSION = 1
# Columns the app needs to label SKUs in change tables (name, category, links, image)
SKU_INFO_COLS = ["SKU", "NOMBRE DE PRODUCTO", "MARCA", "AREA", "NIVEL 1", "NIVEL 2",
                 "VISIBLE", "HABILITADO/DESHABILITADO", "URL", "URL IMAGEN"]


def slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    out = df.copy()
    for c in out.columns:
        # Excel cells can mix numbers, dates and text in one column; store those as text
        if out[c].dtype == object and pd.api.types.infer_dtype(out[c], skipna=True) not in ("string", "empty"):
            out[c] = out[c].map(lambda v: v if pd.isna(v) else str(v)).astype("string")
    buf = io.BytesIO()
    out.to_parquet(buf, index=False)
    return buf.getvalue()


def from_parquet_bytes(data: bytes, columns: list[str] | None = None) -> pd.DataFrame:
    df = pd.read_parquet(io.BytesIO(data), columns=columns)
    # Back to plain object/NaN text like the loaders produce, so the rules behave the same
    for c in df.columns:
        if isinstance(df[c].dtype, pd.StringDtype):
            df[c] = df[c].astype(object).where(df[c].notna(), float("nan"))
    return df


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class ResultStore:
    def __init__(self, storage: Storage, processed_dir: str):
        self.storage = storage
        self.root = processed_dir

    def _p(self, *parts: str) -> str:
        return _join(self.root, *parts)

    # ── index ───────────────────────────────────────────────────────────────
    def load_index(self) -> dict:
        raw = self.storage.read(self._p("index.json"))
        index = json.loads(raw) if raw else {}
        index.setdefault("version", INDEX_VERSION)
        index.setdefault("catalog", {})
        index.setdefault("productivity", {})
        return index

    def save_index(self, index: dict) -> None:
        self.storage.write(self._p("index.json"), json.dumps(index, indent=2, sort_keys=True).encode())

    @staticmethod
    def source_entry(f: FileInfo) -> dict:
        return {"source": f.name, "modified": f.modified.isoformat(), "size": f.size}

    # ── snapshots ───────────────────────────────────────────────────────────
    def save_snapshot(self, day: str, df: pd.DataFrame) -> None:
        self.storage.write(self._p("snapshots", f"{day}.parquet"), to_parquet_bytes(df))

    def load_snapshot(self, day: str, columns: list[str] | None = None) -> pd.DataFrame | None:
        data = self.storage.read(self._p("snapshots", f"{day}.parquet"))
        return None if data is None else from_parquet_bytes(data, columns)

    # ── runs ────────────────────────────────────────────────────────────────
    def save_run(self, day: str, run: CatalogRun, source: dict, previous_day: str | None) -> dict:
        tables: dict[str, dict[str, str]] = {"changes": {}, "inventory": {}}
        counts: dict[str, int] = {}

        def put(group: str, key: str, df: pd.DataFrame) -> None:
            fname = f"{group}__{slug(key)}.parquet"
            self.storage.write(self._p("runs", day, fname), to_parquet_bytes(df))
            tables[group][key] = fname
            counts[f"{group}:{key}"] = len(df)

        for name, df in run.changes.items():
            put("changes", name, df)
        for key, df in run.inventory.sections.items():
            put("inventory", key, df)
        info = run.today_raw[[c for c in SKU_INFO_COLS if c in run.today_raw.columns]]
        self.storage.write(self._p("runs", day, "sku_info.parquet"), to_parquet_bytes(info))

        manifest = {
            "day": day,
            "previous_day": previous_day,
            "baseline": previous_day is None,
            "source": source,
            "processed_at": _utc_now(),
            "summary": {k: (v.item() if hasattr(v, "item") else v)
                        for k, v in run.summary.to_dict("records")[0].items()},
            "sku_changes": None if run.sku_changes is None else {
                "new": len(run.sku_changes["new"]),
                "removed": len(run.sku_changes["removed"]),
                "net": int(run.sku_changes["net"]),
            },
            "inventory_kpis": run.inventory.kpis,
            "tables": tables,
            "row_counts": counts,
        }
        self.storage.write(self._p("runs", day, "manifest.json"),
                           json.dumps(manifest, indent=2, ensure_ascii=False).encode())
        return manifest

    def load_manifest(self, day: str) -> dict | None:
        raw = self.storage.read(self._p("runs", day, "manifest.json"))
        return json.loads(raw) if raw else None

    def load_table(self, day: str, group: str, key: str) -> pd.DataFrame | None:
        manifest = self.load_manifest(day)
        fname = (manifest or {}).get("tables", {}).get(group, {}).get(key)
        if not fname:
            return None
        data = self.storage.read(self._p("runs", day, fname))
        return None if data is None else from_parquet_bytes(data)

    def load_sku_info(self, day: str) -> pd.DataFrame | None:
        data = self.storage.read(self._p("runs", day, "sku_info.parquet"))
        return None if data is None else from_parquet_bytes(data)

    def run_days(self) -> list[str]:
        return sorted(self.load_index()["catalog"])

    # ── productivity ────────────────────────────────────────────────────────
    def save_productivity(self, day: str, team: str, df: pd.DataFrame) -> None:
        self.storage.write(self._p("productivity", day, f"{team}.parquet"), to_parquet_bytes(df))

    def load_productivity(self, day: str, team: str) -> pd.DataFrame | None:
        data = self.storage.read(self._p("productivity", day, f"{team}.parquet"))
        return None if data is None else from_parquet_bytes(data)
