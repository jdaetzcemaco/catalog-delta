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
from catalog_core.quality import quality_checks

from .storage import FileInfo, Storage, _join

INDEX_VERSION = 1
# Columns the app needs to label SKUs in change tables (name, category, links, image)
SKU_INFO_COLS = ["SKU", "PRODUCT ID", "NOMBRE DE PRODUCTO", "MARCA", "AREA", "NIVEL 1", "NIVEL 2",
                 "VISIBLE", "HABILITADO/DESHABILITADO", "STOCK", "TIENE STOCK", "STOCK REAL", "PRECIO", "URL", "URL IMAGEN"]


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
        index.setdefault("lists", {})
        index.setdefault("sku_history", {})
        index.setdefault("alerts", {})
        index.setdefault("ga4", {})
        index.setdefault("gsc", {})
        index.setdefault("seo", {})
        return index

    def save_index(self, index: dict) -> None:
        self.storage.write(self._p("index.json"), json.dumps(index, indent=2, sort_keys=True).encode())

    @staticmethod
    def source_entry(f: FileInfo) -> dict:
        return {"source": f.name, "modified": f.modified.isoformat(), "size": f.size}

    # ── run lock ────────────────────────────────────────────────────────────
    def acquire_lock(self, owner: str, ttl_minutes: int = 20) -> bool:
        """
        Best-effort lease so a long backfill and the next scheduled run never
        process the same files. The holder renews it after every file / API day, so a
        lock older than ttl means its run died (e.g. killed for memory) and is ignored.
        """
        raw = self.storage.read(self._p("lock.json"))
        if raw:
            lock = json.loads(raw)
            age = datetime.now(timezone.utc) - datetime.fromisoformat(lock["since"])
            if lock.get("owner") != owner and age.total_seconds() < ttl_minutes * 60:
                return False
        self.storage.write(self._p("lock.json"), json.dumps({"owner": owner, "since": _utc_now()}).encode())
        return json.loads(self.storage.read(self._p("lock.json")) or b"{}").get("owner") == owner

    def release_lock(self, owner: str) -> None:
        raw = self.storage.read(self._p("lock.json"))
        if raw and json.loads(raw).get("owner") == owner:
            # Expired lock instead of a delete: the storage interface never deletes files
            self.storage.write(self._p("lock.json"), json.dumps(
                {"owner": None, "since": "1970-01-01T00:00:00+00:00"}).encode())

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
            "quality": quality_checks(run.today_raw),
            "tables": tables,
            "row_counts": counts,
        }
        self.storage.write(self._p("runs", day, "manifest.json"),
                           json.dumps(manifest, indent=2, ensure_ascii=False).encode())
        return manifest

    def save_manifest(self, day: str, manifest: dict) -> None:
        self.storage.write(self._p("runs", day, "manifest.json"),
                           json.dumps(manifest, indent=2, ensure_ascii=False).encode())

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

    # ── campaign lists ──────────────────────────────────────────────────────
    def save_list(self, meta: dict) -> None:
        self.storage.write(self._p("lists", meta["id"], "list.json"),
                           json.dumps(meta, indent=2, ensure_ascii=False).encode())

    def load_list(self, list_id: str) -> dict | None:
        raw = self.storage.read(self._p("lists", list_id, "list.json"))
        return json.loads(raw) if raw else None

    def save_list_status(self, list_id: str, day: str, df: pd.DataFrame) -> None:
        self.storage.write(self._p("lists", list_id, "status", f"{day}.parquet"), to_parquet_bytes(df))

    def load_list_status(self, list_id: str, day: str) -> pd.DataFrame | None:
        data = self.storage.read(self._p("lists", list_id, "status", f"{day}.parquet"))
        return None if data is None else from_parquet_bytes(data)

    # ── per-SKU history ─────────────────────────────────────────────────────
    def save_visible_history(self, df: pd.DataFrame) -> None:
        self.storage.write(self._p("sku_history", "visible.parquet"), to_parquet_bytes(df))

    def load_visible_history(self) -> pd.DataFrame | None:
        data = self.storage.read(self._p("sku_history", "visible.parquet"))
        return None if data is None else from_parquet_bytes(data)

    def save_workflow_dates(self, df: pd.DataFrame) -> None:
        self.storage.write(self._p("sku_history", "workflow.parquet"), to_parquet_bytes(df))

    def load_workflow_dates(self) -> pd.DataFrame | None:
        data = self.storage.read(self._p("sku_history", "workflow.parquet"))
        return None if data is None else from_parquet_bytes(data)

    def save_work_log(self, df: pd.DataFrame) -> None:
        self.storage.write(self._p("sku_history", "work_log.parquet"), to_parquet_bytes(df))

    def load_work_log(self) -> pd.DataFrame | None:
        data = self.storage.read(self._p("sku_history", "work_log.parquet"))
        return None if data is None else from_parquet_bytes(data)

    # ── alerts ──────────────────────────────────────────────────────────────
    def save_alerts(self, day: str, payload: dict) -> None:
        self.storage.write(self._p("alerts", f"{day}.json"), json.dumps(payload, indent=2, ensure_ascii=False).encode())

    def load_alerts(self, day: str) -> dict | None:
        raw = self.storage.read(self._p("alerts", f"{day}.json"))
        return json.loads(raw) if raw else None

    def load_acks(self) -> dict:
        raw = self.storage.read(self._p("alerts", "ack.json"))
        return json.loads(raw) if raw else {}

    def save_acks(self, acks: dict) -> None:
        # The only processed/ file the app writes: user actions, never touched by the job
        self.storage.write(self._p("alerts", "ack.json"), json.dumps(acks, indent=2, ensure_ascii=False).encode())

    # ── agent usage (written by the app: spend of the "Preguntar" page per day) ──
    def load_agent_usage(self, day: str) -> dict | None:
        raw = self.storage.read(self._p("agent", f"usage-{day}.json"))
        return json.loads(raw) if raw else None

    def save_agent_usage(self, day: str, usage: dict) -> None:
        self.storage.write(self._p("agent", f"usage-{day}.json"), json.dumps(usage, indent=2).encode())

    # ── GA4 ─────────────────────────────────────────────────────────────────
    def save_ga4_day(self, day: str, items: pd.DataFrame, landing: pd.DataFrame) -> None:
        self.storage.write(self._p("ga4", day, "items.parquet"), to_parquet_bytes(items))
        self.storage.write(self._p("ga4", day, "landing.parquet"), to_parquet_bytes(landing))

    def load_ga4_day(self, day: str, kind: str = "items") -> pd.DataFrame | None:
        data = self.storage.read(self._p("ga4", day, f"{kind}.parquet"))
        return None if data is None else from_parquet_bytes(data)

    def save_perf(self, df: pd.DataFrame) -> None:
        """Rolling per-SKU performance aggregates (7/30/90 days); the only GA4 file the app reads."""
        self.storage.write(self._p("perf", "sku_perf.parquet"), to_parquet_bytes(df))

    def load_perf(self) -> pd.DataFrame | None:
        data = self.storage.read(self._p("perf", "sku_perf.parquet"))
        return None if data is None else from_parquet_bytes(data)

    def save_ga4_channels(self, day: str, df: pd.DataFrame) -> None:
        self.storage.write(self._p("ga4", day, "channels.parquet"), to_parquet_bytes(df))

    def save_window(self, name: str, df: pd.DataFrame) -> None:
        self.storage.write(self._p("perf", f"window_{name}.parquet"), to_parquet_bytes(df))

    def load_window(self, name: str) -> pd.DataFrame | None:
        data = self.storage.read(self._p("perf", f"window_{name}.parquet"))
        return None if data is None else from_parquet_bytes(data)

    def save_cro(self, cro: pd.DataFrame, summary: dict) -> None:
        self.storage.write(self._p("perf", "cro.parquet"), to_parquet_bytes(cro))
        self.storage.write(self._p("perf", "cro_summary.json"), json.dumps(summary, indent=2, ensure_ascii=False,
                                                                          default=str).encode())

    def load_cro(self) -> tuple[pd.DataFrame | None, dict]:
        data = self.storage.read(self._p("perf", "cro.parquet"))
        raw = self.storage.read(self._p("perf", "cro_summary.json"))
        return (None if data is None else from_parquet_bytes(data)), (json.loads(raw) if raw else {})

    def save_daily(self, name: str, df: pd.DataFrame) -> None:
        """Per-day rows for the aggregation window (product, landing, gsc): one file per source."""
        self.storage.write(self._p("perf", f"daily_{name}.parquet"), to_parquet_bytes(df))

    def load_daily(self, name: str) -> pd.DataFrame | None:
        data = self.storage.read(self._p("perf", f"daily_{name}.parquet"))
        return None if data is None else from_parquet_bytes(data)

    def save_gsc_day(self, day: str, pages: pd.DataFrame, queries: pd.DataFrame | None = None) -> None:
        self.storage.write(self._p("gsc", day, "pages.parquet"), to_parquet_bytes(pages))
        if queries is not None:
            self.storage.write(self._p("gsc", day, "queries.parquet"), to_parquet_bytes(queries))

    def load_gsc_day(self, day: str) -> pd.DataFrame | None:
        data = self.storage.read(self._p("gsc", day, "pages.parquet"))
        return None if data is None else from_parquet_bytes(data)

    def load_gsc_queries(self, day: str) -> pd.DataFrame | None:
        data = self.storage.read(self._p("gsc", day, "queries.parquet"))
        return None if data is None else from_parquet_bytes(data)

    # ── SEO impact of the catalog work (cohorts of worked SKUs vs control) ──
    SEO_FILES = ("cohorts", "skus", "daily", "buckets")

    def save_seo(self, frames: dict[str, pd.DataFrame]) -> None:
        for name, df in frames.items():
            self.storage.write(self._p("perf", f"seo_{name}.parquet"), to_parquet_bytes(df))

    def load_seo(self, name: str) -> pd.DataFrame | None:
        data = self.storage.read(self._p("perf", f"seo_{name}.parquet"))
        return None if data is None else from_parquet_bytes(data)
