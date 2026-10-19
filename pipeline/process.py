"""
One pass of the job: process whatever is new in incoming/, then exit.

Safe to run every few minutes: files already processed (same name, size and
modified time) are skipped, so exports can arrive at any hour.
"""

from __future__ import annotations

import gc
import logging
import os
import re
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Protocol

import pandas as pd

from catalog_core import load_catalog, load_productivity, run_catalog
from catalog_core.loaders import attach_inventory, load_inventory_sheet
from catalog_core.alerts import (AlertThresholds, catalog_alerts, export_late_alert, finalize, list_alerts,
                                 new_visible_weak_alert, organic_drop_alerts, tipo_c_stuck_alert, traffic_alerts)
from catalog_core.campaigns import READINESS_VERSION, clean_skus, readiness
from catalog_core.productivity import detect_report
from catalog_core.sku_history import (rebuild_visible_history, update_visible_history, update_work_log,
                                     update_workflow_dates, work_log_rows)

from .config import Settings
from .storage import FileInfo, Storage
from .store import ResultStore

log = logging.getLogger(__name__)

try:
    from zoneinfo import ZoneInfo
    LOCAL_TZ = ZoneInfo("America/Guatemala")
except Exception:  # pragma: no cover - tzdata missing
    LOCAL_TZ = None

DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")
TEAMS = ("diseno", "edicion", "ingresos")


class History(Protocol):
    def upsert(self, day: str, summary: pd.DataFrame) -> list: ...


@dataclass
class PassReport:
    catalogs: list[str] = field(default_factory=list)
    recomputed: list[str] = field(default_factory=list)
    productivity: list[str] = field(default_factory=list)
    mail: list[str] = field(default_factory=list)
    upgraded: list[str] = field(default_factory=list)
    lists: list[str] = field(default_factory=list)
    alerts: int | None = None
    ga4: list[str] = field(default_factory=list)
    gsc: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    skipped: bool = False

    @property
    def did_work(self) -> bool:
        return bool(self.catalogs or self.recomputed or self.productivity or self.mail or self.lists)


def file_day(f: FileInfo) -> str:
    """Business date of an export: from its name, else its modified date in Guatemala."""
    m = DATE_RE.search(f.name)
    if m:
        return m.group(1)
    modified = f.modified.astimezone(LOCAL_TZ) if LOCAL_TZ else f.modified
    return modified.strftime("%Y-%m-%d")


def latest_per_day(files: list[FileInfo]) -> dict[str, FileInfo]:
    """If a day was exported more than once, the most recently modified file wins."""
    out: dict[str, FileInfo] = {}
    for f in files:
        day = file_day(f)
        if day not in out or f.modified > out[day].modified:
            out[day] = f
    return dict(sorted(out.items()))


def is_new(entry: dict | None, f: FileInfo) -> bool:
    return entry is None or entry.get("source") != f.name or entry.get("modified") != f.modified.isoformat() \
        or entry.get("size") != f.size


class Job:
    def __init__(self, settings: Settings, storage: Storage, history: History | None = None,
                 mailbox=None, ga4=None, gsc=None):
        self.settings = settings
        self.storage = storage
        self.store = ResultStore(storage, settings.processed_dir)
        self.history = history
        self.mailbox = mailbox
        self.ga4 = ga4
        self.gsc = gsc
        self._perf_dirty = False
        self.owner: str | None = None
        self._last: tuple[str, pd.DataFrame] | None = None

    # ── catalog ─────────────────────────────────────────────────────────────
    def _previous_day(self, index: dict, day: str) -> str | None:
        earlier = [d for d in index["catalog"] if d < day]
        return max(earlier) if earlier else None

    def _next_day(self, index: dict, day: str) -> str | None:
        later = [d for d in index["catalog"] if d > day]
        return min(later) if later else None

    def _compute(self, index: dict, day: str, today_raw: pd.DataFrame, source: dict) -> None:
        prev = self._previous_day(index, day)
        if prev and self._last and self._last[0] == prev:
            # Backfills go day by day: the previous day is the one just computed
            yesterday_raw = self._last[1]
        else:
            yesterday_raw = self.store.load_snapshot(prev) if prev else None
        log.info("%s: %s SKUs, compared with %s", day, f"{len(today_raw):,}", prev or "nothing (first day)")
        run = run_catalog(today_raw, yesterday_raw)
        del yesterday_raw
        self._last = None  # drop the old reference before holding today's
        self.store.save_snapshot(day, today_raw)
        self.store.save_run(day, run, source, prev)
        if self.history is not None:
            self.history.upsert(day, run.summary)
        self._last = (day, today_raw)
        index["catalog"][day] = {**source, "previous_day": prev,
                                 "processed_at": datetime.now().astimezone().isoformat(timespec="seconds")}
        self.store.save_index(index)

    def process_catalog(self, index: dict, day: str, f: FileInfo) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            local = os.path.join(tmp, f.name)
            self.storage.download_to(f.path, local)
            today_raw = load_catalog(local, name=f.name)
            inventory = load_inventory_sheet(local, name=f.name)
        if inventory is not None:
            today_raw = attach_inventory(today_raw, inventory)
            log.info("%s: stock from Inventarios sheet (%s SKUs with real stock)", day,
                     f"{int((today_raw['STOCK REAL'] > 0).sum()):,}")
        self._compute(index, day, today_raw, ResultStore.source_entry(f))

    def recompute_from_snapshot(self, index: dict, day: str) -> None:
        """Re-run a stored day, e.g. after an earlier day arrived late."""
        today_raw = self.store.load_snapshot(day)
        if today_raw is None:
            raise FileNotFoundError(f"No snapshot for {day}")
        entry = index["catalog"][day]
        source = {k: entry[k] for k in ("source", "modified", "size") if k in entry}
        self._compute(index, day, today_raw, source)

    def select_catalogs(self, index: dict, files: dict[str, FileInfo], backfill: int | None) -> list:
        """
        Which catalog files this pass processes, oldest first.

        Only days from the oldest processed day onward are considered, so older exports
        sitting in incoming/ are never picked up by accident. Going further back is
        explicit: `backfill` = the newest N files; an empty store starts with the newest
        `bootstrap_files`.
        """
        processed = index["catalog"]
        if backfill:
            window = set(list(files)[-backfill:])
        elif not processed:
            window = set(list(files)[-self.settings.bootstrap_files:])
        else:
            window = set()
        oldest = min(processed) if processed else None
        todo = [(day, f) for day, f in files.items()
                if (day in window or (oldest is not None and day >= oldest)) and is_new(processed.get(day), f)]
        skipped = sum(1 for day in files if day not in window and (oldest is None or day < oldest))
        if skipped:
            log.info("%s older catalog files left alone (use --backfill N to process them)", skipped)
        return todo

    def run_catalogs(self, report: PassReport, backfill: int | None = None) -> None:
        s = self.settings
        index = self.store.load_index()
        files = latest_per_day(self.storage.list(s.incoming_dir, s.catalog_pattern))
        todo = self.select_catalogs(index, files, backfill)
        if todo:
            log.info("Processing %s catalog files: %s … %s", len(todo), todo[0][0], todo[-1][0])
        else:
            log.info("No new catalog files")
        todo_days = {d for d, _ in todo}
        for day, f in todo:
            try:
                self.process_catalog(index, day, f)
                report.catalogs.append(day)
            except Exception as exc:
                log.exception("Catalog %s (%s) failed", day, f.name)
                report.errors.append(f"catalog {f.name}: {exc}")
            gc.collect()
            self._renew_lock()

        # A day that arrived late changes the baseline of the stored day right after it
        followers = sorted({n for d in report.catalogs for n in [self._next_day(index, d)] if n and n not in todo_days})
        for day in followers:
            try:
                self.recompute_from_snapshot(index, day)
                report.recomputed.append(day)
            except Exception as exc:
                log.exception("Recompute %s failed", day)
                report.errors.append(f"recompute {day}: {exc}")
            self._renew_lock()

    def _renew_lock(self) -> None:
        if self.owner:
            self.store.acquire_lock(self.owner)  # keep the lease alive during a backfill

    # ── productivity ────────────────────────────────────────────────────────
    def run_productivity(self, report: PassReport) -> None:
        s = self.settings
        index = self.store.load_index()
        patterns = {"diseno": s.diseno_pattern, "edicion": s.edicion_pattern, "ingresos": s.ingresos_pattern}
        for team in TEAMS:
            seen = index["productivity"].setdefault(team, {})
            for day, f in latest_per_day(self.storage.list(s.incoming_dir, patterns[team])).items():
                if not is_new(seen.get(day), f):
                    continue
                rejected = index.setdefault("productivity_rejected", {})
                if rejected.get(f.name) == f.modified.isoformat():
                    continue
                try:
                    df = load_productivity(self.storage.read(f.path))
                    detected = detect_report(df.columns)
                    if detected and detected != team:
                        # Wrong name for its content (e.g. an older mis-labelled copy): never
                        # show it as this team; drop what was stored for that day
                        msg = f"{f.name} is a {detected} report by its columns; ignored as {team}"
                        log.warning("Productivity: %s", msg)
                        report.warnings.append(msg)
                        rejected[f.name] = f.modified.isoformat()
                        seen.pop(day, None)
                        self.store.save_index(index)
                        continue
                    self.store.save_productivity(day, team, df)
                    seen[day] = {**ResultStore.source_entry(f), "verified": detected == team}
                    self.store.save_index(index)
                    report.productivity.append(f"{team} {day}")
                    log.info("Productivity %s %s: %s rows", team, day, len(df))
                except Exception as exc:
                    log.exception("Productivity %s (%s) failed", team, f.name)
                    report.errors.append(f"productivity {f.name}: {exc}")

    # ── upgrades of stored days ────────────────────────────────────────────
    def add_missing_quality(self, report: PassReport) -> None:
        """
        Days stored before the data-quality checks existed get them once, from the
        snapshot's STOCK column; no rule is recomputed.
        """
        from catalog_core.quality import quality_checks

        for day in sorted(self.store.load_index()["catalog"]):
            m = self.store.load_manifest(day)
            if m is None or "quality" in m:
                continue
            try:
                snap = self.store.load_snapshot(day, columns=["SKU", "STOCK"])
                m["quality"] = quality_checks(snap)
                self.store.save_manifest(day, m)
                report.upgraded.append(day)
                log.info("Quality checks added to %s: %s", day, m["quality"])
            except Exception as exc:
                log.exception("Quality checks for %s failed", day)
                report.errors.append(f"quality {day}: {exc}")
            self._renew_lock()

    STOCK_UPGRADES_PER_PASS = 2  # each re-process peaks ~2.5 GB on a 4 GB instance

    def upgrade_stock_source(self, report: PassReport) -> None:
        """
        Stored days that still use the placeholder STOCK (export since 2026-09-22) are
        re-processed from their export file so has_stock comes from the Inventarios sheet.
        Newest first, a few per pass; days whose file is gone from incoming/ are skipped.
        """
        index = self.store.load_index()
        files = {f.name: f for f in self.storage.list(self.settings.incoming_dir, self.settings.catalog_pattern)}
        done = 0
        for day in sorted(index["catalog"], reverse=True):
            if done >= self.STOCK_UPGRADES_PER_PASS:
                break
            m = self.store.load_manifest(day) or {}
            q = m.get("quality") or {}
            if q.get("stock_corrected") or q.get("stock_upgrade_attempted") or q.get("stock_placeholder_pct", 0) <= 1:
                continue
            f = files.get(index["catalog"][day].get("source", ""))
            if f is None:
                continue
            try:
                self._last = None
                self.process_catalog(index, day, f)
                done += 1
                m = self.store.load_manifest(day) or {}
                if (m.get("quality") or {}).get("stock_corrected"):
                    report.upgraded.append(f"stock {day}")
                    log.info("Stock source upgraded for %s", day)
                else:
                    # The export has no Inventarios sheet: remember so this is tried only once
                    m.setdefault("quality", {})["stock_upgrade_attempted"] = True
                    self.store.save_manifest(day, m)
                    log.warning("Stock upgrade %s: export has no Inventarios sheet", day)
            except Exception as exc:
                log.exception("Stock upgrade %s failed", day)
                report.errors.append(f"stock upgrade {day}: {exc}")
            gc.collect()
            self._renew_lock()
        if done:
            # The day after each upgraded day compares against it: recompute from snapshots
            index = self.store.load_index()
            for day in sorted({d for d in index["catalog"] if any(f"stock {p}" in report.upgraded
                                                                    for p in [index["catalog"][d].get("previous_day")])}):
                try:
                    self._last = None
                    self.recompute_from_snapshot(index, day)
                    report.recomputed.append(day)
                except Exception as exc:
                    report.errors.append(f"recompute {day}: {exc}")

    def audit_productivity(self, report: PassReport) -> None:
        """
        Check every stored productivity day once against its columns. Catches copies
        stored under the wrong team before files were classified by content, which the
        normal pass never re-reads because the file itself did not change.
        """
        index = self.store.load_index()
        rejected = index.setdefault("productivity_rejected", {})
        changed = False
        for team in TEAMS:
            entries = index["productivity"].get(team, {})
            for day in sorted(entries):
                entry = entries[day]
                if entry.get("verified"):
                    continue
                df = self.store.load_productivity(day, team)
                detected = detect_report(df.columns) if df is not None else None
                if detected and detected != team:
                    msg = f"stored {team} {day} ({entry.get('source')}) is a {detected} report; removed from {team}"
                    log.warning("Productivity: %s", msg)
                    report.warnings.append(msg)
                    rejected[entry.get("source", "")] = entry.get("modified", "")
                    del entries[day]
                else:
                    entry["verified"] = True
                changed = True
        if changed:
            self.store.save_index(index)

    # ── per-SKU history ─────────────────────────────────────────────────────
    def update_sku_history(self, report: PassReport) -> None:
        """
        Keep visible.parquet (last day visible, streaks) and workflow.parquet (last exit
        from the STEP workflows) current. Visibility folds in new days incrementally; a
        day that arrived out of order triggers a rebuild from every stored snapshot.
        """
        index = self.store.load_index()
        hist_meta = index["sku_history"]
        days = sorted(index["catalog"])
        if days:
            done = set(hist_meta.get("visible_days", []))
            pending = [d for d in days if d not in done]
            last_done = max(done) if done else None
            if pending:
                try:
                    if last_done and min(pending) < last_done:
                        log.info("Visible history: rebuilding from %s snapshots", len(days))
                        hist = rebuild_visible_history(
                            [(d, self.store.load_snapshot(d, columns=["SKU", "VISIBLE"])) for d in days])
                        done = set(days)
                    else:
                        hist = self.store.load_visible_history()
                        for d in pending:
                            hist = update_visible_history(hist, d, self.store.load_snapshot(d, columns=["SKU", "VISIBLE"]))
                            done.add(d)
                            self._renew_lock()
                    self.store.save_visible_history(hist)
                    hist_meta["visible_days"] = sorted(done)
                    self.store.save_index(index)
                    log.info("Visible history: %s SKUs, %s days", f"{len(hist):,}", len(done))
                except Exception as exc:
                    log.exception("Visible history failed")
                    report.errors.append(f"visible history: {exc}")

        done_wf = hist_meta.setdefault("workflow_files", [])
        wf = self.store.load_workflow_dates()
        changed = False
        for team in ("diseno", "edicion"):
            for day in sorted(index["productivity"].get(team, {})):
                key = f"{team}/{day}"
                if key in done_wf:
                    continue
                try:
                    df = self.store.load_productivity(day, team)
                    if df is not None and "<ID>" in df.columns:
                        wf = update_workflow_dates(wf, "Diseño" if team == "diseno" else "Edición", df)
                    done_wf.append(key)
                    changed = True
                except Exception as exc:
                    log.exception("Workflow dates %s failed", key)
                    report.errors.append(f"workflow dates {key}: {exc}")
        if changed and wf is not None:
            self.store.save_workflow_dates(wf)
            self.store.save_index(index)

        # Who worked each SKU: every Diseño/Edición report row, kept (small: ~50 rows a day)
        done_wl = hist_meta.setdefault("work_log_files", [])
        pending_wl = [(team, day) for team in ("diseno", "edicion") for day in sorted(index["productivity"].get(team, {}))
                      if f"{team}/{day}" not in done_wl]
        if pending_wl:
            try:
                log_df = self.store.load_work_log()
                for team, day in pending_wl:
                    df = self.store.load_productivity(day, team)
                    if df is not None:
                        log_df = update_work_log(log_df, work_log_rows("Diseño" if team == "diseno" else "Edición", day, df))
                    done_wl.append(f"{team}/{day}")
                if log_df is not None:
                    self.store.save_work_log(log_df)
                self.store.save_index(index)
                log.info("Work log: %s rows, %s report files", f"{0 if log_df is None else len(log_df):,}", len(done_wl))
            except Exception as exc:
                log.exception("Work log failed")
                report.errors.append(f"work log: {exc}")

    # ── campaign lists ──────────────────────────────────────────────────────
    def run_campaigns(self, report: PassReport) -> None:
        """
        Ingest list files the app wrote to lists_dir (JSON: id, name, skus, status…) and
        compute today's readiness for every active list on the latest catalog day.
        """
        import json

        s = self.settings
        index = self.store.load_index()
        lists = index["lists"]
        for f in self.storage.list(s.lists_dir, "*.json"):
            entry = lists.get(f.name[:-5])
            if entry and not is_new(entry.get("file"), f):
                continue
            try:
                meta = json.loads(self.storage.read(f.path))
                meta["id"] = f.name[:-5]
                self.store.save_list(meta)
                lists[meta["id"]] = {
                    **{k: meta.get(k) for k in ("name", "status", "owner", "created_at")},
                    "sku_count": len(meta.get("skus", [])),
                    "file": ResultStore.source_entry(f),
                    "status_days": (entry or {}).get("status_days", []),
                }
                self.store.save_index(index)
                report.lists.append(f"ingest {meta['id']}")
                log.info("List %s (%s): %s SKUs, %s", meta["id"], meta.get("name"), len(meta.get("skus", [])),
                         meta.get("status", "active"))
            except Exception as exc:
                log.exception("List file %s failed", f.name)
                report.errors.append(f"list {f.name}: {exc}")

        days = sorted(index["catalog"])
        if not days or not lists:
            return
        day = days[-1]
        active = [lid for lid, e in lists.items() if e.get("status", "active") == "active"]
        todo = [lid for lid in active if day not in lists[lid].get("status_days", [])
                or f"ingest {lid}" in report.lists
                or lists[lid].get("readiness_version", 1) < READINESS_VERSION]
        if not todo:
            return
        today_raw = self.store.load_snapshot(day)
        from catalog_core import build_flags
        flags = build_flags(today_raw)
        visible = self.store.load_visible_history()
        workflow = self.store.load_workflow_dates()
        for lid in todo:
            try:
                meta = self.store.load_list(lid) or {}
                status = readiness(clean_skus(meta.get("skus", [])), today_raw, flags, visible, workflow)
                self.store.save_list_status(lid, day, status)
                days_done = set(lists[lid].get("status_days", []))
                days_done.add(day)
                lists[lid]["status_days"] = sorted(days_done)
                lists[lid]["last_day"] = day
                lists[lid]["last_kpis"] = {"listos": int(status["listo"].sum()), "total": len(status)}
                lists[lid]["readiness_version"] = READINESS_VERSION
                self.store.save_index(index)
                report.lists.append(f"status {lid} {day}")
                log.info("List %s: %s/%s ready on %s", lid, int(status["listo"].sum()), len(status), day)
            except Exception as exc:
                log.exception("List %s status failed", lid)
                report.errors.append(f"list status {lid}: {exc}")
            self._renew_lock()

    # ── GA4 / Search Console ────────────────────────────────────────────────
    PERF_MAX_DAYS_PER_PASS = 30

    def _days_to_pull(self, stored: set[str], refresh_days: int, backfill_days: int, lag_days: int) -> list[str]:
        """Newest first: the refresh window always, then days never stored, capped per pass."""
        today = datetime.now(LOCAL_TZ).date() if LOCAL_TZ else datetime.now().date()
        latest = today - timedelta(days=lag_days)
        wanted = [(latest - timedelta(days=i)).isoformat() for i in range(backfill_days)]
        refresh = set(wanted[:refresh_days])
        return [d for d in wanted if d in refresh or d not in stored][:self.PERF_MAX_DAYS_PER_PASS]

    def run_ga4(self, report: PassReport) -> None:
        """Pull GA4 item + landing reports for missing/recent days (GA4 closes a day after 24–48 h)."""
        if self.ga4 is None:
            return
        from catalog_core.performance import daily_item_frame, daily_landing_frame

        from catalog_core.performance import daily_channel_frame

        s = self.settings
        index = self.store.load_index()
        meta = index["ga4"]
        stored = set(meta.get("days", []))
        stored_ch = set(meta.get("channel_days", []))
        todo = self._days_to_pull(stored, s.ga4_refresh_days, s.ga4_backfill_days, lag_days=1)
        todo_ch = self._days_to_pull(stored_ch, s.ga4_refresh_days, s.ga4_backfill_days, lag_days=1)
        days = sorted(set(todo) | set(todo_ch), reverse=True)[:self.PERF_MAX_DAYS_PER_PASS]
        new_items, new_landing, new_channels = {}, {}, {}
        for day in days:
            try:
                if day in todo:
                    items = self.ga4.items_report(day)
                    landing = self.ga4.landing_report(day)
                    self.store.save_ga4_day(day, items, landing)
                    new_items[day], new_landing[day] = items, landing
                    stored.add(day)
                    report.ga4.append(day)
                if day in todo_ch:
                    channels = self.ga4.channels_report(day)
                    self.store.save_ga4_channels(day, channels)
                    new_channels[day] = channels
                    stored_ch.add(day)
                    if day not in todo:
                        report.ga4.append(f"{day} (canales)")
                log.info("GA4 %s: %s", day, ", ".join(f"{k} {len(v[day]):,} rows" for k, v in
                                                     (("items", new_items), ("landing", new_landing),
                                                      ("channels", new_channels)) if day in v))
            except Exception as exc:
                log.exception("GA4 %s failed", day)
                report.errors.append(f"ga4 {day}: {exc}")
                break  # quota or auth problems: stop for this pass, try again next run
            self._renew_lock()
        if not (new_items or new_channels):
            return
        as_of = max(stored) if stored else days[0]
        if new_items:
            self._merge_daily("item", daily_item_frame(new_items), as_of)
            self._merge_daily("landing", daily_landing_frame(new_landing), as_of)
        if new_channels:
            self._merge_daily("channel", daily_channel_frame(new_channels), as_of)
        meta["days"], meta["channel_days"], meta["as_of"] = sorted(stored), sorted(stored_ch), as_of
        self.store.save_index(index)
        self._perf_dirty = True

    def run_gsc(self, report: PassReport) -> None:
        """Pull Search Console pages for missing/recent days (final data lags 2–3 days)."""
        if self.gsc is None:
            return
        from catalog_core.performance import daily_gsc_frame

        s = self.settings
        index = self.store.load_index()
        meta = index["gsc"]
        stored = set(meta.get("days", []))
        todo = self._days_to_pull(stored, s.ga4_refresh_days + 1, s.ga4_backfill_days, lag_days=3)
        new_pages = {}
        for day in todo:
            try:
                pages = self.gsc.pages_report(day)
                self.store.save_gsc_day(day, pages)
                new_pages[day] = pages
                stored.add(day)
                report.gsc.append(day)
                log.info("GSC %s: %s pages, %s product pages", day, len(pages), int(pages["SKU"].notna().sum()))
            except Exception as exc:
                log.exception("GSC %s failed", day)
                report.errors.append(f"gsc {day}: {exc}")
                break
            self._renew_lock()
        if not new_pages:
            return
        self._merge_daily("gsc", daily_gsc_frame(new_pages), todo[0])
        meta["days"], meta["as_of"] = sorted(stored), todo[0]
        self.store.save_index(index)
        self._perf_dirty = True

    def _cro_window(self, index: dict, as_of: str) -> pd.DataFrame | None:
        """
        GA4 user-based rates over the 30 days ending `as_of` (two reports, ~40 s), pulled
        once per as_of and stored; reused from storage when GA4 is not configured here.
        """
        from catalog_core.cro import window_frame

        meta = index.setdefault("ga4", {})
        if self.ga4 is not None and meta.get("window_as_of") != as_of:
            try:
                start = (pd.Timestamp(as_of) - pd.Timedelta(days=29)).date().isoformat()
                rates = self.ga4.window_rates(start, as_of)
                self._renew_lock()
                channel_rates = self.ga4.window_channel_rates(start, as_of)
                self._renew_lock()
                self.store.save_window("rates", rates)
                self.store.save_window("channel_rates", channel_rates)
                meta["window_as_of"] = as_of
                log.info("GA4 30-day window %s..%s: %s SKUs with rates", start, as_of, f"{len(rates):,}")
            except Exception as exc:
                log.exception("GA4 window rates failed")
                return None
        return window_frame(self.store.load_window("rates"), self.store.load_window("channel_rates"))

    def _merge_daily(self, name: str, fresh: pd.DataFrame, as_of: str) -> None:
        from catalog_core.performance import merge_days

        keep = self.settings.ga4_backfill_days + 30
        self.store.save_daily(name, merge_days(self.store.load_daily(name), fresh, keep, as_of))

    def refresh_performance(self, report: PassReport) -> None:
        """Rebuild the per-SKU performance file the app reads, after new GA4/GSC days."""
        index = self.store.load_index()
        days = sorted(index["catalog"])
        if not days or not (index["ga4"].get("as_of") or index["gsc"].get("as_of")):
            return
        stale = (index.get("perf") or {}).get("catalog_day") != days[-1] or "cro" not in (index.get("perf") or {})
        if not (getattr(self, "_perf_dirty", False) or stale):
            return
        from catalog_core import build_flags
        from catalog_core.cro import classify, content_vs_conversion, window_frame
        from catalog_core.performance import (GSC_METRICS, LANDING_METRICS, channel_aggregates, channel_totals,
                                              item_aggregates, rolling_aggregates, sku_performance)

        try:
            as_of = max(filter(None, [index["ga4"].get("as_of"), index["gsc"].get("as_of")]))
            item_daily = self.store.load_daily("item")
            item = item_aggregates(item_daily if item_daily is not None else pd.DataFrame(), as_of)
            landing_daily = self.store.load_daily("landing")
            landing = rolling_aggregates(landing_daily, "SKU", LANDING_METRICS, as_of, "organic_sessions") \
                if landing_daily is not None else None
            gsc_daily = self.store.load_daily("gsc")
            gsc = rolling_aggregates(gsc_daily, "SKU", GSC_METRICS, as_of, "clicks") if gsc_daily is not None else None
            if gsc is not None and gsc_daily is not None and not gsc_daily.empty:
                recent = gsc_daily[pd.to_datetime(gsc_daily["day"]) > pd.Timestamp(as_of) - pd.Timedelta(days=30)]
                pos = recent.assign(w=recent["position"] * recent["impressions"]).groupby("SKU").agg(
                    w=("w", "sum"), i=("impressions", "sum"))
                gsc = gsc.merge((pos["w"] / pos["i"].where(pos["i"] > 0)).round(1).rename("posicion_30d").reset_index(),
                                on="SKU", how="left")
            channel_daily = self.store.load_daily("channel")
            channels = channel_aggregates(channel_daily, as_of) if channel_daily is not None else None
            # Catalog context for the latest day (category, visibility, real stock, content score, price)
            snap = self.store.load_snapshot(days[-1])
            flags = build_flags(snap)
            ctx = pd.DataFrame({"SKU": snap["SKU"].values, "PRODUCT ID": snap.get("PRODUCT ID", pd.Series(index=snap.index)).values,
                                "NIVEL 1": snap.get("NIVEL 1", pd.Series(index=snap.index)).values,
                                "visible": (flags["is_visible"] == 1).values, "con_stock": (flags["has_stock"] == 1).values,
                                "content_score": flags["content_score"].values,
                                "precio": pd.to_numeric(snap.get("PRECIO", pd.Series(index=snap.index)), errors="coerce").values})
            del snap, flags
            perf = sku_performance(ctx[["SKU", "PRODUCT ID"]], item, landing, gsc)
            if channels is not None and not channels.empty:
                perf = perf.merge(channels, on="SKU", how="left")
                ch_cols = [c for c in channels.columns if c != "SKU"]
                perf[ch_cols] = perf[ch_cols].fillna(0)
            perf = perf.merge(ctx.drop(columns=["PRODUCT ID"]), on="SKU", how="left")
            window = self._cro_window(index, as_of)
            if window is not None and not window.empty:
                perf = perf.merge(window, on="SKU", how="left")
            self.store.save_perf(perf)
            cro = classify(perf)
            totals = channel_totals(channel_daily, as_of) if channel_daily is not None else pd.DataFrame()
            summary = {"as_of": as_of, "catalog_day": days[-1],
                       "groups": cro["grupo"].value_counts().to_dict() if not cro.empty else {},
                       "content_vs_conversion": content_vs_conversion(perf).to_dict("records"),
                       "channel_totals": totals.to_dict("records")}
            self.store.save_cro(cro, summary)
            index["perf"] = {"as_of": as_of, "catalog_day": days[-1], "skus": int(len(perf)),
                             "skus_with_views_30d": int((item["vistas_30d"] > 0).sum()) if not item.empty else 0,
                             "cro": summary["groups"], "has_channels": channels is not None and not channels.empty}
            self.store.save_index(index)
            self._perf_dirty = False
            log.info("Performance as of %s: %s SKUs, %s with views in 30d; CRO %s", as_of, f"{len(perf):,}",
                     index["perf"]["skus_with_views_30d"], summary["groups"])
        except Exception as exc:
            log.exception("Performance aggregates failed")
            report.errors.append(f"performance: {exc}")

    # ── alerts ──────────────────────────────────────────────────────────────
    def run_alerts(self, report: PassReport) -> None:
        """
        Recompute the alerts for the latest catalog day from stored results. Cheap, so it
        runs every pass: the "export late" rule depends on the clock, not on new files.
        """
        index = self.store.load_index()
        days = sorted(index["catalog"])
        if not days:
            return
        day = days[-1]
        today = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d") if LOCAL_TZ else datetime.now().strftime("%Y-%m-%d")
        t = AlertThresholds(**{k: v for k, v in self.settings.alert_thresholds.items()
                               if k in AlertThresholds.__dataclass_fields__})
        try:
            manifest = self.store.load_manifest(day) or {}
            prev_day = manifest.get("previous_day")
            previous = self.store.load_manifest(prev_day) if prev_day else None
            alerts = catalog_alerts(day, manifest, previous, t)
            alerts += new_visible_weak_alert(day, self.store.load_table(day, "changes", "New SKUs"), t)
            alerts += tipo_c_stuck_alert(day, self.store.load_table(day, "inventory", "tipo_c_stock"),
                                         self.store.load_visible_history(), t)
            alerts += export_late_alert(today, day, t)

            statuses, prev_statuses = {}, {}
            for lid, e in index["lists"].items():
                if e.get("status", "active") != "active" or day not in e.get("status_days", []):
                    continue
                statuses[lid] = self.store.load_list_status(lid, day)
                earlier = [d for d in e["status_days"] if d < day]
                prev_statuses[lid] = self.store.load_list_status(lid, max(earlier)) if earlier else None
            alerts += list_alerts(day, index["lists"], statuses, prev_statuses, t)
            perf = self.store.load_perf() if index.get("perf") else None
            if perf is not None:
                alerts += traffic_alerts(day, perf, self.store.load_sku_info(day), t)
                alerts += organic_drop_alerts(day, index["lists"], statuses, perf, t)

            previous_alerts = (self.store.load_alerts(prev_day) or {}).get("alerts") if prev_day else None
            payload = {"day": day, "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                       "alerts": finalize(alerts, previous_alerts, datetime.now(timezone.utc).isoformat(timespec="seconds"))}
            self.store.save_alerts(day, payload)
            by_sev = {}
            for a in payload["alerts"]:
                by_sev[a["severity"]] = by_sev.get(a["severity"], 0) + 1
            index["alerts"] = {"last_day": day, "counts": by_sev,
                               "days": sorted(set(index["alerts"].get("days", [])) | {day})}
            self.store.save_index(index)
            report.alerts = len(payload["alerts"])
            log.info("Alerts for %s: %s %s", day, len(payload["alerts"]), by_sev)
        except Exception as exc:
            log.exception("Alerts failed")
            report.errors.append(f"alerts: {exc}")

    # ── e-mail reports ──────────────────────────────────────────────────────
    def run_mail(self, report: PassReport) -> None:
        """Save new STEP report attachments to incoming/ (before productivity runs)."""
        if self.mailbox is None:
            return
        from .mail import MAIL_VERSION, MailPermissionError, collect_reports

        s = self.settings
        index = self.store.load_index()
        if index.get("mail_version") != MAIL_VERSION:
            index["mail"] = {}
            index["mail_version"] = MAIL_VERSION
        seen = index.setdefault("mail", {})
        try:
            report.mail = collect_reports(self.mailbox, self.storage, s.incoming_dir, seen,
                                          s.mail_sender, s.mail_lookback_days)
        except MailPermissionError as exc:
            # Expected until IT grants the permission: keep the rest of the run going
            log.warning("Mail: %s", exc)
            report.warnings.append(str(exc))
            return
        except Exception as exc:
            log.exception("Mail collection failed")
            report.errors.append(f"mail: {exc}")
            return
        # Forget message ids well past the lookback window so the index stays small
        cutoff = (datetime.now(timezone.utc) - timedelta(days=2 * s.mail_lookback_days)).isoformat()
        index["mail"] = {k: v for k, v in seen.items() if v.get("received", "") >= cutoff}
        self.store.save_index(index)

    def run(self, backfill: int | None = None) -> PassReport:
        report = PassReport()
        owner = self.owner = f"{os.uname().nodename}:{os.getpid()}:{datetime.now().timestamp():.0f}"
        if not self.store.acquire_lock(owner):
            log.info("Another run is in progress; skipping this one")
            report.skipped = True
            return report
        try:
            self.run_catalogs(report, backfill)
            self.add_missing_quality(report)
            self.upgrade_stock_source(report)
            self.run_mail(report)
            self.run_productivity(report)
            self.audit_productivity(report)
            self.update_sku_history(report)
            self.run_ga4(report)
            self.run_gsc(report)
            self.refresh_performance(report)
            self.run_campaigns(report)
            self.run_alerts(report)
        finally:
            self.store.release_lock(owner)
        return report
