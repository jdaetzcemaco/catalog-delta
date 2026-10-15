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
from catalog_core.campaigns import readiness
from catalog_core.productivity import detect_report
from catalog_core.sku_history import rebuild_visible_history, update_visible_history, update_workflow_dates

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
                 mailbox=None):
        self.settings = settings
        self.storage = storage
        self.store = ResultStore(storage, settings.processed_dir)
        self.history = history
        self.mailbox = mailbox
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
                or f"ingest {lid}" in report.lists]
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
                status = readiness(meta.get("skus", []), today_raw, flags, visible, workflow)
                self.store.save_list_status(lid, day, status)
                days_done = set(lists[lid].get("status_days", []))
                days_done.add(day)
                lists[lid]["status_days"] = sorted(days_done)
                lists[lid]["last_day"] = day
                lists[lid]["last_kpis"] = {"listos": int(status["listo"].sum()), "total": len(status)}
                self.store.save_index(index)
                report.lists.append(f"status {lid} {day}")
                log.info("List %s: %s/%s ready on %s", lid, int(status["listo"].sum()), len(status), day)
            except Exception as exc:
                log.exception("List %s status failed", lid)
                report.errors.append(f"list status {lid}: {exc}")
            self._renew_lock()

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
            self.run_mail(report)
            self.run_productivity(report)
            self.audit_productivity(report)
            self.update_sku_history(report)
            self.run_campaigns(report)
        finally:
            self.store.release_lock(owner)
        return report
