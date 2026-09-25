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
from datetime import datetime
from typing import Protocol

import pandas as pd

from catalog_core import load_catalog, load_productivity, run_catalog

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
TEAMS = ("diseno", "edicion")


class History(Protocol):
    def upsert(self, day: str, summary: pd.DataFrame) -> list: ...


@dataclass
class PassReport:
    catalogs: list[str] = field(default_factory=list)
    recomputed: list[str] = field(default_factory=list)
    productivity: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    skipped: bool = False

    @property
    def did_work(self) -> bool:
        return bool(self.catalogs or self.recomputed or self.productivity)


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
    def __init__(self, settings: Settings, storage: Storage, history: History | None = None):
        self.settings = settings
        self.storage = storage
        self.store = ResultStore(storage, settings.processed_dir)
        self.history = history
        self.owner: str | None = None

    # ── catalog ─────────────────────────────────────────────────────────────
    def _previous_day(self, index: dict, day: str) -> str | None:
        earlier = [d for d in index["catalog"] if d < day]
        return max(earlier) if earlier else None

    def _next_day(self, index: dict, day: str) -> str | None:
        later = [d for d in index["catalog"] if d > day]
        return min(later) if later else None

    def _compute(self, index: dict, day: str, today_raw: pd.DataFrame, source: dict) -> None:
        prev = self._previous_day(index, day)
        yesterday_raw = self.store.load_snapshot(prev) if prev else None
        log.info("%s: %s SKUs, compared with %s", day, f"{len(today_raw):,}", prev or "nothing (first day)")
        run = run_catalog(today_raw, yesterday_raw)
        del yesterday_raw
        self.store.save_snapshot(day, today_raw)
        self.store.save_run(day, run, source, prev)
        if self.history is not None:
            self.history.upsert(day, run.summary)
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

    def run_catalogs(self, report: PassReport) -> None:
        s = self.settings
        index = self.store.load_index()
        files = latest_per_day(self.storage.list(s.incoming_dir, s.catalog_pattern))
        todo = [(day, f) for day, f in files.items() if is_new(index["catalog"].get(day), f)]
        if not index["catalog"] and len(todo) > s.bootstrap_files:
            log.info("Empty store: starting from the newest %s of %s catalog files", s.bootstrap_files, len(todo))
            todo = todo[-s.bootstrap_files:]
        if not todo:
            log.info("No new catalog files")
        for day, f in todo:
            try:
                follower = self._next_day(index, day)
                self.process_catalog(index, day, f)
                report.catalogs.append(day)
                # A day that arrived late changes the baseline of the day after it
                if follower and follower not in {d for d, _ in todo}:
                    self.recompute_from_snapshot(index, follower)
                    report.recomputed.append(follower)
            except Exception as exc:
                log.exception("Catalog %s (%s) failed", day, f.name)
                report.errors.append(f"catalog {f.name}: {exc}")
            gc.collect()
            if self.owner:
                self.store.acquire_lock(self.owner)  # keep the lease alive during a backfill

    # ── productivity ────────────────────────────────────────────────────────
    def run_productivity(self, report: PassReport) -> None:
        s = self.settings
        index = self.store.load_index()
        patterns = {"diseno": s.diseno_pattern, "edicion": s.edicion_pattern}
        for team in TEAMS:
            seen = index["productivity"].setdefault(team, {})
            for day, f in latest_per_day(self.storage.list(s.incoming_dir, patterns[team])).items():
                if not is_new(seen.get(day), f):
                    continue
                try:
                    df = load_productivity(self.storage.read(f.path))
                    self.store.save_productivity(day, team, df)
                    seen[day] = ResultStore.source_entry(f)
                    self.store.save_index(index)
                    report.productivity.append(f"{team} {day}")
                    log.info("Productivity %s %s: %s rows", team, day, len(df))
                except Exception as exc:
                    log.exception("Productivity %s (%s) failed", team, f.name)
                    report.errors.append(f"productivity {f.name}: {exc}")

    def run(self) -> PassReport:
        report = PassReport()
        owner = self.owner = f"{os.uname().nodename}:{os.getpid()}:{datetime.now().timestamp():.0f}"
        if not self.store.acquire_lock(owner):
            log.info("Another run is in progress; skipping this one")
            report.skipped = True
            return report
        try:
            self.run_catalogs(report)
            self.run_productivity(report)
        finally:
            self.store.release_lock(owner)
        return report
