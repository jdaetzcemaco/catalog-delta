from __future__ import annotations

import io
import os
import time

import pandas as pd
import pytest

from pipeline.config import Settings
from pipeline.process import Job, file_day, latest_per_day
from pipeline.storage import FileInfo, LocalStorage
from pipeline.store import ResultStore, from_parquet_bytes, to_parquet_bytes


class FakeHistory:
    def __init__(self):
        self.calls = []

    def upsert(self, day, summary):
        self.calls.append((day, int(summary.iloc[0]["Total SKUs"])))
        return []


def _xlsx(df: pd.DataFrame, sheet="SKUs") -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        df.to_excel(w, sheet_name=sheet, index=False)
    return out.getvalue()


@pytest.fixture
def env(tmp_path, make_sku):
    root = tmp_path / "onedrive"
    incoming = root / "cemaco-reports" / "incoming"
    incoming.mkdir(parents=True)
    settings = Settings(storage="local", local_root=str(root), history_enabled=False)
    history = FakeHistory()
    job = Job(settings, LocalStorage(str(root)), history)

    def drop(name: str, rows: list[dict] | pd.DataFrame, sheet="SKUs"):
        df = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(rows)
        path = incoming / name
        path.write_bytes(_xlsx(df, sheet))
        # Distinct modified times so "replaced file" detection is deterministic
        t = time.time() + len(os.listdir(incoming))
        os.utime(path, (t, t))

    return job, drop, history, make_sku


def test_first_day_is_a_baseline_then_second_day_has_deltas(env):
    job, drop, history, sku = env
    drop("catalog-daily-2026-04-01.xlsx", [sku("1"), sku("2")])
    r1 = job.run()
    assert r1.catalogs == ["2026-04-01"] and not r1.errors
    m1 = job.store.load_manifest("2026-04-01")
    assert m1["baseline"] and m1["sku_changes"] is None and m1["tables"]["changes"] == {}

    drop("catalog-daily-2026-04-02.xlsx", [sku("1", VISIBLE="No"), sku("3")])
    r2 = job.run()
    assert r2.catalogs == ["2026-04-02"]
    m2 = job.store.load_manifest("2026-04-02")
    assert m2["previous_day"] == "2026-04-01"
    assert m2["sku_changes"] == {"new": 1, "removed": 1, "net": 0}
    assert job.store.load_table("2026-04-02", "changes", "No Longer Visible")["SKU"].tolist() == ["1"]
    assert m2["summary"]["Total SKUs"] == 2
    assert history.calls == [("2026-04-01", 2), ("2026-04-02", 2)]


def test_second_pass_does_nothing(env):
    job, drop, history, sku = env
    drop("catalog-daily-2026-04-01.xlsx", [sku("1")])
    job.run()
    again = job.run()
    assert not again.did_work and len(history.calls) == 1


def test_replaced_file_is_reprocessed(env):
    job, drop, history, sku = env
    drop("catalog-daily-2026-04-01.xlsx", [sku("1")])
    job.run()
    drop("catalog-daily-2026-04-01.xlsx", [sku("1"), sku("2")])
    assert job.run().catalogs == ["2026-04-01"]
    assert job.store.load_manifest("2026-04-01")["summary"]["Total SKUs"] == 2


def test_late_day_recomputes_the_day_after_it(env):
    job, drop, history, sku = env
    drop("catalog-daily-2026-04-01.xlsx", [sku("1")])
    drop("catalog-daily-2026-04-03.xlsx", [sku("1"), sku("2"), sku("3")])
    job.run()
    assert job.store.load_manifest("2026-04-03")["previous_day"] == "2026-04-01"

    drop("catalog-daily-2026-04-02.xlsx", [sku("1"), sku("2")])
    r = job.run()
    assert r.catalogs == ["2026-04-02"] and r.recomputed == ["2026-04-03"]
    m3 = job.store.load_manifest("2026-04-03")
    assert m3["previous_day"] == "2026-04-02" and m3["sku_changes"]["new"] == 1


def test_bootstrap_only_takes_newest_files(env):
    job, drop, history, sku = env
    for d in ["01", "02", "03", "04"]:
        drop(f"catalog-daily-2026-04-{d}.xlsx", [sku("1")])
    assert job.run().catalogs == ["2026-04-03", "2026-04-04"]


def test_bad_file_is_reported_and_others_continue(env):
    job, drop, history, sku = env
    drop("catalog-daily-2026-04-01.xlsx", pd.DataFrame({"NO_SKU": [1]}))
    drop("catalog-daily-2026-04-02.xlsx", [sku("1")])
    r = job.run()
    assert r.catalogs == ["2026-04-02"] and len(r.errors) == 1 and "SKU" in r.errors[0]


def test_productivity_files_are_stored_per_team_and_day(env):
    job, drop, history, sku = env
    df = pd.DataFrame({"<ID>": [1193552], "<Name>": ["X"], "Total Omnicanal": [3],
                       "Fecha de Salida del Flujo de trabajo": ["2026-04-22"]})
    drop("productivity-diseno-2026-04-23_07.30.10.xlsx", df, sheet="Sheet1")
    r = job.run()
    assert r.productivity == ["diseno 2026-04-23"]
    stored = job.store.load_productivity("2026-04-23", "diseno")
    assert stored["<ID>"].tolist() == ["1193552"]


def test_file_day_and_latest_per_day():
    from datetime import datetime, timezone
    a = FileInfo("catalog-daily-2026-04-01.xlsx", "x", datetime(2026, 4, 1, 12, tzinfo=timezone.utc), 1)
    b = FileInfo("catalog-daily-2026-04-01 (1).xlsx", "y", datetime(2026, 4, 1, 13, tzinfo=timezone.utc), 1)
    # No date in the name: modified time in Guatemala (UTC-6) is still 2026-03-31
    c = FileInfo("catalog-daily.xlsx", "z", datetime(2026, 4, 1, 3, tzinfo=timezone.utc), 1)
    assert file_day(c) == "2026-03-31"
    assert latest_per_day([a, b, c]) == {"2026-03-31": c, "2026-04-01": b}


def test_parquet_round_trip_keeps_text_and_nan():
    df = pd.DataFrame({"SKU": ["1", "2"], "MODAL": [float("nan"), "M"], "mixed": [1, "a"]})
    back = from_parquet_bytes(to_parquet_bytes(df))
    assert back["SKU"].tolist() == ["1", "2"]
    assert pd.isna(back.loc[0, "MODAL"]) and back.loc[1, "MODAL"] == "M"
    assert back["mixed"].tolist() == ["1", "a"]
