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


def test_excel_from_store_matches_sheet_order(env):
    from pipeline.export import excel_sheets_from_store

    job, drop, history, sku = env
    drop("catalog-daily-2026-04-01.xlsx", [sku("1")])
    drop("catalog-daily-2026-04-02.xlsx", [sku("1", VISIBLE="No"), sku("2")])
    job.run()
    sheets = excel_sheets_from_store(job.store, "2026-04-02")
    names = list(sheets)
    assert names[0] == "Catalog Health" and names[1:3] == ["New SKUs", "Removed SKUs"]
    assert names.index("Stock No Visible") > names.index("Stock Not Visible")
    assert sheets["Catalog Health"]["Total SKUs"].iloc[0] == 2


def test_concurrent_run_is_skipped_and_stale_lock_ignored(env):
    import json
    from datetime import datetime, timedelta, timezone

    job, drop, history, sku = env
    drop("catalog-daily-2026-04-01.xlsx", [sku("1")])
    lock_path = "cemaco-reports/processed/lock.json"
    fresh = {"owner": "other-host", "since": datetime.now(timezone.utc).isoformat()}
    job.storage.write(lock_path, json.dumps(fresh).encode())
    r = job.run()
    assert r.skipped and not r.catalogs

    stale = {"owner": "other-host", "since": (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()}
    job.storage.write(lock_path, json.dumps(stale).encode())
    r = job.run()
    assert not r.skipped and r.catalogs == ["2026-04-01"]
    # Released afterwards, so the next scheduled run can go
    assert not job.run().skipped


def test_older_files_are_left_alone_until_backfill(env):
    job, drop, history, sku = env
    for d, rows in [("01", [sku("1")]), ("02", [sku("1"), sku("2")]), ("03", [sku("1")]), ("04", [sku("1"), sku("4")])]:
        drop(f"catalog-daily-2026-04-{d}.xlsx", rows)
    assert job.run().catalogs == ["2026-04-03", "2026-04-04"]
    # A later pass must not pick up 01/02 just because they are unprocessed
    assert not job.run().did_work

    r = job.run(backfill=4)
    assert r.catalogs == ["2026-04-01", "2026-04-02"] and r.recomputed == ["2026-04-03"]
    m3 = job.store.load_manifest("2026-04-03")
    assert m3["previous_day"] == "2026-04-02" and m3["sku_changes"]["removed"] == 1
    assert job.store.load_manifest("2026-04-04")["previous_day"] == "2026-04-03"


def test_late_file_inside_processed_window_is_still_picked_up(env):
    job, drop, history, sku = env
    drop("catalog-daily-2026-04-01.xlsx", [sku("1")])
    drop("catalog-daily-2026-04-03.xlsx", [sku("1")])
    job.run()
    drop("catalog-daily-2026-04-02.xlsx", [sku("1")])
    assert job.run().catalogs == ["2026-04-02"]


def test_backfill_reuses_the_day_just_computed(env, monkeypatch):
    job, drop, history, sku = env
    for d in ["01", "02", "03"]:
        drop(f"catalog-daily-2026-04-{d}.xlsx", [sku("1"), sku(d)])
    loads = []
    real = job.store.load_snapshot
    monkeypatch.setattr(job.store, "load_snapshot", lambda day, columns=None: loads.append(day) or real(day, columns))
    assert job.run(backfill=3).catalogs == ["2026-04-01", "2026-04-02", "2026-04-03"]
    assert loads == []  # no snapshot downloaded: each previous day was still in memory
    assert job.store.load_manifest("2026-04-03")["sku_changes"] == {"new": 1, "removed": 1, "net": 0}


def test_dotenv_fills_only_missing_variables(tmp_path, monkeypatch):
    from pipeline.config import load_dotenv

    (tmp_path / ".env").write_text("# comment\nGRAPH_DRIVE_USER=a@cemaco.com\nGRAPH_CLIENT_ID='abc'\n")
    monkeypatch.delenv("GRAPH_DRIVE_USER", raising=False)
    monkeypatch.setenv("GRAPH_CLIENT_ID", "already-set")
    load_dotenv(str(tmp_path / ".env"))
    import os
    assert os.environ["GRAPH_DRIVE_USER"] == "a@cemaco.com"
    assert os.environ["GRAPH_CLIENT_ID"] == "already-set"


def test_job_collects_mail_then_processes_it_and_survives_missing_permission(env):
    import pandas as pd
    from pipeline.mail import MailPermissionError, Message

    job, drop, history, sku = env
    df = pd.DataFrame({"<ID>": ["catgo-1", "1230797"], "<Name>": ["A", "B"], "Categoría": [None, "X"],
                       "Completitud Mercadeo (Calculado)": [12, 0], "Estado Imagen Primaria": ["Sin Imagen"] * 2,
                       "Fecha de Creación STEP": ["2026-09-28"] * 2})

    class Box:
        def messages_since(self, since):
            from datetime import datetime, timezone
            return [Message("m1", "STEP - Reporte Diario Productos que Ingresaron al flujo de STEP",
                            "noreply@cloudmail.stibo.com", datetime.now(timezone.utc))]

        def xlsx_attachments(self, message_id):
            return [("excel.xlsx", _xlsx(df, "Sheet1"))]

    job.mailbox = Box()
    r = job.run()
    assert len(r.mail) == 1 and r.productivity == [f"ingresos {r.mail[0][22:32]}"]
    assert job.store.load_productivity(r.mail[0][22:32], "ingresos")["<ID>"].tolist() == ["catgo-1", "1230797"]

    class NoPermission:
        def messages_since(self, since):
            raise MailPermissionError("Mail.Read not granted")

    job.mailbox = NoPermission()
    r = job.run()
    assert r.warnings and not r.errors


def test_quality_checks_are_stored_and_added_to_older_days(env):
    job, drop, history, sku = env
    drop("catalog-daily-2026-04-01.xlsx", [sku("1"), sku("2", STOCK="1000000"), sku("3", STOCK="1000100")])
    job.run()
    assert job.store.load_manifest("2026-04-01")["quality"] == {"stock_placeholder": 2, "stock_placeholder_pct": 66.67}

    # A day stored before the checks existed gets them on the next pass, without recomputing rules
    m = job.store.load_manifest("2026-04-01")
    del m["quality"]
    job.store.save_manifest("2026-04-01", m)
    r = job.run()
    assert r.upgraded == ["2026-04-01"] and not r.catalogs
    assert job.store.load_manifest("2026-04-01")["quality"]["stock_placeholder"] == 2
    assert job.run().upgraded == []


def test_mislabelled_productivity_file_is_ignored_and_forgotten(env):
    import pandas as pd

    job, drop, history, sku = env
    edicion_cols = pd.DataFrame({"<ID>": [1], "<Name>": ["A"], "Usuario": ["dani"],
                                 "Usuario Promueve desde Compras": ["eva"], "Total Omnicanal": [1],
                                 "Fecha de Salida del Flujo de trabajo": ["2026-09-28"]})
    diseno_cols = edicion_cols.rename(columns={"Usuario Promueve desde Compras": "Usuario Promueve desde Catalogo"})
    drop("productivity-diseno-2026-09-28.xlsx", diseno_cols, sheet="Sheet1")
    assert job.run().productivity == ["diseno 2026-09-28"]

    # The same name now holds an Edición workbook (the old mis-labelling): rejected, entry dropped
    drop("productivity-diseno-2026-09-28.xlsx", edicion_cols, sheet="Sheet1")
    r = job.run()
    assert r.productivity == [] and r.warnings and not r.errors
    assert "2026-09-28" not in job.store.load_index()["productivity"]["diseno"]
    # Warned once, not on every run
    assert not job.run().warnings
