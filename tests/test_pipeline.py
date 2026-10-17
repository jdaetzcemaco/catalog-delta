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

    stale = {"owner": "other-host", "since": (datetime.now(timezone.utc) - timedelta(minutes=25)).isoformat()}
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
    monkeypatch.setattr(job.store, "load_snapshot",
                        lambda day, columns=None: (loads.append(day) if columns is None else None) or real(day, columns))
    assert job.run(backfill=3).catalogs == ["2026-04-01", "2026-04-02", "2026-04-03"]
    # No full snapshot downloaded as a baseline: each previous day was still in memory
    # (the visible-history step reads only SKU/VISIBLE columns and is not counted)
    assert loads == []
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
    q0 = job.store.load_manifest("2026-04-01")["quality"]
    assert {k: q0[k] for k in ("stock_placeholder", "stock_placeholder_pct", "stock_corrected", "stock_source")} == {
        "stock_placeholder": 2, "stock_placeholder_pct": 66.67, "stock_corrected": False, "stock_source": "skus"}

    # A day stored before the checks existed gets them on the next pass, without recomputing rules
    m = job.store.load_manifest("2026-04-01")
    del m["quality"]
    job.store.save_manifest("2026-04-01", m)
    r = job.run()
    assert "2026-04-01" in r.upgraded and not r.catalogs
    q = job.store.load_manifest("2026-04-01")["quality"]
    assert q["stock_placeholder"] == 2
    # The placeholder day was re-read for a stock upgrade; its export has no Inventarios sheet,
    # so it is marked attempted and not retried
    assert q.get("stock_upgrade_attempted") and not q["stock_corrected"]
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


def test_audit_removes_stored_day_of_the_wrong_team_even_if_file_unchanged(env):
    import pandas as pd

    job, drop, history, sku = env
    edicion_cols = pd.DataFrame({"<ID>": [1], "<Name>": ["A"], "Usuario": ["dani"],
                                 "Usuario Promueve desde Compras": ["eva"], "Total Omnicanal": [1],
                                 "Fecha de Salida del Flujo de trabajo": ["2026-09-28"]})
    # What the old version did: an Edición workbook stored as Diseño, file untouched since
    drop("productivity-diseno-2026-09-28.xlsx", edicion_cols, sheet="Sheet1")
    f = job.storage.list("cemaco-reports/incoming", "productivity-diseno-*")[0]
    index = job.store.load_index()
    index["productivity"]["diseno"] = {"2026-09-28": job.store.source_entry(f)}
    job.store.save_index(index)
    job.store.save_productivity("2026-09-28", "diseno", edicion_cols)

    r = job.run()
    assert r.warnings and not r.errors and r.productivity == []
    assert "2026-09-28" not in job.store.load_index()["productivity"]["diseno"]
    # Stays out, and is not warned about again
    r = job.run()
    assert not r.warnings and "2026-09-28" not in job.store.load_index()["productivity"]["diseno"]


def test_campaign_list_ingested_and_tracked_daily(env):
    import json

    job, drop, history, sku = env
    drop("catalog-daily-2026-04-01.xlsx", [sku("1"), sku("2", VISIBLE="No")])
    job.storage.write("cemaco-reports/incoming/lists/navidad.json", json.dumps(
        {"name": "Navidad", "skus": ["1", "2", "9"], "status": "active", "owner": "ana"}).encode())
    r = job.run()
    assert "ingest navidad" in r.lists and "status navidad 2026-04-01" in r.lists
    st = job.store.load_list_status("navidad", "2026-04-01").set_index("SKU")
    assert st.loc["1", "listo"] and not st.loc["2", "listo"] and not st.loc["9", "en_catalogo"]
    assert job.store.load_index()["lists"]["navidad"]["last_kpis"] == {"listos": 1, "total": 3}
    # Visible history was built and joined
    assert st.loc["1", "ultima_vez_visible"] == pd.Timestamp("2026-04-01")

    # Nothing new: no recompute. New catalog day: new status; archived list: skipped
    assert not job.run().lists
    drop("catalog-daily-2026-04-02.xlsx", [sku("1"), sku("2")])
    job.storage.write("cemaco-reports/incoming/lists/navidad.json", json.dumps(
        {"name": "Navidad", "skus": ["1", "2", "9"], "status": "archived"}).encode())
    r = job.run()
    assert r.lists == ["ingest navidad"]
    assert job.store.load_list_status("navidad", "2026-04-02") is None
    hist = job.store.load_visible_history().set_index("SKU")
    assert hist.loc["2", "visible_streak"] == 1 and hist.loc["1", "visible_streak"] == 2


def test_visible_history_rebuilds_when_a_day_arrives_late(env):
    job, drop, history, sku = env
    drop("catalog-daily-2026-04-01.xlsx", [sku("1", VISIBLE="No")])
    drop("catalog-daily-2026-04-03.xlsx", [sku("1")])
    job.run()
    drop("catalog-daily-2026-04-02.xlsx", [sku("1")])
    job.run()
    hist = job.store.load_visible_history().set_index("SKU")
    assert hist.loc["1", "visible_streak"] == 2 and hist.loc["1", "days_visible"] == 2
    assert job.store.load_index()["sku_history"]["visible_days"] == ["2026-04-01", "2026-04-02", "2026-04-03"]


def test_alerts_are_written_each_pass_and_remember_first_seen(env, monkeypatch):
    import json

    job, drop, history, sku = env
    drop("catalog-daily-2026-04-01.xlsx", [sku(str(i)) for i in range(20)])
    job.storage.write("cemaco-reports/incoming/lists/promo.json", json.dumps(
        {"name": "Promo", "skus": [str(i) for i in range(20)], "status": "active"}).encode())
    job.run()
    # Everything ready, nothing dropped; only the clock-based "export late" rule can fire on test dates
    assert [a["rule"] for a in job.store.load_alerts("2026-04-01")["alerts"]] == ["export_late"]

    # Next day: 5 SKUs hidden -> list regression (alta) + low readiness? 75% < 80 -> yes
    drop("catalog-daily-2026-04-02.xlsx", [sku(str(i), VISIBLE="No" if i < 5 else "Si") for i in range(20)])
    r = job.run()
    payload = job.store.load_alerts("2026-04-02")
    rules = {a["rule"]: a for a in payload["alerts"]}
    assert set(rules) >= {"list_regression", "list_low_ready"} and rules["list_regression"]["skus"] == [str(i) for i in range(5)]
    assert rules["list_regression"]["severity"] == "alta"
    assert job.store.load_index()["alerts"]["last_day"] == "2026-04-02"

    # Day 3 same state: regression gone, low readiness persists with first_seen from day 2
    drop("catalog-daily-2026-04-03.xlsx", [sku(str(i), VISIBLE="No" if i < 5 else "Si") for i in range(20)])
    job.run()
    rules = {a["rule"]: a for a in job.store.load_alerts("2026-04-03")["alerts"]}
    assert "list_regression" not in rules and rules["list_low_ready"]["first_seen"] == "2026-04-02"


def test_alert_thresholds_from_env(monkeypatch):
    from pipeline.config import Settings

    monkeypatch.setenv("ALERT_LIST_MIN_READY_PCT", "95")
    assert Settings.from_env().alert_thresholds["list_min_ready_pct"] == 95.0


def test_ga4_and_gsc_pass_backfill_in_chunks_and_build_sku_performance(env):
    from datetime import date, timedelta

    job, drop, history, sku = env
    drop("catalog-daily-2026-04-01.xlsx", [sku("1"), sku("2")])
    job.settings.ga4_backfill_days = 40
    job.settings.ga4_refresh_days = 2

    class FakeGA4:
        calls = []

        def items_report(self, day):
            self.calls.append(day)
            return pd.DataFrame({"day": [day], "itemId": ["100"], "itemVariant": ["1"], "SKU": ["1"], "itemName": ["A"],
                                 "itemsViewed": [3], "itemsAddedToCart": [1], "itemsPurchased": [0], "itemRevenue": [0.0]})

        def window_rates(self, start, end):
            return pd.DataFrame({"SKU": ["1"], "itemsViewed": [400.0], "cartToViewRate": [0.1], "purchaseToViewRate": [0.03]})

        def window_channel_rates(self, start, end):
            return pd.DataFrame({"SKU": ["1", "1"], "bucket": ["organico", "pagado_social"], "views": [300.0, 100.0],
                                 "buyers": [9.0, 1.0]})

        def channels_report(self, day):
            return pd.DataFrame({"day": [day, day], "itemVariant": ["1", "1"], "SKU": ["1", "1"],
                                 "sessionDefaultChannelGroup": ["Organic Search", "Paid Social"],
                                 "bucket": ["organico", "pagado_social"], "itemsViewed": [2, 1],
                                 "itemsAddedToCart": [1, 0], "itemsPurchased": [0, 0], "itemRevenue": [0.0, 0.0]})

        def landing_report(self, day):
            return pd.DataFrame({"day": [day], "landingPagePlusQueryString": ["/a-1/p"],
                                 "sessionDefaultChannelGroup": ["Organic Search"], "sessions": [2],
                                 "engagedSessions": [1], "SKU": ["1"]})

    class FakeGSC:
        def pages_report(self, day):
            return pd.DataFrame({"day": [day], "page": ["/a-1/p"], "clicks": [1], "impressions": [10], "ctr": [0.1],
                                 "position": [4.0], "SKU": ["1"]})

    job.ga4, job.gsc = FakeGA4(), FakeGSC()
    r = job.run()
    assert len(r.ga4) == 30 and len(r.gsc) == 30  # capped per pass
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    assert r.ga4[0] == yesterday and r.gsc[0] == (date.today() - timedelta(days=3)).isoformat()
    r2 = job.run()
    assert len(r2.ga4) == 12 and yesterday in r2.ga4   # 10 left + 2 refreshed

    perf = job.store.load_perf().set_index("SKU")
    # Both SKUs belong to the test fixture's product? conftest has no PRODUCT ID → map by SKU only
    assert perf.loc["1", "organic_sessions_7d"] == 2 * 7 and perf.loc["1", "clicks_30d"] == 28  # GSC lags 3 days
    assert perf.loc["1", "vistas_30d"] == 3 * 30 and perf.loc["2", "vistas_30d"] == 0
    # Channel split and catalog context are on the performance file
    assert perf.loc["1", "vistas_30d_pagado_social"] == 30 and perf.loc["1", "vistas_30d_organico"] == 60
    assert perf.loc["1", "NIVEL 1"] == "Hogar" and bool(perf.loc["1", "visible"]) and perf.loc["1", "content_score"] == 95
    assert perf.loc["1", "vistas_ventana"] == 400 and perf.loc["1", "paid_buy_rate"] == 0.01
    cro, summary = job.store.load_cro()
    assert cro is not None and summary["channel_totals"][0]["bucket"] == "organico"
    assert perf.loc["1", "posicion_30d"] == 4.0
    assert job.store.load_index()["perf"]["as_of"] == yesterday
    assert len(job.run().ga4) == 2  # only the refresh window remains


def test_stock_source_upgrade_reprocesses_placeholder_days(env):
    import io

    job, drop, history, sku = env
    incoming = job.storage.root + "/cemaco-reports/incoming"

    def drop_with_inventory(name, rows, inv_rows):
        out = io.BytesIO()
        with pd.ExcelWriter(out, engine="openpyxl") as w:
            pd.DataFrame(rows).to_excel(w, sheet_name="SKUs", index=False)
            pd.DataFrame(inv_rows, columns=["Sku", "ProductName", "Total", "MDR", "T004"]).to_excel(
                w, sheet_name="Inventarios", index=False)
        with open(f"{incoming}/{name}", "wb") as fh:
            fh.write(out.getvalue())

    # Day 1 processed by "old code": placeholder STOCK, no Inventarios read → simulate by dropping a
    # workbook without the sheet first, then replacing it with the full export (same name)
    drop("catalog-daily-2026-09-22.xlsx", [sku("1", STOCK="1000003"), sku("2", STOCK="1000000")])
    drop("catalog-daily-2026-09-23.xlsx", [sku("1", STOCK="1000003"), sku("2", STOCK="1000000")])
    job.run()
    m = job.store.load_manifest("2026-09-22")
    assert not m["quality"]["stock_corrected"] and m["summary"]["With Stock %"] == 100.0

    drop_with_inventory("catalog-daily-2026-09-22.xlsx", [sku("1", STOCK="1000003"), sku("2", STOCK="1000000")],
                        [[1, "A", 1000003, 1000000, 3], [2, "B", 1000000, 1000000, 0]])
    drop_with_inventory("catalog-daily-2026-09-23.xlsx", [sku("1", STOCK="1000003"), sku("2", STOCK="1000000")],
                        [[1, "A", 1000003, 1000000, 3], [2, "B", 1000000, 1000000, 0]])
    r = job.run()  # files changed → normal reprocess picks them up with the Inventarios sheet
    for day in ("2026-09-22", "2026-09-23"):
        m = job.store.load_manifest(day)
        assert m["quality"]["stock_corrected"] and m["summary"]["With Stock %"] == 50.0
    # Snapshot carries STOCK REAL so recomputes keep the corrected stock
    snap = job.store.load_snapshot("2026-09-23", columns=["SKU", "STOCK REAL"]).set_index("SKU")
    assert snap.loc["2", "STOCK REAL"] == 0


def test_stock_source_upgrade_step_runs_for_unchanged_files(env, monkeypatch):
    """Stored day with placeholder + unchanged file → upgrade step re-processes it."""
    import io

    job, drop, history, sku = env
    incoming = job.storage.root + "/cemaco-reports/incoming"
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        pd.DataFrame([sku("1", STOCK="1000003"), sku("2", STOCK="1000000")]).to_excel(w, sheet_name="SKUs", index=False)
        pd.DataFrame([[1, "A", 1000003, 1000000, 3], [2, "B", 1000000, 1000000, 0]],
                     columns=["Sku", "ProductName", "Total", "MDR", "T004"]).to_excel(w, sheet_name="Inventarios", index=False)
    with open(f"{incoming}/catalog-daily-2026-09-22.xlsx", "wb") as fh:
        fh.write(out.getvalue())
    # First pass with the Inventarios reader disabled = what the old code stored
    monkeypatch.setattr("pipeline.process.load_inventory_sheet", lambda *a, **k: None)
    job.run()
    m = job.store.load_manifest("2026-09-22")
    assert not m["quality"]["stock_corrected"]
    # Old code never set the "attempted" marker; drop the one this pass's upgrade attempt left
    m["quality"].pop("stock_upgrade_attempted", None)
    job.store.save_manifest("2026-09-22", m)
    monkeypatch.undo()
    r = job.run()
    assert r.upgraded == ["stock 2026-09-22"]
    m = job.store.load_manifest("2026-09-22")
    assert m["quality"]["stock_corrected"] and m["summary"]["With Stock %"] == 50.0
    assert not job.run().upgraded
