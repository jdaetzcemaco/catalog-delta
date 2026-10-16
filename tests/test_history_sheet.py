from __future__ import annotations

import pandas as pd

from pipeline.history_sheet import SheetHistory


def test_history_sheet_failure_does_not_raise(caplog):
    h = SheetHistory("sheet", {"client_email": "sa@x.iam.gserviceaccount.com"})

    def boom():
        raise PermissionError("Google Sheets API has not been used in project 1 before or it is disabled")

    h._connect = boom
    summary = pd.DataFrame([{"Total SKUs": 1, "Visible": 1, "Visible %": 100.0, "With Image %": 100.0,
                             "With Price %": 100.0, "With Stock %": 100.0, "Avg Content Score": 95.0, "Perfect Score": 1}])
    with caplog.at_level("WARNING"):
        assert h.upsert("2026-10-15", summary) == []
        assert h.upsert("2026-10-16", summary) == []
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1 and "enable the Google Sheets API" in warnings[0].getMessage()
