import pandas as pd

from catalog_core import HISTORY_HEADER, build_history_row
from catalog_core.history import row_index_for_day

SUMMARY = pd.DataFrame([{
    "Total SKUs": 1000, "Visible": 300, "Visible %": 30.0, "With Image %": 90.0,
    "With Price %": 99.0, "With Stock %": 25.0, "Avg Content Score": 85.5, "Score = 100": 0,
}])


def test_first_row_has_zero_deltas():
    row = build_history_row(SUMMARY, "2026-04-01", [HISTORY_HEADER])
    assert row == ["2026-04-01", 1000, 0, 300, 0, 30.0, 90.0, 0.0, 99.0, 0.0, 25.0, 0.0, 85.5, 0.0, 0, 0]


def test_deltas_against_previous_day_not_last_row():
    rows = [
        HISTORY_HEADER,
        ["2026-03-31", "990", "0", "310", "0", "31", "89.5", "0", "99", "0", "24", "0", "85", "0", "0", "0"],
        # Earlier save of the same day must not be the baseline (FIX F6)
        ["2026-04-01", "1000", "10", "300", "-10", "30", "90", "0.5", "99", "0", "25", "1", "85.5", "0.5", "0", "0"],
    ]
    row = build_history_row(SUMMARY, "2026-04-01", rows)
    assert row[2] == 10 and row[4] == -10 and row[7] == 0.5 and row[13] == 0.5
    assert row_index_for_day(rows, "2026-04-01") == 3


def test_reads_old_nine_column_layout_and_sheet_formats():
    rows = [
        ["Date", "Total", "Visible", "Vis%", "Img%", "Price%", "Stock%", "Score", "Perfect"],
        ["3/31/2026", "1,000", "250", "25", "80", "99", "20", "80", "0"],
    ]
    row = build_history_row(SUMMARY, "2026-04-01", rows)
    assert row[2] == 0 and row[4] == 50 and row[7] == 10.0 and row[13] == 5.5
    assert row_index_for_day(rows, "2026-03-31") == 2
