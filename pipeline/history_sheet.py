"""Google Sheets adapter for the daily catalog-health history (one row per day)."""

from __future__ import annotations

import logging

import pandas as pd

from catalog_core.history import HISTORY_HEADER, build_history_row, row_index_for_day

log = logging.getLogger(__name__)
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


class SheetHistory:
    def __init__(self, sheet_id: str, service_account_info: dict):
        import gspread
        from google.oauth2.service_account import Credentials

        creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
        self.ws = gspread.authorize(creds).open_by_key(sheet_id).sheet1

    def upsert(self, day: str, summary: pd.DataFrame) -> list:
        rows = self.ws.get_all_values()
        if not rows:
            self.ws.append_row(HISTORY_HEADER)
            rows = [HISTORY_HEADER]
        row = build_history_row(summary, day, rows)
        idx = row_index_for_day(rows, day)
        if idx:
            self.ws.update(range_name=f"A{idx}:P{idx}", values=[row], value_input_option="USER_ENTERED")
            log.info("History: updated row %s for %s", idx, day)
        else:
            self.ws.append_row(row, value_input_option="USER_ENTERED")
            log.info("History: appended %s", day)
        return row
