"""Google Sheets adapter for the daily catalog-health history (one row per day)."""

from __future__ import annotations

import logging

import pandas as pd

from catalog_core.history import HISTORY_HEADER, build_history_row, row_index_for_day

log = logging.getLogger(__name__)
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


class SheetHistory:
    """
    Connects lazily and never raises out of `upsert`: the sheet is a convenience copy of
    numbers the store already holds, so a Sheets outage or a missing permission must
    not stop the job. The first failure is logged as a warning; later ones at debug.
    """

    def __init__(self, sheet_id: str, service_account_info: dict):
        self.sheet_id, self.info = sheet_id, service_account_info
        self.ws = None
        self.failed = False

    def _connect(self):
        import gspread
        from google.oauth2.service_account import Credentials

        creds = Credentials.from_service_account_info(self.info, scopes=SCOPES)
        self.ws = gspread.authorize(creds).open_by_key(self.sheet_id).sheet1

    def upsert(self, day: str, summary: pd.DataFrame) -> list:
        try:
            if self.ws is None:
                self._connect()
            return self._upsert(day, summary)
        except Exception as exc:  # noqa: BLE001 - optional side channel
            msg = str(exc)
            hint = ""
            if "has not been used in project" in msg or "it is disabled" in msg:
                hint = " → enable the Google Sheets API for the project and retry"
            elif "403" in msg or "PERMISSION" in msg.upper() or isinstance(exc, PermissionError):
                hint = f" → share the sheet with {self.info.get('client_email')} (Editor)"
            (log.debug if self.failed else log.warning)("History sheet skipped for %s: %s%s", day, msg[:200], hint)
            self.failed = True
            return []

    def _upsert(self, day: str, summary: pd.DataFrame) -> list:
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
