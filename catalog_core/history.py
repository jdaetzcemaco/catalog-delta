"""
Daily catalog-health history row (the Google Sheet, one row per day).

Pure functions only; reading and writing the sheet lives in the job/app adapters.
"""

from __future__ import annotations

import pandas as pd

HISTORY_HEADER = [
    "Date", "Total SKUs", "Δ SKUs", "Visible", "Δ Visible", "Visible %",
    "With Image %", "Δ Image %", "With Price %", "Δ Price %",
    "With Stock %", "Δ Stock %", "Avg Content Score", "Δ Score",
    "Score = 100", "Δ Perfect",
]

# Where each value sits in a previous row. v1 wrote a 9-column layout before deltas existed.
_NEW_LAYOUT = {"total": 1, "visible": 3, "image": 6, "price": 8, "stock": 10, "score": 12, "perfect": 14}
_OLD_LAYOUT = {"total": 1, "visible": 2, "image": 4, "price": 5, "stock": 6, "score": 7, "perfect": 8}


def _num(value: str, cast=float):
    text = str(value).strip().replace(",", "")
    return cast(float(text)) if text else cast(0)


def _day(value: str) -> str | None:
    """Sheet date cell as YYYY-MM-DD; Sheets may display it as e.g. 3/31/2026."""
    parsed = pd.to_datetime(str(value).strip(), errors="coerce")
    return None if pd.isna(parsed) else parsed.strftime("%Y-%m-%d")


def _parse_previous(row: list[str]) -> dict | None:
    if len(row) < 9:
        return None
    layout = _OLD_LAYOUT if len(row) < 15 else _NEW_LAYOUT
    try:
        return {
            "total": _num(row[layout["total"]], int),
            "visible": _num(row[layout["visible"]], int),
            "image": _num(row[layout["image"]]),
            "price": _num(row[layout["price"]]),
            "stock": _num(row[layout["stock"]]),
            "score": _num(row[layout["score"]]),
            "perfect": _num(row[layout["perfect"]], int),
        }
    except (ValueError, IndexError):
        return None


def previous_day_row(rows: list[list[str]], day: str) -> list[str] | None:
    """
    Latest data row dated before `day` (YYYY-MM-DD).

    FIX F6: v1 used the sheet's last row, so saving twice in a day compared the
    day against itself.
    """
    earlier = [(d, r) for r in rows[1:] if r for d in [_day(r[0])] if d and d < day]
    return max(earlier, key=lambda x: x[0])[1] if earlier else None


def build_history_row(summary: pd.DataFrame, day: str, rows: list[list[str]]) -> list:
    """History row for `day`, with deltas against the previous day already in `rows`."""
    s = summary.iloc[0]
    today = {
        "total": int(s["Total SKUs"]),
        "visible": int(s["Visible"]),
        "visible_pct": float(s["Visible %"]),
        "image": float(s["With Image %"]),
        "price": float(s["With Price %"]),
        "stock": float(s["With Stock %"]),
        "score": float(s["Avg Content Score"]),
        "perfect": int(s["Score = 100"]),
    }
    prev_row = previous_day_row(rows, day)
    prev = _parse_previous(prev_row) if prev_row else None

    def d(key: str, ndigits: int | None = None):
        if prev is None:
            return 0 if ndigits is None else 0.0
        diff = today[key] - prev[key]
        return diff if ndigits is None else round(diff, ndigits)

    return [
        day,
        today["total"], d("total"),
        today["visible"], d("visible"),
        today["visible_pct"],
        today["image"], d("image", 2),
        today["price"], d("price", 2),
        today["stock"], d("stock", 2),
        today["score"], d("score", 2),
        today["perfect"], d("perfect"),
    ]


def row_index_for_day(rows: list[list[str]], day: str) -> int | None:
    """1-based sheet row already holding `day`, so re-running a day updates it (FIX F6)."""
    for i, r in enumerate(rows[1:], start=2):
        if r and _day(r[0]) == day:
            return i
    return None
