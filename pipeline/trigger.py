"""Start processing right after a manual upload instead of waiting for the next cron run."""

from __future__ import annotations

import logging
import os

import requests

from .config import Settings

log = logging.getLogger(__name__)


def trigger_processing(settings: Settings) -> str:
    """
    Returns "triggered" (Render cron run started), "processed" (ran here; local dev)
    or "scheduled" (picked up by the next cron run).
    """
    api_key, job_id = os.environ.get("RENDER_API_KEY"), os.environ.get("RENDER_CRON_JOB_ID")
    if api_key and job_id:
        r = requests.post(f"https://api.render.com/v1/cron-jobs/{job_id}/runs",
                          headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json"},
                          timeout=30)
        r.raise_for_status()
        return "triggered"
    if os.environ.get("PROCESS_INLINE", "").lower() in ("1", "true", "yes"):
        # Needs ~2 GB RAM; meant for local development, not the Render web service
        from .process import Job
        from .storage import make_storage

        history = None
        if settings.history_enabled and settings.google_service_account:
            from .history_sheet import SheetHistory
            history = SheetHistory(settings.google_sheet_id, settings.google_service_account)
        report = Job(settings, make_storage(settings), history).run()
        if report.errors:
            raise RuntimeError("; ".join(report.errors))
        return "processed"
    return "scheduled"
