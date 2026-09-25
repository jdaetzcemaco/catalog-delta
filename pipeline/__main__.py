"""
Run one pass of the processing job.

    python -m pipeline                       # OneDrive + Google Sheets (Render cron)
    python -m pipeline --local data/onedrive --no-history
    python -m pipeline --recompute 2026-03-31
"""

from __future__ import annotations

import argparse
import logging
import sys

from .config import Settings
from .process import Job, PassReport
from .storage import make_storage


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m pipeline", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--local", metavar="ROOT", help="Use a local folder instead of OneDrive")
    p.add_argument("--no-history", action="store_true", help="Do not write the Google Sheets history")
    p.add_argument("--recompute", metavar="YYYY-MM-DD", help="Re-run a stored day from its snapshot")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = Settings.from_env()
    if args.local:
        settings.storage, settings.local_root = "local", args.local
    if args.no_history:
        settings.history_enabled = False

    history = None
    if settings.history_enabled:
        if not settings.google_service_account:
            logging.error("GCP_SERVICE_ACCOUNT_JSON is not set (or pass --no-history)")
            return 2
        from .history_sheet import SheetHistory
        history = SheetHistory(settings.google_sheet_id, settings.google_service_account)

    job = Job(settings, make_storage(settings), history)
    if args.recompute:
        index = job.store.load_index()
        if args.recompute not in index["catalog"]:
            logging.error("%s has not been processed", args.recompute)
            return 2
        job.recompute_from_snapshot(index, args.recompute)
        report = PassReport(recomputed=[args.recompute])
    else:
        report = job.run()

    logging.info("Done: catalogs=%s recomputed=%s productivity=%s errors=%s",
                 report.catalogs, report.recomputed, report.productivity, len(report.errors))
    for err in report.errors:
        logging.error(err)
    return 1 if report.errors else 0


if __name__ == "__main__":
    sys.exit(main())
