"""
Read-only mailbox check: lists the STEP report e-mails the job would pick up.
Downloads and writes nothing.

    python3 tools/check_mail.py          # uses .env / the same env vars as the job
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.config import Settings  # noqa: E402
from pipeline.mail import LOCAL_TZ, GraphMailbox, MailPermissionError, match_report  # noqa: E402
from pipeline.storage import GraphStorage  # noqa: E402


def main() -> int:
    s = Settings.from_env()
    user = s.mail_user or s.graph_drive_user
    try:
        box = GraphMailbox(GraphStorage(s.graph_tenant_id, s.graph_client_id, s.graph_client_secret,
                                        s.graph_drive_user), user)
        since = datetime.now(timezone.utc) - timedelta(days=s.mail_lookback_days)
        messages = box.messages_since(since)
    except ValueError as exc:
        print(f"Config: {exc}")
        return 2
    except MailPermissionError as exc:
        print(f"✗ {exc}")
        return 1

    print(f"✓ Mailbox {user}: {len(messages)} messages with attachments in the last {s.mail_lookback_days} days")
    found = 0
    for m in messages:
        r = match_report(m.subject)
        if r is None or m.sender != s.mail_sender.lower():
            continue
        found += 1
        day = m.received.astimezone(LOCAL_TZ).strftime("%Y-%m-%d")
        print(f"  {m.received.astimezone(LOCAL_TZ):%Y-%m-%d %H:%M}  → productivity-{r.kind}-{day}.xlsx   {m.subject[:70]}")
    print(f"  {found} STEP report e-mails would be collected")
    return 0


if __name__ == "__main__":
    sys.exit(main())
