"""
Collect the STEP report e-mails from a mailbox and drop their .xlsx attachments into
incoming/ under stable names, so the normal productivity processing picks them up.

The attachments are all called excel-YYYY-MM-DD_HH.MM.SS.xlsx, so the report is
identified by the e-mail subject. The file date is the day the e-mail arrived
(Guatemala time); each report describes the previous working day.
"""

from __future__ import annotations

import logging
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol
from urllib.parse import quote

from .storage import GRAPH, GraphStorage, Storage, _join

log = logging.getLogger(__name__)

try:
    from zoneinfo import ZoneInfo
    LOCAL_TZ = ZoneInfo("America/Guatemala")
except Exception:  # pragma: no cover - tzdata missing
    LOCAL_TZ = timezone(timedelta(hours=-6))


@dataclass(frozen=True)
class MailReport:
    kind: str       # file name part: productivity-<kind>-<day>.xlsx
    subject: str    # distinctive part of the subject (accents and case ignored)


# The subject only picks the candidate; the workbook's columns decide (detect_report).
# "Catálogo" flow = Edición (promotes from Compras); "Imagenes y Atributos Compras" = Diseño.
REPORTS = [
    MailReport("edicion", "reporte de productividad diario catalogo"),
    MailReport("diseno", "reporte de productividad diario del flujo imagenes y atributos compras"),
    MailReport("ingresos", "reporte diario productos que ingresaron al flujo de step"),
]
# Bump when the naming rules change so already-seen e-mails are collected again
MAIL_VERSION = 2


def _norm(text: str) -> str:
    text = unicodedata.normalize("NFKD", text or "")
    return " ".join("".join(c for c in text if not unicodedata.combining(c)).lower().split())


def match_report(subject: str) -> MailReport | None:
    s = _norm(subject)
    return next((r for r in REPORTS if r.subject in s), None)


@dataclass(frozen=True)
class Message:
    id: str
    subject: str
    sender: str
    received: datetime


class Mailbox(Protocol):
    def messages_since(self, since: datetime) -> list[Message]: ...
    def xlsx_attachments(self, message_id: str) -> list[tuple[str, bytes]]: ...


class MailPermissionError(RuntimeError):
    pass


class GraphMailbox:
    """A mailbox read through Microsoft Graph with the job's app-only credentials."""

    def __init__(self, graph: GraphStorage, user: str):
        self.graph = graph
        self.base = f"{GRAPH}/users/{quote(user)}"

    def _get(self, url: str, **kw):
        r = self.graph._call("get", url, timeout=60, **kw)
        if r.status_code in (401, 403):
            raise MailPermissionError(
                "Mail.Read (Application) is not granted for the Delta-Catalogo app yet, "
                "or an Exchange access policy blocks this mailbox")
        r.raise_for_status()
        return r

    def messages_since(self, since: datetime) -> list[Message]:
        # Only receivedDateTime in $filter/$orderby: Graph rejects mixed filters as inefficient
        stamp = since.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        url = (f"{self.base}/messages?$filter=receivedDateTime ge {stamp} and hasAttachments eq true"
               f"&$orderby=receivedDateTime desc&$select=id,subject,from,receivedDateTime&$top=100")
        out = []
        while url:
            body = self._get(url).json()
            for m in body.get("value", []):
                out.append(Message(
                    id=m["id"], subject=m.get("subject", ""),
                    sender=(m.get("from") or {}).get("emailAddress", {}).get("address", "").lower(),
                    received=datetime.fromisoformat(m["receivedDateTime"].replace("Z", "+00:00")),
                ))
            url = body.get("@odata.nextLink")
        return out

    def xlsx_attachments(self, message_id: str) -> list[tuple[str, bytes]]:
        url = f"{self.base}/messages/{message_id}/attachments?$select=id,name,size"
        out = []
        for a in self._get(url).json().get("value", []):
            if a.get("@odata.type", "").endswith("fileAttachment") and a["name"].lower().endswith(".xlsx"):
                raw = self._get(f"{self.base}/messages/{message_id}/attachments/{a['id']}/$value")
                out.append((a["name"], raw.content))
        return out


def _kind_from_content(data: bytes) -> str | None:
    import io

    import pandas as pd

    from catalog_core.productivity import detect_report

    try:
        return detect_report(pd.read_excel(io.BytesIO(data), engine="calamine", nrows=0).columns)
    except Exception:
        return None


def collect_reports(mailbox: Mailbox, storage: Storage, incoming_dir: str, seen: dict,
                    sender: str, lookback_days: int, now: datetime | None = None) -> list[str]:
    """
    Save new report attachments to incoming/. `seen` (message id → saved name) is
    updated in place so a message is only downloaded once. Returns saved file names.
    """
    now = now or datetime.now(timezone.utc)
    saved = []
    for m in sorted(mailbox.messages_since(now - timedelta(days=lookback_days)), key=lambda m: m.received):
        report = match_report(m.subject)
        if report is None or m.sender != sender.lower() or m.id in seen:
            continue
        files = mailbox.xlsx_attachments(m.id)
        if not files:
            continue
        day = m.received.astimezone(LOCAL_TZ).strftime("%Y-%m-%d")
        # One workbook per report; if a mail carried several, keep the largest
        _, data = max(files, key=lambda f: len(f[1]))
        kind = _kind_from_content(data) or report.kind
        if kind != report.kind:
            log.warning("Mail: '%s' contains a %s report (by its columns), saving it as %s",
                        m.subject[:60], kind, kind)
        name = f"productivity-{kind}-{day}.xlsx"
        storage.write(_join(incoming_dir, name), data)
        seen[m.id] = {"file": name, "subject": m.subject, "received": m.received.isoformat()}
        saved.append(name)
        log.info("Mail: %s → %s (%s KB)", m.subject[:60], name, len(data) // 1024)
    return saved
