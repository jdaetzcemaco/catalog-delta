from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipeline.mail import GraphMailbox, MailPermissionError, Message, collect_reports, match_report
from pipeline.storage import GraphStorage, LocalStorage

SENDER = "noreply@cloudmail.stibo.com"
NOW = datetime(2026, 9, 29, 16, 0, tzinfo=timezone.utc)


def at(hour):  # 13:30 UTC = 07:30 Guatemala
    return datetime(2026, 9, 29, hour, 30, tzinfo=timezone.utc)


class FakeMailbox:
    def __init__(self, messages, attachments):
        self.messages, self.attachments, self.downloads = messages, attachments, 0

    def messages_since(self, since):
        return [m for m in self.messages if m.received >= since]

    def xlsx_attachments(self, message_id):
        self.downloads += 1
        return self.attachments.get(message_id, [])


@pytest.mark.parametrize("subject,kind", [
    ("[STEP] Reporte de Productividad Diario Catálogo", "edicion"),
    ("[STEP] Reporte de Productividad Diario del flujo Imagenes y Atributos Compras", "diseno"),
    ("STEP - Reporte Diario Productos que Ingresaron al flujo de STEP", "ingresos"),
    ("RE: [STEP] reporte de productividad diario CATALOGO", "edicion"),
    ("[STEP] Otro reporte", None),
])
def test_subject_identifies_report(subject, kind):
    r = match_report(subject)
    assert (r.kind if r else None) == kind


def _mail():
    msgs = [
        Message("m1", "[STEP] Reporte de Productividad Diario Catálogo", SENDER, at(13)),
        Message("m2", "[STEP] Reporte de Productividad Diario del flujo Imagenes y Atributos Compras", SENDER, at(13)),
        Message("m3", "STEP - Reporte Diario Productos que Ingresaron al flujo de STEP", SENDER, at(13)),
        Message("m4", "[STEP] Reporte de Productividad Diario Catálogo", "phish@example.com", at(14)),
        Message("m5", "Factura", SENDER, at(14)),
    ]
    files = {m: [("excel-2026-09-29_07.30.00.xlsx", f"data-{m}".encode())] for m in ["m1", "m2", "m3", "m4", "m5"]}
    return FakeMailbox(msgs, files)


def test_saves_each_report_under_its_own_name_once(tmp_path):
    storage, box, seen = LocalStorage(str(tmp_path)), _mail(), {}
    saved = collect_reports(box, storage, "incoming", seen, SENDER, 7, now=NOW)
    assert sorted(saved) == ["productivity-diseno-2026-09-29.xlsx", "productivity-edicion-2026-09-29.xlsx",
                             "productivity-ingresos-2026-09-29.xlsx"]
    # "Catálogo" is the Edición report, "Imagenes y Atributos Compras" the Diseño one
    assert storage.read("incoming/productivity-edicion-2026-09-29.xlsx") == b"data-m1"
    assert storage.read("incoming/productivity-diseno-2026-09-29.xlsx") == b"data-m2"
    # Other senders and subjects are ignored; a second pass downloads nothing
    downloads = box.downloads
    assert collect_reports(box, storage, "incoming", seen, SENDER, 7, now=NOW) == []
    assert box.downloads == downloads


def test_day_is_the_guatemala_date_the_mail_arrived(tmp_path):
    # 03:00 UTC on the 30th is still the 29th in Guatemala
    msg = Message("late", "[STEP] Reporte de Productividad Diario Catálogo", SENDER,
                  datetime(2026, 9, 30, 3, 0, tzinfo=timezone.utc))
    box = FakeMailbox([msg], {"late": [("excel.xlsx", b"x")]})
    saved = collect_reports(box, LocalStorage(str(tmp_path)), "incoming", {}, SENDER, 7,
                            now=datetime(2026, 9, 30, 4, tzinfo=timezone.utc))
    assert saved == ["productivity-edicion-2026-09-29.xlsx"]


def test_columns_decide_over_the_subject(tmp_path):
    import io

    import pandas as pd

    buf = io.BytesIO()
    pd.DataFrame({"<ID>": [1], "Usuario Promueve desde Catalogo": ["ana"]}).to_excel(buf, index=False)
    # Subject says Catálogo (Edición) but the workbook is a Diseño report
    msg = Message("x", "[STEP] Reporte de Productividad Diario Catálogo", SENDER, at(13))
    box = FakeMailbox([msg], {"x": [("excel.xlsx", buf.getvalue())]})
    saved = collect_reports(box, LocalStorage(str(tmp_path)), "incoming", {}, SENDER, 7, now=NOW)
    assert saved == ["productivity-diseno-2026-09-29.xlsx"]


class _Resp:
    def __init__(self, status, body=None, content=b""):
        self.status_code, self._body, self.content = status, body or {}, content

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise AssertionError(self.status_code)


class FakeGraph:
    def __init__(self, status=200):
        self.status = status

    def post(self, url, **kw):
        return _Resp(200, {"access_token": "t", "expires_in": 3600})

    def get(self, url, **kw):
        if self.status != 200:
            return _Resp(self.status)
        if url.endswith("/$value"):
            return _Resp(200, content=b"xlsx-bytes")
        if "/attachments" in url:
            return _Resp(200, {"value": [
                {"@odata.type": "#microsoft.graph.fileAttachment", "id": "a1", "name": "excel-2026.xlsx"},
                {"@odata.type": "#microsoft.graph.fileAttachment", "id": "a2", "name": "logo.png"},
            ]})
        assert "receivedDateTime ge 2026-09-22T16:00:00Z" in url
        return _Resp(200, {"value": [{
            "id": "m1", "subject": "STEP - Reporte Diario Productos que Ingresaron al flujo de STEP",
            "from": {"emailAddress": {"address": "NoReply@cloudmail.stibo.com"}},
            "receivedDateTime": "2026-09-29T13:00:12Z",
        }]})


def test_graph_mailbox_reads_messages_and_xlsx_attachments(tmp_path):
    box = GraphMailbox(GraphStorage("t", "c", "s", "jdaetz@cemaco.com", session=FakeGraph()), "jdaetz@cemaco.com")
    saved = collect_reports(box, LocalStorage(str(tmp_path)), "incoming", {}, SENDER, 7, now=NOW)
    assert saved == ["productivity-ingresos-2026-09-29.xlsx"]
    assert (tmp_path / "incoming" / "productivity-ingresos-2026-09-29.xlsx").read_bytes() == b"xlsx-bytes"


def test_missing_permission_is_a_clear_error():
    box = GraphMailbox(GraphStorage("t", "c", "s", "u@cemaco.com", session=FakeGraph(403)), "u@cemaco.com")
    with pytest.raises(MailPermissionError, match="Mail.Read"):
        box.messages_since(NOW)
