from __future__ import annotations

import pytest

from pipeline.storage import GraphStorage


class _Resp:
    def __init__(self, status: int, body: dict | None = None):
        self.status_code, self._body = status, body or {}

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise AssertionError(f"unexpected HTTP {self.status_code}")


class FakeGraph:
    """Answers token, drive and folder requests for one known user and folder."""

    def __init__(self, user: str, folders: dict[str, list[dict]]):
        self.user, self.folders = user, folders

    def post(self, url, **kw):
        return _Resp(200, {"access_token": "t", "expires_in": 3600})

    def get(self, url, **kw):
        if f"/users/{self.user}/" not in url + "/":
            return _Resp(404)
        for folder, items in self.folders.items():
            if f"root:/{folder}:/children" in url:
                return _Resp(200, {"value": items})
            if f"root:/{folder}:" in url:
                return _Resp(200, {"folder": {}})
        if "root:" in url:
            return _Resp(404)
        return _Resp(200, {"driveType": "business", "owner": {"user": {"displayName": "Juan"}}})


FILE = {"name": "catalog-daily-2026-04-01.xlsx", "size": 5, "lastModifiedDateTime": "2026-04-01T12:00:00Z", "file": {}}


def _storage(user="juan@cemaco.com"):
    graph = FakeGraph("juan%40cemaco.com", {"cemaco-reports/incoming": [FILE, {"name": "sub", "folder": {}}]})
    return GraphStorage("tenant", "client", "secret", user, session=graph)


def test_lists_files_only():
    files = _storage().list("cemaco-reports/incoming", "catalog-daily-*.xlsx")
    assert [f.name for f in files] == ["catalog-daily-2026-04-01.xlsx"]


def test_wrong_account_fails_loudly():
    with pytest.raises(FileNotFoundError, match="No OneDrive"):
        _storage("CUENTA@cemaco.com").list("cemaco-reports/incoming")


def test_missing_folder_fails_loudly():
    s = _storage()
    assert not s.folder_exists("cemaco-reports/typo")
    with pytest.raises(FileNotFoundError, match="not found"):
        s.list("cemaco-reports/typo")


def test_missing_settings_are_named():
    with pytest.raises(ValueError, match="GRAPH_CLIENT_SECRET"):
        GraphStorage("t", "c", "", "u")


class FlakyGraph(FakeGraph):
    """Drops the connection or throttles the first calls, then answers normally."""

    def __init__(self, *a, failures):
        super().__init__(*a)
        self.failures = list(failures)
        self.calls = 0

    def get(self, url, **kw):
        self.calls += 1
        if self.failures:
            f = self.failures.pop(0)
            if isinstance(f, Exception):
                raise f
            return _Resp(f)
        return super().get(url, **kw)


def _flaky(failures):
    import requests
    graph = FlakyGraph("juan%40cemaco.com", {"cemaco-reports/incoming": [FILE]}, failures=failures)
    s = GraphStorage("tenant", "client", "secret", "juan@cemaco.com", session=graph)
    s._sleep = lambda seconds: None
    return s, graph, requests


def test_dropped_connection_and_throttling_are_retried():
    s, graph, requests = _flaky([])
    graph.failures = [requests.exceptions.ChunkedEncodingError("reset by peer"), 429, 503]
    assert [f.name for f in s.list("cemaco-reports/incoming")] == ["catalog-daily-2026-04-01.xlsx"]
    assert graph.calls == 4


def test_gives_up_after_repeated_failures():
    s, graph, requests = _flaky([])
    graph.failures = [requests.ConnectionError("down")] * 10
    with pytest.raises(requests.ConnectionError):
        s.list("cemaco-reports/incoming")
    assert graph.calls == 5
