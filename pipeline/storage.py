"""
File storage behind one small interface: OneDrive (Microsoft Graph) or a local folder.

Graph uses app-only client credentials (an Entra ID app registration with the
Files.ReadWrite.All application permission, or Sites.Selected on the OneDrive site).
The SharePoint "ACS" client-secret login used by v1 was retired by Microsoft in 2026.
"""

from __future__ import annotations

import fnmatch
import os
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol
from urllib.parse import quote

import requests

GRAPH = "https://graph.microsoft.com/v1.0"
SIMPLE_UPLOAD_MAX = 4 * 1024 * 1024
CHUNK = 320 * 1024 * 32  # 10 MiB, a multiple of 320 KiB as Graph requires


@dataclass(frozen=True)
class FileInfo:
    name: str
    path: str          # path relative to the storage root, "/"-separated
    modified: datetime
    size: int


class Storage(Protocol):
    def list(self, folder: str, pattern: str = "*") -> list[FileInfo]: ...
    def read(self, path: str) -> bytes | None: ...
    def download_to(self, path: str, local_path: str) -> None: ...
    def write(self, path: str, data: bytes) -> None: ...


def _join(*parts: str) -> str:
    return "/".join(p.strip("/") for p in parts if p)


# ── Local folder ────────────────────────────────────────────────────────────
class LocalStorage:
    def __init__(self, root: str):
        self.root = os.path.abspath(root)

    def _abs(self, path: str) -> str:
        return os.path.join(self.root, *path.split("/"))

    def list(self, folder: str, pattern: str = "*") -> list[FileInfo]:
        base = self._abs(folder)
        if not os.path.isdir(base):
            return []
        out = []
        for name in os.listdir(base):
            full = os.path.join(base, name)
            if os.path.isfile(full) and fnmatch.fnmatch(name, pattern):
                st = os.stat(full)
                out.append(FileInfo(
                    name=name, path=_join(folder, name),
                    modified=datetime.fromtimestamp(st.st_mtime, tz=timezone.utc), size=st.st_size,
                ))
        return out

    def read(self, path: str) -> bytes | None:
        try:
            with open(self._abs(path), "rb") as fh:
                return fh.read()
        except FileNotFoundError:
            return None

    def download_to(self, path: str, local_path: str) -> None:
        shutil.copyfile(self._abs(path), local_path)

    def write(self, path: str, data: bytes) -> None:
        full = self._abs(path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        tmp = full + ".tmp"
        with open(tmp, "wb") as fh:
            fh.write(data)
        os.replace(tmp, full)


# ── OneDrive via Microsoft Graph ────────────────────────────────────────────
class GraphStorage:
    def __init__(self, tenant_id: str, client_id: str, client_secret: str, drive_user: str,
                 session: requests.Session | None = None):
        missing = [n for n, v in [("GRAPH_TENANT_ID", tenant_id), ("GRAPH_CLIENT_ID", client_id),
                                  ("GRAPH_CLIENT_SECRET", client_secret), ("GRAPH_DRIVE_USER", drive_user)] if not v]
        if missing:
            raise ValueError(f"Missing OneDrive settings: {', '.join(missing)}")
        self.tenant_id, self.client_id, self.client_secret = tenant_id, client_id, client_secret
        self.drive = f"{GRAPH}/users/{quote(drive_user)}/drive"
        self.http = session or requests.Session()
        self._token, self._token_exp = "", 0.0

    def _headers(self) -> dict:
        if time.time() > self._token_exp - 60:
            r = self.http.post(
                f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token",
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "scope": "https://graph.microsoft.com/.default",
                },
                timeout=30,
            )
            r.raise_for_status()
            body = r.json()
            self._token, self._token_exp = body["access_token"], time.time() + int(body.get("expires_in", 3600))
        return {"Authorization": f"Bearer {self._token}"}

    def _item(self, path: str) -> str:
        return f"{self.drive}/root:/{quote(path.strip('/'))}:"

    def list(self, folder: str, pattern: str = "*") -> list[FileInfo]:
        url = f"{self._item(folder)}/children?$select=name,size,lastModifiedDateTime,file&$top=999"
        out = []
        while url:
            r = self.http.get(url, headers=self._headers(), timeout=60)
            if r.status_code == 404:
                return []
            r.raise_for_status()
            body = r.json()
            for it in body.get("value", []):
                if "file" in it and fnmatch.fnmatch(it["name"], pattern):
                    out.append(FileInfo(
                        name=it["name"], path=_join(folder, it["name"]),
                        modified=datetime.fromisoformat(it["lastModifiedDateTime"].replace("Z", "+00:00")),
                        size=int(it.get("size", 0)),
                    ))
            url = body.get("@odata.nextLink")
        return out

    def read(self, path: str) -> bytes | None:
        r = self.http.get(f"{self._item(path)}/content", headers=self._headers(), timeout=300)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.content

    def download_to(self, path: str, local_path: str) -> None:
        # Stream to disk: catalog exports are 100+ MB
        with self.http.get(f"{self._item(path)}/content", headers=self._headers(), timeout=600, stream=True) as r:
            r.raise_for_status()
            with open(local_path, "wb") as fh:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    fh.write(chunk)

    def write(self, path: str, data: bytes) -> None:
        # Uploading by path creates missing folders
        if len(data) <= SIMPLE_UPLOAD_MAX:
            r = self.http.put(f"{self._item(path)}/content", headers=self._headers(), data=data, timeout=300)
            r.raise_for_status()
            return
        r = self.http.post(
            f"{self._item(path)}/createUploadSession", headers=self._headers(),
            json={"item": {"@microsoft.graph.conflictBehavior": "replace"}}, timeout=60,
        )
        r.raise_for_status()
        upload_url, total = r.json()["uploadUrl"], len(data)
        for start in range(0, total, CHUNK):
            end = min(start + CHUNK, total) - 1
            # The pre-authenticated upload URL must not get the Authorization header
            r = self.http.put(upload_url, data=data[start:end + 1], timeout=300, headers={
                "Content-Length": str(end - start + 1),
                "Content-Range": f"bytes {start}-{end}/{total}",
            })
            r.raise_for_status()


def make_storage(settings) -> Storage:
    if settings.storage == "local":
        return LocalStorage(settings.local_root)
    if settings.storage == "graph":
        return GraphStorage(settings.graph_tenant_id, settings.graph_client_id,
                            settings.graph_client_secret, settings.graph_drive_user)
    raise ValueError(f"Unknown STORAGE '{settings.storage}' (use 'graph' or 'local')")
