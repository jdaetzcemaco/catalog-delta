"""
Read-only OneDrive connection check: lists the incoming folder. Writes nothing.

    export GRAPH_TENANT_ID=... GRAPH_CLIENT_ID=... GRAPH_DRIVE_USER=someone@cemaco.com
    read -s GRAPH_CLIENT_SECRET && export GRAPH_CLIENT_SECRET
    python tools/check_onedrive.py
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests  # noqa: E402

from pipeline.config import Settings  # noqa: E402
from pipeline.storage import GraphStorage  # noqa: E402


def main() -> int:
    s = Settings.from_env()
    try:
        storage = GraphStorage(s.graph_tenant_id, s.graph_client_id, s.graph_client_secret, s.graph_drive_user)
        drive = storage.drive_info()
        owner = drive.get("owner", {}).get("user", {}).get("displayName", "?")
        print(f"Signed in to Microsoft Graph. OneDrive of {owner} ({drive.get('webUrl', '')})")
        if not storage.folder_exists(s.incoming_dir):
            print(f"✗ Folder '{s.incoming_dir}' does not exist in this OneDrive.")
            print("→ Check GRAPH_DRIVE_USER (is it the account whose OneDrive has cemaco-reports?) and INCOMING_DIR.")
            return 1
        files = storage.list(s.incoming_dir)
    except ValueError as exc:
        print(f"Config: {exc}")
        return 2
    except FileNotFoundError as exc:
        print(f"✗ {exc}")
        print("→ GRAPH_DRIVE_USER must be the real email of the account whose OneDrive holds cemaco-reports.")
        return 1
    except requests.HTTPError as exc:
        body = exc.response.text[:500] if exc.response is not None else ""
        print(f"Graph error {exc.response.status_code if exc.response is not None else ''}: {body}")
        if "AADSTS7000215" in body:
            print("→ The secret is wrong: use the secret's *Value* (shown once when created), not its Secret ID.")
        elif exc.response is not None and exc.response.status_code in (401, 403):
            print("→ Usually: admin consent not granted yet, wrong secret value, or wrong GRAPH_DRIVE_USER.")
        return 1

    print(f"✓ Folder '{s.incoming_dir}': {len(files)} files")
    for f in sorted(files, key=lambda f: f.modified, reverse=True)[:20]:
        print(f"  {f.modified:%Y-%m-%d %H:%M}  {f.size / 1e6:8.1f} MB  {f.name}")
    patterns = {"catalog": s.catalog_pattern, "diseno": s.diseno_pattern, "edicion": s.edicion_pattern}
    import fnmatch
    for kind, pat in patterns.items():
        n = sum(fnmatch.fnmatch(f.name, pat) for f in files)
        print(f"  matches {pat!r}: {n}" + ("" if n else "   ← no file matches this name pattern"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
