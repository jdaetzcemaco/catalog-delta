"""Job settings, read from environment variables (Render env vars / a local .env)."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

# v1 history sheet
DEFAULT_SHEET_ID = "1jcL_nEsyMqpzssXFh-0IHpfWKDFtFhzlERzKjcdX69Y"


@dataclass
class Settings:
    # "graph" = OneDrive via Microsoft Graph, "local" = a folder on disk (dev/tests)
    storage: str = "graph"
    local_root: str = "data/onedrive"
    graph_tenant_id: str = ""
    graph_client_id: str = ""
    graph_client_secret: str = ""
    # OneDrive owner (UPN), e.g. the account whose OneDrive holds cemaco-reports/
    graph_drive_user: str = ""

    incoming_dir: str = "cemaco-reports/incoming"
    processed_dir: str = "cemaco-reports/processed"
    catalog_pattern: str = "catalog-daily-*.xlsx"
    diseno_pattern: str = "productivity-diseno-*.xlsx"
    edicion_pattern: str = "productivity-edicion-*.xlsx"
    # On an empty store, only the newest N catalog files are processed
    bootstrap_files: int = 2

    history_enabled: bool = True
    google_sheet_id: str = DEFAULT_SHEET_ID
    google_service_account: dict | None = None

    @classmethod
    def from_env(cls) -> "Settings":
        env = os.environ.get
        sa = env("GCP_SERVICE_ACCOUNT_JSON", "")
        return cls(
            storage=env("STORAGE", "graph"),
            local_root=env("LOCAL_ROOT", "data/onedrive"),
            graph_tenant_id=env("GRAPH_TENANT_ID", ""),
            graph_client_id=env("GRAPH_CLIENT_ID", ""),
            graph_client_secret=env("GRAPH_CLIENT_SECRET", ""),
            graph_drive_user=env("GRAPH_DRIVE_USER", ""),
            incoming_dir=env("INCOMING_DIR", cls.incoming_dir),
            processed_dir=env("PROCESSED_DIR", cls.processed_dir),
            catalog_pattern=env("CATALOG_PATTERN", cls.catalog_pattern),
            diseno_pattern=env("DISENO_PATTERN", cls.diseno_pattern),
            edicion_pattern=env("EDICION_PATTERN", cls.edicion_pattern),
            bootstrap_files=int(env("BOOTSTRAP_FILES", str(cls.bootstrap_files))),
            history_enabled=env("HISTORY_ENABLED", "true").lower() in ("1", "true", "yes"),
            google_sheet_id=env("GOOGLE_SHEET_ID", DEFAULT_SHEET_ID),
            google_service_account=json.loads(sa) if sa else None,
        )
