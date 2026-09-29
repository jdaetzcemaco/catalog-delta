"""Job settings, read from environment variables (Render env vars / a local .env)."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

# v1 history sheet
DEFAULT_SHEET_ID = "1jcL_nEsyMqpzssXFh-0IHpfWKDFtFhzlERzKjcdX69Y"


def load_dotenv(path: str = ".env") -> None:
    """
    Local runs only: KEY=VALUE lines from a git-ignored .env fill variables that are
    not already set. Render never has this file and uses its own environment.
    """
    if not os.path.isfile(path):
        return
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


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
    ingresos_pattern: str = "productivity-ingresos-*.xlsx"
    # On an empty store, only the newest N catalog files are processed
    bootstrap_files: int = 2

    # Read the STEP report e-mails (productivity, ingresos) straight from a mailbox.
    # Needs the Graph Mail.Read application permission; without it the job only warns.
    mail_enabled: bool = True
    mail_user: str = ""            # defaults to graph_drive_user
    mail_sender: str = "noreply@cloudmail.stibo.com"
    mail_lookback_days: int = 7

    history_enabled: bool = True
    google_sheet_id: str = DEFAULT_SHEET_ID
    google_service_account: dict | None = None

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
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
            ingresos_pattern=env("INGRESOS_PATTERN", cls.ingresos_pattern),
            mail_enabled=env("MAIL_ENABLED", "true").lower() in ("1", "true", "yes"),
            mail_user=env("MAIL_USER", ""),
            mail_sender=env("MAIL_SENDER", cls.mail_sender),
            mail_lookback_days=int(env("MAIL_LOOKBACK_DAYS", str(cls.mail_lookback_days))),
            bootstrap_files=int(env("BOOTSTRAP_FILES", str(cls.bootstrap_files))),
            history_enabled=env("HISTORY_ENABLED", "true").lower() in ("1", "true", "yes"),
            google_sheet_id=env("GOOGLE_SHEET_ID", DEFAULT_SHEET_ID),
            google_service_account=json.loads(sa) if sa else None,
        )
