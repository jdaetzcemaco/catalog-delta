"""Job settings, read from environment variables (Render env vars / a local .env)."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field

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


def _key_json(value: str, path: str) -> str:
    """A service-account key given inline (JSON text), or as a path to the key file."""
    value = (value or "").strip()
    if value and not value.startswith("{") and os.path.isfile(value):
        path = value
        value = ""
    if not value and path and os.path.isfile(path):
        with open(path, encoding="utf-8") as fh:
            value = fh.read()
    return value


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
    # Campaign lists written by the app (JSON with name + SKUs); the job ingests them
    lists_dir: str = "cemaco-reports/incoming/lists"
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
    # One Google service account for GA4, Search Console and the history sheet. Either the
    # key JSON itself (GOOGLE_SERVICE_ACCOUNT_JSON, or legacy GCP_SERVICE_ACCOUNT_JSON) or a
    # path to the key file (GOOGLE_APPLICATION_CREDENTIALS, e.g. a Render secret file).
    google_service_account: dict | None = None
    ga4_property_id: str = ""
    gsc_site: str = "sc-domain:cemaco.com"
    ga4_enabled: bool = True
    ga4_backfill_days: int = 90
    ga4_refresh_days: int = 3          # re-pull the last N days (GA4 closes a day after 24–48 h)
    # Search Console through its BigQuery bulk export (query-level data, ~8x the API's coverage).
    # Days >= gsc_bq_start come from BigQuery when credentials allow; earlier days from the API.
    gsc_bq_enabled: bool = True
    gsc_bq_project: str = "cemaco-content-system"
    gsc_bq_dataset: str = "searchconsole"
    gsc_bq_table: str = "searchdata_url_impression"
    gsc_bq_billing_project: str = ""   # project that runs the jobs; empty = gsc_bq_project
    gsc_bq_start: str = "2026-08-27"   # first day in the export
    # Optional second key (GSC_BQ_SERVICE_ACCOUNT_JSON); falls back to google_service_account
    gsc_bq_service_account: dict | None = None
    seo_window_days: int = 14          # before/after window of the SEO impact cohorts

    # VTEX (read-only checks: stock/price per SKU through the public search API)
    vtex_account: str = ""
    vtex_app_key: str = ""
    vtex_app_token: str = ""

    # Alert thresholds (catalog_core.alerts.AlertThresholds), overridable as ALERT_<FIELD>
    alert_thresholds: dict = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        env = os.environ.get
        sa = _key_json(env("GOOGLE_SERVICE_ACCOUNT_JSON", "") or env("GCP_SERVICE_ACCOUNT_JSON", ""),
                       env("GOOGLE_APPLICATION_CREDENTIALS", ""))
        bq_sa = _key_json(env("GSC_BQ_SERVICE_ACCOUNT_JSON", ""), "")
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
            lists_dir=env("LISTS_DIR", cls.lists_dir),
            mail_enabled=env("MAIL_ENABLED", "true").lower() in ("1", "true", "yes"),
            mail_user=env("MAIL_USER", ""),
            mail_sender=env("MAIL_SENDER", cls.mail_sender),
            mail_lookback_days=int(env("MAIL_LOOKBACK_DAYS", str(cls.mail_lookback_days))),
            bootstrap_files=int(env("BOOTSTRAP_FILES", str(cls.bootstrap_files))),
            history_enabled=env("HISTORY_ENABLED", "true").lower() in ("1", "true", "yes"),
            google_sheet_id=env("GOOGLE_SHEET_ID", DEFAULT_SHEET_ID),
            google_service_account=json.loads(sa) if sa else None,
            ga4_property_id=env("GA4_PROPERTY_ID", ""),
            gsc_site=env("GSC_SITE", cls.gsc_site),
            ga4_enabled=env("GA4_ENABLED", "true").lower() in ("1", "true", "yes"),
            ga4_backfill_days=int(env("GA4_BACKFILL_DAYS", str(cls.ga4_backfill_days))),
            ga4_refresh_days=int(env("GA4_REFRESH_DAYS", str(cls.ga4_refresh_days))),
            gsc_bq_enabled=env("GSC_BQ_ENABLED", "true").lower() in ("1", "true", "yes"),
            gsc_bq_project=env("GSC_BQ_PROJECT", cls.gsc_bq_project),
            gsc_bq_dataset=env("GSC_BQ_DATASET", cls.gsc_bq_dataset),
            gsc_bq_table=env("GSC_BQ_TABLE", cls.gsc_bq_table),
            gsc_bq_billing_project=env("GSC_BQ_BILLING_PROJECT", ""),
            gsc_bq_start=env("GSC_BQ_START", cls.gsc_bq_start),
            gsc_bq_service_account=json.loads(bq_sa) if bq_sa else None,
            seo_window_days=int(env("SEO_WINDOW_DAYS", str(cls.seo_window_days))),
            vtex_account=env("VTEX_ACCOUNT", ""),
            vtex_app_key=env("VTEX_APP_KEY", ""),
            vtex_app_token=env("VTEX_APP_TOKEN", ""),
            alert_thresholds={k[6:].lower(): float(v) for k, v in os.environ.items()
                              if k.startswith("ALERT_") and v.strip()},
        )
