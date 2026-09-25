# Deploying on Render

Two services from one Blueprint (`render.yaml`), in a new Render project:

| Service | Type | Plan | What it does |
|---|---|---|---|
| `catalog-delta-job` | Cron, every 30 min | Pro (4 GB) | `python -m pipeline`: processes new files in OneDrive `incoming/` |
| `catalog-delta-web` | Web | Standard (2 GB) | The Streamlit app; reads stored results |

Both use Python 3.12 and the pinned `requirements.txt` (verified with the full parity run).

## Before you start

- OneDrive connection works locally: `python3 tools/check_onedrive.py` prints `OK` and
  the catalog files match `catalog-daily-*.xlsx` (see docs/PIPELINE.md).
- The Entra app **Delta-Catalogo** has `Files.ReadWrite.All` (Application) with admin
  consent, and you have the client secret **value**.
- Optional: the Google service-account JSON for the history sheet. Without it the job
  logs a warning and skips the sheet; everything else, including the app's Historial
  page, works.

## Create the services

1. Render → **New → Blueprint** → connect `jdaetzcemaco/catalog-delta`, branch `rebuild`
   (switch to `main` after merging).
2. Render asks for each `sync: false` value:

   | Variable | Job | Web | Value |
   |---|---|---|---|
   | `GRAPH_DRIVE_USER` | ✓ | ✓ | account whose OneDrive holds `cemaco-reports/` |
   | `GRAPH_CLIENT_SECRET` | ✓ | ✓ | client secret **value** |
   | `GCP_SERVICE_ACCOUNT_JSON` | ✓ | | whole key file on one line (or leave empty for now) |
   | `APP_PASSWORD` | | ✓ | password for the app login |
   | `RENDER_API_KEY` | | ✓ | optional, see below |
   | `RENDER_CRON_JOB_ID` | | ✓ | optional, the job's ID (`crn-…`, in its URL) |

3. **Apply**. When the job's first build finishes, open it and click **Trigger Run** instead
   of waiting 30 minutes. The first run on an empty store processes the newest 2 catalog
   files (about 20 s each) and all productivity files. Check its logs for `Done: … errors=0`.
4. Open the web service URL, log in, and check that Resumen shows the latest day.

## Manual uploads

Uploading on **Cargar archivos** saves the file to OneDrive `incoming/`. Then:

- with `RENDER_API_KEY` + `RENDER_CRON_JOB_ID`: the app starts a job run right away
  (about a minute);
- without them: the next scheduled run (≤ 30 min) picks it up.

`RENDER_API_KEY` is an account-level key (Account settings → API keys) with access to the
whole Render account. Leave it empty if that is not acceptable.

## After it works

- Merge `rebuild` into `main` and point the Blueprint at `main`.
- Retire the old Streamlit Cloud app and the old `catalog-delta-api` Render service, if
  still running (it wrote approximate numbers to the history sheet; see docs/CHANGES.md F1).
- Rotate the client secret before it expires (Entra → Certificates & secrets), then update
  it in both services.

## Troubleshooting

| Symptom | Where to look |
|---|---|
| App says "Aún no hay resultados" | Job logs: did a run finish with `errors=0`? |
| Job log `401 invalid_client` | Wrong secret (use the Value, not the Secret ID) |
| Job log `403` / `accessDenied` | Admin consent missing, or wrong `GRAPH_DRIVE_USER` |
| Job log `No new catalog files` but a file is there | File name does not match `CATALOG_PATTERN` |
| Job killed / out of memory | Job plan must be Pro (4 GB) or larger |
