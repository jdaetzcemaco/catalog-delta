# Processing job (`python -m pipeline`)

Runs every 30 minutes as a Render cron job. Each pass processes whatever is new in
OneDrive `cemaco-reports/incoming/` and exits; a pass with nothing new takes under a second.

## What it does

1. Lists `catalog-daily-*.xlsx`. The date comes from the file name (else the modified
   date, Guatemala time). A file is processed when its name, size or modified time is
   new, so a re-sent export replaces that day.
2. For each new day, oldest first: reads the `SKUs` sheet, compares it with the most
   recent earlier snapshot, and writes to `cemaco-reports/processed/`:
   - `snapshots/<day>.parquet`: the full catalog (about 33 MB/day)
   - `runs/<day>/manifest.json`: KPIs, counts, which day it was compared with
   - `runs/<day>/*.parquet`: every change and inventory table, plus `sku_info`
   - the Google Sheets history row for that day (updated if the day is re-run)
3. If a day arrives late, the day after it is recomputed so its comparison is right.
4. Stores the newest `productivity-diseno-*` / `productivity-edicion-*` per day under
   `productivity/<day>/`.
5. On an empty store it starts from the newest `BOOTSTRAP_FILES` catalog files (2 by
   default, 30 on Render). After that it only considers days from the oldest processed
   day onward, so old exports in `incoming/` are never picked up by accident; use
   `python -m pipeline --backfill N` to go further back on purpose.

Measured on the real exports (193,661 and 203,595 SKUs): 18 s and 1.8 GB peak memory for
both days. Use an instance with at least 4 GB for the cron job.

## Settings (environment variables)

| Variable | Value |
|---|---|
| `STORAGE` | `graph` (OneDrive) or `local` |
| `GRAPH_TENANT_ID`, `GRAPH_CLIENT_ID`, `GRAPH_CLIENT_SECRET` | Entra ID app registration (see below) |
| `GRAPH_DRIVE_USER` | UPN of the OneDrive that holds `cemaco-reports/` |
| `GCP_SERVICE_ACCOUNT_JSON` | Google service-account key JSON (one line); the sheet must be shared with its email |
| `GOOGLE_SHEET_ID` | defaults to the v1 history sheet |
| `INCOMING_DIR`, `PROCESSED_DIR`, `*_PATTERN`, `BOOTSTRAP_FILES`, `HISTORY_ENABLED` | optional overrides |

### OneDrive access (IT)

v1 used SharePoint app-only "ACS" client secrets, which Microsoft retired in April 2026.
The job uses Microsoft Graph instead:

1. Entra ID → App registrations → New registration (single tenant).
2. API permissions → Microsoft Graph → **Application** → `Files.ReadWrite.All`
   → Grant admin consent. (Stricter option: `Sites.Selected`, then grant the app write
   access to that one OneDrive site.)
3. Certificates & secrets → New client secret. Give tenant ID, client ID and secret to
   whoever configures Render.

## Running locally

```bash
python -m pipeline --local data/onedrive --no-history      # folder laid out like OneDrive
python -m pipeline --recompute 2026-03-31 --local data/onedrive --no-history
python -m pipeline --backfill 30 --no-history                  # real OneDrive, newest 30 exports
```
