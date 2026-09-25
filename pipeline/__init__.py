"""
Processing job: turns the daily exports in OneDrive `incoming/` into stored results.

    incoming/catalog-daily-YYYY-MM-DD.xlsx          (dropped by the mail automation)
    incoming/productivity-diseno-YYYY-MM-DD*.xlsx
    incoming/productivity-edicion-YYYY-MM-DD*.xlsx
        │  python -m pipeline   (Render cron, every 30 min)
        ▼
    processed/index.json                            what has been processed
    processed/snapshots/YYYY-MM-DD.parquet          full catalog, all columns as text
    processed/runs/YYYY-MM-DD/manifest.json         KPIs, counts, sources, table list
    processed/runs/YYYY-MM-DD/<table>.parquet       every result table
    processed/productivity/YYYY-MM-DD/<team>.parquet
"""
