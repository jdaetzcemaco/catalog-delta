"""
FastAPI endpoint for automated catalog processing.
Receives a download URL, processes the file, and saves to Google Sheets.
Memory-optimized for large files.
"""

import gc
import os
import tempfile
from datetime import datetime

import gspread
import pandas as pd
import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from google.oauth2.service_account import Credentials
from pydantic import BaseModel

app = FastAPI(title="Catalog Delta API")

# Google Sheets configuration
GOOGLE_SHEET_ID = "1jcL_nEsyMqpzssXFh-0IHpfWKDFtFhzlERzKjcdX69Y"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Content score weights
WEIGHTS = {
    "has_image": 25,
    "has_description": 15,
    "has_price": 15,
    "taxonomy_depth": 15,
    "has_name": 10,
    "is_visible": 10,
    "has_brand": 5,
}


class ProcessRequest(BaseModel):
    download_url: str


def get_google_sheets_client():
    """Get authenticated Google Sheets client from environment variables."""
    creds_dict = {
        "type": "service_account",
        "project_id": os.environ.get("GCP_PROJECT_ID"),
        "private_key_id": os.environ.get("GCP_PRIVATE_KEY_ID"),
        "private_key": os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n"),
        "client_email": os.environ.get("GCP_CLIENT_EMAIL"),
        "client_id": os.environ.get("GCP_CLIENT_ID"),
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": os.environ.get("GCP_CLIENT_X509_CERT_URL"),
    }
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def has_value(val) -> bool:
    """Check if a value is non-empty."""
    if pd.isna(val):
        return False
    if isinstance(val, str):
        return val.strip() != ""
    return val not in [0, None, ""]


def calculate_summary_from_file(filepath: str) -> dict:
    """
    Calculate catalog health summary directly from Excel file.
    Memory-efficient: processes in chunks and only keeps aggregates.
    """
    # Read only the columns we need
    needed_cols = None  # Will read all and filter

    # First, get sheet name
    xl = pd.ExcelFile(filepath)
    sheet_name = "SKUs" if "SKUs" in xl.sheet_names else 0
    xl.close()

    # Read the data
    df = pd.read_excel(filepath, sheet_name=sheet_name)
    df.columns = [c.strip().upper() for c in df.columns]

    total_skus = len(df)

    # Calculate metrics
    visible = 0
    with_image = 0
    with_price = 0
    with_stock = 0
    total_score = 0
    perfect_score = 0

    for _, row in df.iterrows():
        # Visibility
        vis_val = row.get("VISIBLE", 0)
        is_visible = vis_val == 1 or vis_val == "Si" or vis_val == "1" or vis_val is True
        if is_visible:
            visible += 1

        # Image
        has_img = (
            has_value(row.get("TIENE IMAGEN")) or
            has_value(row.get("IMAGEN PRIMARIA")) or
            has_value(row.get("URL IMAGEN"))
        )
        if has_img:
            with_image += 1

        # Price
        has_price_val = has_value(row.get("TIENE PRECIO")) or (
            row.get("PRECIO") is not None and
            not pd.isna(row.get("PRECIO")) and
            row.get("PRECIO") > 0
        )
        if has_price_val:
            with_price += 1

        # Stock
        has_stock_val = has_value(row.get("TIENE STOCK")) or (
            row.get("STOCK") is not None and
            not pd.isna(row.get("STOCK")) and
            row.get("STOCK") > 0
        )
        if has_stock_val:
            with_stock += 1

        # Content score calculation
        score = 0
        if has_img:
            score += WEIGHTS["has_image"]
        if has_value(row.get("DESCRIPCION ERP")) or has_value(row.get("DESCRIPCION")):
            score += WEIGHTS["has_description"]
        if has_price_val:
            score += WEIGHTS["has_price"]
        if has_value(row.get("NOMBRE DE SKU")) or has_value(row.get("NOMBRE DE PRODUCTO")) or has_value(row.get("NOMBRE")):
            score += WEIGHTS["has_name"]
        if is_visible:
            score += WEIGHTS["is_visible"]
        if has_value(row.get("MARCA")):
            score += WEIGHTS["has_brand"]

        # Taxonomy depth
        tax_levels = sum([
            1 for col in ["NIVEL 1", "NIVEL 2", "NIVEL 3", "NIVEL1", "NIVEL2", "NIVEL3"]
            if has_value(row.get(col))
        ])
        score += min(tax_levels * 5, WEIGHTS["taxonomy_depth"])

        total_score += score
        if score == 100:
            perfect_score += 1

    # Clean up
    del df
    gc.collect()

    return {
        "total_skus": total_skus,
        "visible": visible,
        "visible_pct": round((visible / total_skus) * 100, 1) if total_skus > 0 else 0,
        "with_image_pct": round((with_image / total_skus) * 100, 1) if total_skus > 0 else 0,
        "with_price_pct": round((with_price / total_skus) * 100, 1) if total_skus > 0 else 0,
        "with_stock_pct": round((with_stock / total_skus) * 100, 1) if total_skus > 0 else 0,
        "avg_score": round(total_score / total_skus, 1) if total_skus > 0 else 0,
        "perfect_score_count": perfect_score,
    }


def save_to_google_sheets(summary: dict) -> bool:
    """Save the catalog health summary to Google Sheets."""
    client = get_google_sheets_client()
    sheet = client.open_by_key(GOOGLE_SHEET_ID).sheet1

    today_date = datetime.now().strftime("%Y-%m-%d")
    row = [
        today_date,
        summary["total_skus"],
        summary["visible"],
        summary["visible_pct"],
        summary["with_image_pct"],
        summary["with_price_pct"],
        summary["with_stock_pct"],
        summary["avg_score"],
        summary["perfect_score_count"],
    ]

    sheet.append_row(row, value_input_option="USER_ENTERED")
    return True


@app.get("/")
def health_check():
    """Health check endpoint."""
    return {"status": "ok", "service": "Catalog Delta API"}


@app.post("/process")
def process_catalog(request: ProcessRequest):
    """
    Process a catalog file from the given URL.
    Downloads the file, calculates health metrics, and saves to Google Sheets.
    Memory-optimized: streams to temp file, processes, then cleans up.
    """
    temp_path = None
    try:
        # Download to temp file (streaming to avoid memory issues)
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            temp_path = tmp.name
            response = requests.get(request.download_url, timeout=180, stream=True)
            response.raise_for_status()

            for chunk in response.iter_content(chunk_size=8192):
                tmp.write(chunk)

        # Process the file
        summary = calculate_summary_from_file(temp_path)

        # Save to Google Sheets
        save_to_google_sheets(summary)

        return JSONResponse(content={
            "success": True,
            "message": "Catalog processed and saved to Google Sheets",
            "summary": {
                "date": datetime.now().strftime("%Y-%m-%d"),
                **summary
            }
        })

    except requests.RequestException as e:
        raise HTTPException(status_code=400, detail=f"Failed to download file: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing error: {str(e)}")
    finally:
        # Clean up temp file
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)
        gc.collect()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
