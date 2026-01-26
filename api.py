"""
FastAPI endpoint for automated catalog processing.
Receives a download URL, processes the file, and saves to Google Sheets.
Memory-optimized for large files. Uses background processing.
"""

import gc
import logging
import os
import tempfile
import threading
from datetime import datetime

import gspread
import pandas as pd
import requests
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from google.oauth2.service_account import Credentials
from pydantic import BaseModel

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Catalog Delta API")

# Track processing status
processing_status = {"status": "idle", "last_run": None, "last_result": None}

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
    Calculate catalog health summary using vectorized pandas operations.
    Much faster and more memory-efficient than iterrows().
    """
    logger.info("Opening Excel file...")

    # First, get sheet name
    xl = pd.ExcelFile(filepath)
    sheet_name = "SKUs" if "SKUs" in xl.sheet_names else 0
    xl.close()

    # Read the data
    logger.info(f"Reading sheet: {sheet_name}")
    df = pd.read_excel(filepath, sheet_name=sheet_name)
    df.columns = [c.strip().upper() for c in df.columns]

    total_skus = len(df)
    logger.info(f"Loaded {total_skus} rows")

    # Vectorized calculations (much faster than iterrows)

    # Visibility
    if "VISIBLE" in df.columns:
        visible = ((df["VISIBLE"] == 1) | (df["VISIBLE"] == "Si") | (df["VISIBLE"] == "1") | (df["VISIBLE"] == True)).sum()
    else:
        visible = 0

    # Image - check multiple possible columns
    with_image = 0
    for col in ["TIENE IMAGEN", "IMAGEN PRIMARIA", "URL IMAGEN"]:
        if col in df.columns:
            with_image = max(with_image, df[col].notna().sum() - (df[col] == "").sum() - (df[col] == 0).sum())

    # Price
    with_price = 0
    if "TIENE PRECIO" in df.columns:
        with_price = df["TIENE PRECIO"].notna().sum() - (df["TIENE PRECIO"] == "").sum() - (df["TIENE PRECIO"] == 0).sum()
    elif "PRECIO" in df.columns:
        with_price = (df["PRECIO"].notna() & (df["PRECIO"] > 0)).sum()

    # Stock
    with_stock = 0
    if "TIENE STOCK" in df.columns:
        with_stock = df["TIENE STOCK"].notna().sum() - (df["TIENE STOCK"] == "").sum() - (df["TIENE STOCK"] == 0).sum()
    elif "STOCK" in df.columns:
        with_stock = (df["STOCK"].notna() & (df["STOCK"] > 0)).sum()

    # Simple average score calculation (simplified for memory efficiency)
    # Just estimate based on the percentages we already calculated
    components = 0
    if with_image > 0:
        components += (with_image / total_skus) * WEIGHTS["has_image"]
    if with_price > 0:
        components += (with_price / total_skus) * WEIGHTS["has_price"]
    if visible > 0:
        components += (visible / total_skus) * WEIGHTS["is_visible"]

    # Check for description
    for col in ["DESCRIPCION ERP", "DESCRIPCION"]:
        if col in df.columns:
            desc_count = df[col].notna().sum() - (df[col] == "").sum()
            components += (desc_count / total_skus) * WEIGHTS["has_description"]
            break

    # Check for name
    for col in ["NOMBRE DE SKU", "NOMBRE DE PRODUCTO", "NOMBRE"]:
        if col in df.columns:
            name_count = df[col].notna().sum() - (df[col] == "").sum()
            components += (name_count / total_skus) * WEIGHTS["has_name"]
            break

    # Check for brand
    if "MARCA" in df.columns:
        brand_count = df["MARCA"].notna().sum() - (df["MARCA"] == "").sum()
        components += (brand_count / total_skus) * WEIGHTS["has_brand"]

    # Taxonomy (simplified)
    tax_count = 0
    for col in ["NIVEL 1", "NIVEL 2", "NIVEL 3", "NIVEL1", "NIVEL2", "NIVEL3"]:
        if col in df.columns:
            tax_count += 1
    components += min(tax_count * 5, WEIGHTS["taxonomy_depth"])

    avg_score = round(components, 1)

    # Perfect score count (simplified estimate)
    perfect_score = 0  # Hard to calculate without full row iteration

    logger.info("Calculations complete")

    # Clean up
    del df
    gc.collect()

    return {
        "total_skus": total_skus,
        "visible": int(visible),
        "visible_pct": round((visible / total_skus) * 100, 1) if total_skus > 0 else 0,
        "with_image_pct": round((with_image / total_skus) * 100, 1) if total_skus > 0 else 0,
        "with_price_pct": round((with_price / total_skus) * 100, 1) if total_skus > 0 else 0,
        "with_stock_pct": round((with_stock / total_skus) * 100, 1) if total_skus > 0 else 0,
        "avg_score": avg_score,
        "perfect_score_count": perfect_score,
    }


def save_to_google_sheets(summary: dict) -> bool:
    """Save the catalog health summary to Google Sheets with delta from yesterday."""
    logger.info("Connecting to Google Sheets...")

    # Check if credentials are set
    if not os.environ.get("GCP_PROJECT_ID"):
        raise ValueError("GCP_PROJECT_ID environment variable not set")
    if not os.environ.get("GCP_PRIVATE_KEY"):
        raise ValueError("GCP_PRIVATE_KEY environment variable not set")
    if not os.environ.get("GCP_CLIENT_EMAIL"):
        raise ValueError("GCP_CLIENT_EMAIL environment variable not set")

    logger.info(f"Using service account: {os.environ.get('GCP_CLIENT_EMAIL')}")

    client = get_google_sheets_client()
    logger.info(f"Opening sheet: {GOOGLE_SHEET_ID}")
    sheet = client.open_by_key(GOOGLE_SHEET_ID).sheet1

    # Get today's values
    today_date = datetime.now().strftime("%Y-%m-%d")
    total_skus = summary["total_skus"]
    visible = summary["visible"]
    visible_pct = summary["visible_pct"]
    with_image_pct = summary["with_image_pct"]
    with_price_pct = summary["with_price_pct"]
    with_stock_pct = summary["with_stock_pct"]
    avg_score = summary["avg_score"]
    perfect_score = summary["perfect_score_count"]

    # Get yesterday's values to calculate delta
    logger.info("Reading previous row for delta calculation...")
    all_rows = sheet.get_all_values()

    # Calculate deltas (default to 0 if no previous data)
    delta_skus = 0
    delta_visible = 0
    delta_image_pct = 0.0
    delta_price_pct = 0.0
    delta_stock_pct = 0.0
    delta_score = 0.0
    delta_perfect = 0

    if len(all_rows) > 1:  # Has data beyond header
        try:
            last_row = all_rows[-1]
            if len(last_row) >= 9:
                # Check if old format (9 cols) or new format (16 cols)
                if len(last_row) < 15:
                    # Old format without deltas
                    prev_total = int(last_row[1]) if last_row[1] else 0
                    prev_visible = int(last_row[2]) if last_row[2] else 0
                    prev_image_pct = float(last_row[4]) if last_row[4] else 0
                    prev_price_pct = float(last_row[5]) if last_row[5] else 0
                    prev_stock_pct = float(last_row[6]) if last_row[6] else 0
                    prev_score = float(last_row[7]) if last_row[7] else 0
                    prev_perfect = int(last_row[8]) if last_row[8] else 0
                else:
                    # New format with deltas
                    prev_total = int(last_row[1]) if last_row[1] else 0
                    prev_visible = int(last_row[3]) if last_row[3] else 0
                    prev_image_pct = float(last_row[6]) if last_row[6] else 0
                    prev_price_pct = float(last_row[8]) if last_row[8] else 0
                    prev_stock_pct = float(last_row[10]) if last_row[10] else 0
                    prev_score = float(last_row[12]) if last_row[12] else 0
                    prev_perfect = int(last_row[14]) if last_row[14] else 0

                delta_skus = total_skus - prev_total
                delta_visible = visible - prev_visible
                delta_image_pct = round(with_image_pct - prev_image_pct, 2)
                delta_price_pct = round(with_price_pct - prev_price_pct, 2)
                delta_stock_pct = round(with_stock_pct - prev_stock_pct, 2)
                delta_score = round(avg_score - prev_score, 2)
                delta_perfect = perfect_score - prev_perfect

                logger.info(f"Delta from previous: SKUs {delta_skus:+d}, Visible {delta_visible:+d}")
        except (ValueError, IndexError) as e:
            logger.warning(f"Could not calculate delta: {e}")

    # Prepare row with deltas
    row = [
        today_date,
        total_skus,
        delta_skus,
        visible,
        delta_visible,
        visible_pct,
        with_image_pct,
        delta_image_pct,
        with_price_pct,
        delta_price_pct,
        with_stock_pct,
        delta_stock_pct,
        avg_score,
        delta_score,
        perfect_score,
        delta_perfect,
    ]

    logger.info(f"Appending row with deltas: {row}")
    sheet.append_row(row, value_input_option="USER_ENTERED")
    logger.info("Row appended successfully")
    return True


@app.get("/")
def health_check():
    """Health check endpoint."""
    return {"status": "ok", "service": "Catalog Delta API"}


@app.get("/status")
def get_status():
    """Get the current processing status."""
    return processing_status


def process_in_background(download_url: str):
    """Background task to download and process the catalog."""
    global processing_status
    temp_path = None

    try:
        logger.info(f"Starting background process for URL: {download_url[:100]}...")
        processing_status["status"] = "downloading"

        # Download to temp file (streaming to avoid memory issues)
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            temp_path = tmp.name
            logger.info(f"Downloading to temp file: {temp_path}")
            response = requests.get(download_url, timeout=300, stream=True)
            response.raise_for_status()

            total_size = 0
            for chunk in response.iter_content(chunk_size=8192):
                tmp.write(chunk)
                total_size += len(chunk)

            logger.info(f"Download complete. Total size: {total_size / 1024 / 1024:.2f} MB")

        processing_status["status"] = "processing"
        logger.info("Processing file...")

        # Process the file
        summary = calculate_summary_from_file(temp_path)
        logger.info(f"Processing complete. Found {summary['total_skus']} SKUs")

        processing_status["status"] = "saving"
        logger.info("Saving to Google Sheets...")

        # Save to Google Sheets
        save_to_google_sheets(summary)
        logger.info("Saved to Google Sheets successfully!")

        processing_status["status"] = "completed"
        processing_status["last_run"] = datetime.now().isoformat()
        processing_status["last_result"] = {
            "success": True,
            "summary": summary
        }
        logger.info("Background processing completed successfully!")

    except Exception as e:
        logger.error(f"Background processing failed: {str(e)}", exc_info=True)
        processing_status["status"] = "error"
        processing_status["last_result"] = {
            "success": False,
            "error": str(e)
        }
    finally:
        # Clean up temp file
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)
            logger.info("Temp file cleaned up")
        gc.collect()


@app.post("/process")
def process_catalog(request: ProcessRequest, background_tasks: BackgroundTasks):
    """
    Process a catalog file from the given URL.
    Returns immediately and processes in background.
    Check /status endpoint for progress.
    """
    global processing_status

    if processing_status["status"] in ["downloading", "processing", "saving"]:
        return JSONResponse(content={
            "success": False,
            "message": "Another process is already running",
            "current_status": processing_status["status"]
        }, status_code=409)

    # Start background processing
    processing_status["status"] = "starting"
    background_tasks.add_task(process_in_background, request.download_url)

    return JSONResponse(content={
        "success": True,
        "message": "Processing started in background. Check /status for progress.",
        "status_url": "/status"
    })


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
