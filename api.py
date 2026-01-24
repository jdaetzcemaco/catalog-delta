"""
FastAPI endpoint for automated catalog processing.
Receives a download URL, processes the file, and saves to Google Sheets.
"""

import io
import os
from datetime import datetime

import gspread
import pandas as pd
import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from google.oauth2.service_account import Credentials
from pydantic import BaseModel

# Import processing functions from catalog_delta
from catalog_delta import build_flags, build_summary

app = FastAPI(title="Catalog Delta API")

# Google Sheets configuration
GOOGLE_SHEET_ID = "1jcL_nEsyMqpzssXFh-0IHpfWKDFtFhzlERzKjcdX69Y"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


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


def download_excel(url: str) -> pd.DataFrame:
    """Download Excel file from URL and return as DataFrame."""
    response = requests.get(url, timeout=120, stream=True)
    response.raise_for_status()

    # Read into memory
    content = io.BytesIO(response.content)

    # Try to read the SKUs sheet, fallback to first sheet
    excel_file = pd.ExcelFile(content)
    sheet_name = "SKUs" if "SKUs" in excel_file.sheet_names else 0
    df = pd.read_excel(excel_file, sheet_name=sheet_name)

    # Normalize column names
    df.columns = [c.strip().upper() for c in df.columns]
    return df


def save_to_google_sheets(summary: pd.DataFrame) -> bool:
    """Save the catalog health summary to Google Sheets."""
    client = get_google_sheets_client()
    sheet = client.open_by_key(GOOGLE_SHEET_ID).sheet1

    today_date = datetime.now().strftime("%Y-%m-%d")
    row = [
        today_date,
        int(summary["Total SKUs"].iloc[0]),
        int(summary["Visible"].iloc[0]),
        float(summary["Visible %"].iloc[0]),
        float(summary["With Image %"].iloc[0]),
        float(summary["With Price %"].iloc[0]),
        float(summary["With Stock %"].iloc[0]),
        float(summary["Avg Content Score"].iloc[0]),
        int(summary["Score = 100"].iloc[0]),
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
    """
    try:
        # Download and process the file
        df = download_excel(request.download_url)

        # Build flags and summary
        df_with_flags = build_flags(df)
        summary = build_summary(df_with_flags)

        # Save to Google Sheets
        save_to_google_sheets(summary)

        # Return the summary
        return JSONResponse(content={
            "success": True,
            "message": "Catalog processed and saved to Google Sheets",
            "summary": {
                "date": datetime.now().strftime("%Y-%m-%d"),
                "total_skus": int(summary["Total SKUs"].iloc[0]),
                "visible": int(summary["Visible"].iloc[0]),
                "visible_pct": float(summary["Visible %"].iloc[0]),
                "with_image_pct": float(summary["With Image %"].iloc[0]),
                "with_price_pct": float(summary["With Price %"].iloc[0]),
                "with_stock_pct": float(summary["With Stock %"].iloc[0]),
                "avg_score": float(summary["Avg Content Score"].iloc[0]),
                "perfect_score_count": int(summary["Score = 100"].iloc[0]),
            }
        })

    except requests.RequestException as e:
        raise HTTPException(status_code=400, detail=f"Failed to download file: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing error: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
