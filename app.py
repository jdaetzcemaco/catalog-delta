"""
Streamlit web interface for the Catalog Delta Report Generator.

Run with: streamlit run app.py
"""

import io
from datetime import datetime

import pandas as pd
import streamlit as st
import gspread
import gspread.exceptions
from google.oauth2.service_account import Credentials

from catalog_delta import (
    build_flags,
    build_summary,
    compute_deltas,
    filter_sheet,
    validate_dataframe,
)

# Google Sheets configuration
GOOGLE_SHEET_ID = "1jcL_nEsyMqpzssXFh-0IHpfWKDFtFhzlERzKjcdX69Y"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def save_to_google_sheets(summary: pd.DataFrame) -> bool:
    """Save the catalog health summary to Google Sheets."""
    try:
        # Get credentials from Streamlit secrets
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        client = gspread.authorize(creds)

        # Open the spreadsheet
        try:
            spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
            sheet = spreadsheet.sheet1
        except gspread.exceptions.APIError as api_err:
            st.error(f"Google API Error: {api_err}")
            st.error(f"API Response: {api_err.response.text if hasattr(api_err, 'response') else 'N/A'}")
            return False

        # Get today's values
        today_date = datetime.now().strftime("%Y-%m-%d")
        total_skus = int(summary["Total SKUs"].iloc[0])
        visible = int(summary["Visible"].iloc[0])
        visible_pct = float(summary["Visible %"].iloc[0])
        with_image_pct = float(summary["With Image %"].iloc[0])
        with_price_pct = float(summary["With Price %"].iloc[0])
        with_stock_pct = float(summary["With Stock %"].iloc[0])
        avg_score = float(summary["Avg Content Score"].iloc[0])
        perfect_score = int(summary["Score = 100"].iloc[0])

        # Get yesterday's values to calculate delta
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
                # Previous values (columns: Date, Total, Δ, Visible, Δ, Vis%, Img%, Δ, Price%, Δ, Stock%, Δ, Score, Δ, Perfect, Δ)
                # If old format without deltas, columns are: Date, Total, Visible, Vis%, Img%, Price%, Stock%, Score, Perfect
                if len(last_row) >= 9:
                    prev_total = int(last_row[1]) if last_row[1] else 0
                    prev_visible = int(last_row[3]) if len(last_row) > 3 and last_row[3] else int(last_row[2]) if last_row[2] else 0

                    # Check if old format (9 cols) or new format (17 cols)
                    if len(last_row) < 15:
                        # Old format
                        prev_visible = int(last_row[2]) if last_row[2] else 0
                        prev_image_pct = float(last_row[4]) if last_row[4] else 0
                        prev_price_pct = float(last_row[5]) if last_row[5] else 0
                        prev_stock_pct = float(last_row[6]) if last_row[6] else 0
                        prev_score = float(last_row[7]) if last_row[7] else 0
                        prev_perfect = int(last_row[8]) if last_row[8] else 0
                    else:
                        # New format with deltas
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
            except (ValueError, IndexError):
                pass  # Use default delta values of 0

        # Prepare the row with deltas
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

        # Append the row
        sheet.append_row(row, value_input_option="USER_ENTERED")
        return True
    except KeyError as e:
        st.error(f"Missing secret key: {e}. Make sure gcp_service_account is configured in Streamlit secrets.")
        return False
    except Exception as e:
        st.error(f"Failed to save to Google Sheets: {type(e).__name__}: {e}")
        st.exception(e)
        return False

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="Catalog Delta Report",
    page_icon="📊",
    layout="wide",
)

# --- HEADER ---
st.title("📊 Catalog Delta Report Generator")
st.markdown("Upload your **today** and **yesterday** catalog files to generate a comparison report.")

# --- FILE UPLOADERS ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Today's Catalog")
    today_file = st.file_uploader(
        "Upload today's file",
        type=["xlsx", "csv"],
        key="today",
        help="Excel (.xlsx) or CSV file with SKU data",
    )

with col2:
    st.subheader("Yesterday's Catalog")
    yesterday_file = st.file_uploader(
        "Upload yesterday's file",
        type=["xlsx", "csv"],
        key="yesterday",
        help="Excel (.xlsx) or CSV file with SKU data",
    )


def load_uploaded_file(uploaded_file) -> pd.DataFrame:
    """Load a DataFrame from an uploaded file."""
    if uploaded_file.name.endswith(".xlsx"):
        excel_file = pd.ExcelFile(uploaded_file)
        sheet_name = "SKUs" if "SKUs" in excel_file.sheet_names else 0
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
    else:
        df = pd.read_csv(uploaded_file)

    df.columns = [c.strip().upper() for c in df.columns]
    return df


def generate_excel_download(sheets: dict[str, pd.DataFrame]) -> bytes:
    """Generate Excel file bytes for download."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    return output.getvalue()


# --- PROCESSING ---
if today_file and yesterday_file:
    try:
        # Load data
        with st.spinner("Loading files..."):
            today_raw = load_uploaded_file(today_file)
            yesterday_raw = load_uploaded_file(yesterday_file)

        # Validate
        validate_dataframe(today_raw, today_file.name)
        validate_dataframe(yesterday_raw, yesterday_file.name)

        # Process
        with st.spinner("Processing data..."):
            today = build_flags(today_raw)
            yesterday = build_flags(yesterday_raw)
            merged = compute_deltas(today, yesterday)

        st.success(f"Processed {len(today):,} SKUs from today, {len(yesterday):,} from yesterday")

        # --- DEBUG: Show data diagnostics ---
        with st.expander("🔍 Debug: Data Diagnostics", expanded=False):
            st.write("**Today's columns:**", list(today_raw.columns))
            st.write("**Has Image values (today):**", today["has_image"].value_counts().to_dict())
            st.write("**Has Price values (today):**", today["has_price"].value_counts().to_dict())
            st.write("**Has Image values (yesterday):**", yesterday["has_image"].value_counts().to_dict())
            st.write("**Has Price values (yesterday):**", yesterday["has_price"].value_counts().to_dict())

            # Check merged data
            skus_in_both = len(merged[merged["content_score_today"].notna() & merged["content_score_yesterday"].notna()])
            st.write(f"**SKUs in both files:** {skus_in_both:,}")

            # Sample where image or price might differ
            sample_merged = merged[merged["content_score_today"].notna() & merged["content_score_yesterday"].notna()].head(10)
            st.write("**Sample merged data (first 10 SKUs in both files):**")
            st.dataframe(sample_merged[["SKU", "has_image_today", "has_image_yesterday", "has_price_today", "has_price_yesterday"]].head(10))

            # Check if any differences exist
            in_both = merged["content_score_today"].notna() & merged["content_score_yesterday"].notna()
            img_diff = (merged.loc[in_both, "has_image_today"] != merged.loc[in_both, "has_image_yesterday"]).sum()
            price_diff = (merged.loc[in_both, "has_price_today"] != merged.loc[in_both, "has_price_yesterday"]).sum()
            st.write(f"**Raw image differences:** {img_diff}")
            st.write(f"**Raw price differences:** {price_diff}")

        # --- CALCULATE SKU CHANGES ---
        # New SKUs = exist today but not yesterday
        skus_today = set(today["SKU"])
        skus_yesterday = set(yesterday["SKU"])
        new_skus = skus_today - skus_yesterday
        removed_skus = skus_yesterday - skus_today
        net_sku_change = len(today) - len(yesterday)

        # --- SKU CHANGE SUMMARY ---
        st.header("🆕 SKU Changes (Today vs Yesterday)")
        sku_cols = st.columns(3)
        with sku_cols[0]:
            st.metric(
                "New SKUs",
                f"{len(new_skus):,}",
                delta=f"+{len(new_skus)}" if new_skus else None,
                help="SKUs that exist today but didn't exist yesterday"
            )
        with sku_cols[1]:
            st.metric(
                "Removed SKUs",
                f"{len(removed_skus):,}",
                delta=f"-{len(removed_skus)}" if removed_skus else None,
                delta_color="inverse",
                help="SKUs that existed yesterday but don't exist today"
            )
        with sku_cols[2]:
            st.metric(
                "Net SKU Change",
                f"{net_sku_change:+,}",
                delta=f"{net_sku_change:+,}",
                delta_color="normal" if net_sku_change >= 0 else "inverse",
                help="Total SKUs today minus total SKUs yesterday"
            )

        # --- KPI SUMMARY ---
        st.header("📈 Catalog Health Summary")
        summary = build_summary(today)

        # Display KPIs as metrics
        kpi_cols = st.columns(4)
        with kpi_cols[0]:
            st.metric("Total SKUs", f"{summary['Total SKUs'].iloc[0]:,}")
            st.metric("Visible", f"{summary['Visible'].iloc[0]:,}")
        with kpi_cols[1]:
            st.metric("Visible %", f"{summary['Visible %'].iloc[0]}%")
            st.metric("With Image %", f"{summary['With Image %'].iloc[0]}%")
        with kpi_cols[2]:
            st.metric("With Price %", f"{summary['With Price %'].iloc[0]}%")
            st.metric("With Stock %", f"{summary['With Stock %'].iloc[0]}%")
        with kpi_cols[3]:
            st.metric("Avg Content Score", f"{summary['Avg Content Score'].iloc[0]}")
            st.metric("Perfect Score (100)", f"{summary['Score = 100'].iloc[0]:,}")

        # --- SAVE TO GOOGLE SHEETS ---
        st.divider()
        col_save, col_status = st.columns([1, 3])
        with col_save:
            if st.button("📊 Save to History", type="primary", help="Save today's summary to Google Sheets"):
                with st.spinner("Saving to Google Sheets..."):
                    if save_to_google_sheets(summary):
                        st.success("✅ Saved to Google Sheets!")
                    else:
                        st.error("Failed to save. Check your configuration.")
        with col_status:
            st.caption("Click to save today's catalog health metrics to your Google Sheets history.")

        # --- DELTA SUMMARIES ---
        st.header("🔄 Changes Detected")

        # Build all sheets
        sheets = {
            "Catalog Health": summary,
            "New SKUs": today[today["SKU"].isin(new_skus)][["SKU", "content_score", "is_visible", "has_image", "has_price", "has_stock"]].copy(),
            "Removed SKUs": yesterday[yesterday["SKU"].isin(removed_skus)][["SKU", "content_score", "is_visible", "has_image", "has_price", "has_stock"]].copy(),
            "No Longer Visible": filter_sheet(
                merged,
                merged["no_longer_visible"],
                ["SKU", "content_score_today", "is_visible_today", "is_visible_yesterday"]
            ),
            "Newly Visible": filter_sheet(
                merged,
                merged["newly_visible"],
                ["SKU", "content_score_today", "is_visible_today", "is_visible_yesterday"]
            ),
            "Image Changes": filter_sheet(
                merged,
                merged["image_changed"],
                ["SKU", "has_image_today", "has_image_yesterday"]
            ),
            "Price Changes": filter_sheet(
                merged,
                merged["price_changed"],
                ["SKU", "has_price_today", "has_price_yesterday"]
            ),
            "Stock Flips": filter_sheet(
                merged,
                merged["stock_flipped"],
                ["SKU", "has_stock_today", "has_stock_yesterday"]
            ),
            "Score Changes": filter_sheet(
                merged,
                merged["score_changed"],
                ["SKU", "content_score_today", "content_score_yesterday", "delta_score"]
            ),
            "Top Priorities": today.loc[
                (today["content_score"] < 80) & (today["is_visible"] == 1) & (today["has_stock"] == 1)
            ].sort_values("content_score").head(50),
            "Stock Not Visible": today.loc[
                (today["has_stock"] == 1) & (today["is_visible"] == 0)
            ][["SKU", "content_score", "has_image", "has_price", "has_stock"]].sort_values("content_score", ascending=False),
        }

        # Change summary cards
        change_cols = st.columns(5)
        with change_cols[0]:
            st.metric("New SKUs", len(sheets["New SKUs"]), delta=f"+{len(sheets['New SKUs'])}" if len(sheets["New SKUs"]) > 0 else None, help="SKUs added to catalog")
            st.metric("Removed SKUs", len(sheets["Removed SKUs"]), delta=f"-{len(sheets['Removed SKUs'])}" if len(sheets["Removed SKUs"]) > 0 else None, delta_color="inverse", help="SKUs removed from catalog")
        with change_cols[1]:
            st.metric("Newly Visible", len(sheets["Newly Visible"]), delta=f"+{len(sheets['Newly Visible'])}" if len(sheets["Newly Visible"]) > 0 else None)
            st.metric("No Longer Visible", len(sheets["No Longer Visible"]), delta=f"-{len(sheets['No Longer Visible'])}" if len(sheets["No Longer Visible"]) > 0 else None, delta_color="inverse")
        with change_cols[2]:
            st.metric("Image Changes", len(sheets["Image Changes"]))
            st.metric("Price Changes", len(sheets["Price Changes"]))
        with change_cols[3]:
            st.metric("Stock Flips", len(sheets["Stock Flips"]))
            st.metric("Score Changes (±10+)", len(sheets["Score Changes"]))
        with change_cols[4]:
            st.metric("Stock Not Visible", len(sheets["Stock Not Visible"]), help="⚠️ SKUs with stock but NOT visible - potential lost sales!")
            st.metric("Top Priorities", len(sheets["Top Priorities"]), help="Visible SKUs with stock but score < 80")

        # --- DETAILED TABLES ---
        st.header("📋 Detailed Reports")

        tab_names = [name for name in sheets.keys() if name != "Catalog Health"]
        tabs = st.tabs(tab_names)

        for tab, name in zip(tabs, tab_names):
            with tab:
                df = sheets[name]
                if len(df) > 0:
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else:
                    st.info(f"No {name.lower()} found.")

        # --- DOWNLOAD BUTTON ---
        st.header("📥 Download Report")

        excel_data = generate_excel_download(sheets)
        filename = f"Catalog_Delta_{datetime.now().strftime('%Y%m%d')}.xlsx"

        st.download_button(
            label="Download Excel Report",
            data=excel_data,
            file_name=filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
        )

    except ValueError as e:
        st.error(f"Validation Error: {e}")
    except Exception as e:
        st.error(f"Error processing files: {e}")
        st.exception(e)

else:
    # Show instructions when no files uploaded
    st.info("👆 Upload both files above to generate the report.")

    with st.expander("📖 Expected File Format"):
        st.markdown("""
        Your catalog files should contain the following columns (column names are case-insensitive):

        | Column | Required | Description |
        |--------|----------|-------------|
        | **SKU** | Yes | Unique product identifier |
        | NOMBRE DE SKU / NOMBRE DE PRODUCTO | No | Product name |
        | DESCRIPCION ERP | No | Product description |
        | MARCA | No | Brand |
        | TIENE PRECIO / PRECIO | No | Has price flag or price value |
        | TIENE IMAGEN / IMAGEN PRIMARIA / URL IMAGEN | No | Image availability |
        | STOCK / TIENE STOCK | No | Stock availability |
        | VISIBLE | No | Visibility flag |
        | HABILITADO/DESHABILITADO | No | Enabled status |
        | NIVEL 1, NIVEL 2, NIVEL 3 | No | Taxonomy levels |

        **Supported formats:** Excel (.xlsx) or CSV (.csv)

        For Excel files, the tool will look for a sheet named "SKUs" first.
        """)

    with st.expander("📊 Content Score Weights"):
        st.markdown("""
        The content score (0-100) is calculated using these weights:

        | Attribute | Weight |
        |-----------|--------|
        | Has Image | 25 |
        | Has Description | 15 |
        | Has Price | 15 |
        | Taxonomy Depth | 15 |
        | Has Name | 10 |
        | Is Visible | 10 |
        | Has Brand | 5 |
        """)
