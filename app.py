"""
Streamlit web interface for the Catalog Delta Report Generator.

Run with: streamlit run app.py
"""

from __future__ import annotations

import fnmatch
import io
from datetime import datetime

import pandas as pd
import streamlit as st
import gspread
import gspread.exceptions
from google.oauth2.service_account import Credentials

try:
    from office365.runtime.auth.client_credential import ClientCredential
    from office365.sharepoint.client_context import ClientContext
    from office365.sharepoint.files.file import File as SharePointFile
    _ONEDRIVE_AVAILABLE = True
except ImportError:
    _ONEDRIVE_AVAILABLE = False

from catalog_delta import (
    build_flags,
    build_summary,
    compute_deltas,
    filter_sheet,
    validate_dataframe,
    yesno,
    nonempty,
    safe_get,
)

# Google Sheets configuration
GOOGLE_SHEET_ID = "1jcL_nEsyMqpzssXFh-0IHpfWKDFtFhzlERzKjcdX69Y"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# OneDrive folder path (server-relative URL)
ONEDRIVE_FOLDER = "/cemaco-reports/incoming"


class NamedBytesIO(io.BytesIO):
    """BytesIO with a .name attribute so it works like an UploadedFile."""
    def __init__(self, content: bytes, name: str):
        super().__init__(content)
        self.name = name


@st.cache_resource(show_spinner=False)
def load_from_onedrive() -> dict:
    """
    Try to auto-load files from OneDrive for Business.

    Returns a dict with keys: today, today_name, yesterday, yesterday_name,
    diseno, diseno_name, edicion, edicion_name, loaded, error.
    All file values are raw bytes or None.

    Requires st.secrets["onedrive"] with keys: site_url, client_id, client_secret.
    """
    result = {
        "today": None, "today_name": "",
        "yesterday": None, "yesterday_name": "",
        "diseno": None, "diseno_name": "",
        "edicion": None, "edicion_name": "",
        "loaded": False, "error": None,
    }

    if not _ONEDRIVE_AVAILABLE:
        result["error"] = "office365 library not installed"
        return result

    try:
        if "onedrive" not in st.secrets:
            return result  # Not configured — silently skip

        od = st.secrets["onedrive"]
        site_url = od["site_url"]
        client_id = od["client_id"]
        client_secret = od["client_secret"]

        ctx = ClientContext(site_url).with_credentials(
            ClientCredential(client_id, client_secret)
        )

        # List files in the incoming folder
        folder = ctx.web.get_folder_by_server_relative_url(ONEDRIVE_FOLDER)
        files = folder.files
        ctx.load(files)
        ctx.execute_query()

        file_infos = []
        for f in files:
            name = f.properties.get("Name", "")
            modified = f.properties.get("TimeLastModified", "")
            rel_url = f.properties.get("ServerRelativeUrl", "")
            file_infos.append((name, modified, rel_url))

        def _download(rel_url: str) -> bytes:
            response = SharePointFile.open_binary(ctx, rel_url)
            return response.content

        def _find(pattern: str) -> list:
            matched = [(n, m, u) for n, m, u in file_infos if fnmatch.fnmatch(n, pattern)]
            return sorted(matched, key=lambda x: x[1], reverse=True)

        # Catalog files — most recent = today, second = yesterday
        catalog_files = _find("catalog-daily-*.xlsx")
        if catalog_files:
            result["today"] = _download(catalog_files[0][2])
            result["today_name"] = catalog_files[0][0]
            if len(catalog_files) > 1:
                result["yesterday"] = _download(catalog_files[1][2])
                result["yesterday_name"] = catalog_files[1][0]

        # Diseño productivity file
        diseno_files = _find("productivity-diseno-*.xlsx")
        if diseno_files:
            result["diseno"] = _download(diseno_files[0][2])
            result["diseno_name"] = diseno_files[0][0]

        # Edición productivity file
        edicion_files = _find("productivity-edicion-*.xlsx")
        if edicion_files:
            result["edicion"] = _download(edicion_files[0][2])
            result["edicion_name"] = edicion_files[0][0]

        result["loaded"] = any([
            result["today"], result["diseno"], result["edicion"]
        ])

    except Exception as exc:
        result["error"] = str(exc)

    return result


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

# --- AUTH ---
if not st.session_state.get("authenticated"):
    st.title("🔐 Catalog Delta Report")
    pwd = st.text_input("Password", type="password", placeholder="Enter access password")
    if st.button("Login", type="primary"):
        if pwd == st.secrets.get("app_password", ""):
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Incorrect password")
    st.stop()

# --- HEADER ---
st.title("📊 Catalog Delta Report Generator")
st.markdown("Upload your **today** and **yesterday** catalog files to generate a comparison report.")

# --- ONEDRIVE AUTO-LOAD (once per session) ---
if "od_data" not in st.session_state:
    with st.spinner("Checking OneDrive for latest files…"):
        st.session_state.od_data = load_from_onedrive()
od_data: dict = st.session_state.od_data

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

col3, col4 = st.columns(2)

with col3:
    st.subheader("📋 Diseño Team")
    diseno_file = st.file_uploader(
        "Upload Diseño productivity report",
        type=["xlsx"],
        key="diseno",
        help="Daily productivity file for Diseño team",
    )

with col4:
    st.subheader("📋 Edición Team")
    edicion_file = st.file_uploader(
        "Upload Edición productivity report",
        type=["xlsx"],
        key="edicion",
        help="Daily productivity file for Edición team",
    )

# --- SIDEBAR: File Status ---
with st.sidebar:
    if od_data.get("loaded"):
        st.success("✅ Auto-loaded from OneDrive")
    elif od_data.get("error"):
        st.caption(f"OneDrive: {od_data['error']}")

    st.header("📬 File Status")
    st.write("Today Catalog:", "✅ Uploaded" if (today_file or od_data.get("today")) else "⏳ Pending")
    st.write("Yesterday Catalog:", "✅ Uploaded" if (yesterday_file or od_data.get("yesterday")) else "⏳ Pending")
    st.write("Diseño Team:", "✅ Uploaded" if (diseno_file or od_data.get("diseno")) else "⏳ Pending (optional)")
    st.write("Edición Team:", "✅ Uploaded" if (edicion_file or od_data.get("edicion")) else "⏳ Pending (optional)")


def load_uploaded_file(uploaded_file) -> pd.DataFrame:
    """Load a DataFrame from an uploaded file."""
    if uploaded_file.name.endswith(".xlsx"):
        excel_file = pd.ExcelFile(uploaded_file)
        sheet_name = "SKUs" if "SKUs" in excel_file.sheet_names else 0
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
    else:
        df = pd.read_csv(uploaded_file)

    df.columns = [c.strip().upper() for c in df.columns]
    if "SKU" in df.columns:
        df["SKU"] = df["SKU"].astype(str).str.strip()
    return df


def generate_excel_download(sheets: dict[str, pd.DataFrame]) -> bytes:
    """Generate Excel file bytes for download."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    return output.getvalue()


def load_productivity_file(source) -> pd.DataFrame:
    """
    Load a productivity DataFrame from an uploaded file or raw bytes.
    Strips whitespace from column names but does NOT uppercase them,
    preserving angle-bracket columns like <ID> and <Name>.
    """
    if isinstance(source, (bytes, bytearray)):
        df = pd.read_excel(io.BytesIO(source))
    else:
        df = pd.read_excel(source)
    df.columns = [c.strip() for c in df.columns]
    if "<ID>" in df.columns:
        df["<ID>"] = df["<ID>"].astype(str).str.strip()
    return df


def render_inventory_tab(
    today_raw: pd.DataFrame,
    today_flagged: pd.DataFrame,
    yesterday_raw: pd.DataFrame | None = None,
) -> None:
    """Render the 📦 Inventario Omnicanal tab — 6 QA sections."""

    # ── Shared masks ────────────────────────────────────────────────────────
    has_stock_m   = yesno(safe_get(today_raw, "TIENE STOCK")) == 1
    is_visible_m  = yesno(safe_get(today_raw, "VISIBLE")) == 1
    nivel1_lower  = safe_get(today_raw, "NIVEL 1").str.strip().str.lower()
    temp_lower    = safe_get(today_raw, "TEMPORADA ERP").str.strip().str.lower()
    modal_empty   = ~nonempty(safe_get(today_raw, "MODAL")).astype(bool)
    no_img_mask   = (
        ~nonempty(safe_get(today_raw, "TIENE IMAGEN")).astype(bool) |
        (safe_get(today_raw, "TIENE IMAGEN").str.strip().str.lower() == "no")
    )
    disabled_mask = safe_get(today_raw, "HABILITADO/DESHABILITADO").str.lower().str.contains("deshab", na=False)

    # No Físicos mask — excluded from all actionable sections
    no_fisicos_mask = nivel1_lower.str.contains("mesa de regalos|certificado de regalo", na=False)
    is_physical     = ~no_fisicos_mask

    # ── KPI Row ─────────────────────────────────────────────────────────────
    try:
        # All KPIs exclude No Físicos
        is_tipo_c_kpi = nivel1_lower == "catalogo completo"
        stock_no_vis = int((has_stock_m & ~is_visible_m & is_physical & ~is_tipo_c_kpi & ~disabled_mask).sum())

        try:
            merged_score = today_raw[["SKU"]].merge(
                today_flagged[["SKU", "content_score"]], on="SKU", how="left"
            )
            stock_low_score = int(
                (has_stock_m & is_visible_m & is_physical & (merged_score["content_score"].fillna(100) < 80)).sum()
            )
        except Exception:
            stock_low_score = 0

        disabled_with_stock = int((disabled_mask & has_stock_m & is_physical).sum())
        tipo_c_stock        = int(((nivel1_lower == "catalogo completo") & has_stock_m & is_physical).sum())
        long_tail_sin_modal = int(((temp_lower == "long tail proveedor") & modal_empty & is_physical).sum())
        acciones_urgentes   = stock_no_vis + disabled_with_stock + tipo_c_stock + long_tail_sin_modal

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("🚫 Stock sin Visibilidad",     stock_no_vis)
        k2.metric("📉 Stock+Visible, Score<80",   stock_low_score)
        k3.metric("⛔ Deshabilitados con Stock",  disabled_with_stock)
        k4.metric("🔴 Tipo C con Inventario",     tipo_c_stock)
        k5.metric("🚚 Long Tail sin Modal",       long_tail_sin_modal)
        st.caption(f"⚠️ **Acciones urgentes totales:** {acciones_urgentes:,}  ·  Productos no físicos excluidos: {int(no_fisicos_mask.sum()):,}")
    except Exception as exc:
        st.warning(f"No se pudo calcular KPIs: {exc}")

    st.divider()

    # ── Section 1 — Tiene Stock pero NO Visible ──────────────────────────────
    st.subheader("⚠️ 1. Tiene Stock pero NO Visible")
    st.caption("Excluye: Tipo C (ver §2), Deshabilitados (ver §5), No Físicos (ver §3).")
    try:
        s1_cols = ["SKU", "NOMBRE DE PRODUCTO", "NIVEL 1", "TEMPORADA ERP",
                   "HABILITADO/DESHABILITADO", "TIENE STOCK", "MODAL"]
        is_tipo_c = nivel1_lower == "catalogo completo"
        mask_s1 = has_stock_m & ~is_visible_m & is_physical & ~is_tipo_c & ~disabled_mask
        df_s1 = today_raw[mask_s1][
            [c for c in s1_cols if c in today_raw.columns]
        ].reset_index(drop=True)

        if len(df_s1):
            st.dataframe(df_s1, use_container_width=True, hide_index=True)
        else:
            st.success("No se encontraron SKUs con stock pero sin visibilidad.")
    except Exception as exc:
        st.warning(f"Sección 1 no disponible — columna faltante: {exc}")

    st.divider()

    # ── Section 2 — Tipo C con Inventario ────────────────────────────────────
    st.subheader("🔴 2. Tipo C con Inventario")
    st.caption("Productos sin foto/contenido completo — disponibles en tienda pero no en línea. NIVEL 1 = 'Catalogo Complete'.")
    try:
        s2_cols = ["SKU", "NOMBRE DE PRODUCTO", "NIVEL 1", "TIENE STOCK", "VISIBLE", "HABILITADO/DESHABILITADO"]

        # ── Pendientes: siguen siendo Tipo C hoy ────────────────────────────
        mask_pendientes = (nivel1_lower == "catalogo completo") & has_stock_m & is_physical
        df_pendientes = today_raw[mask_pendientes][
            [c for c in s2_cols if c in today_raw.columns]
        ].reset_index(drop=True)

        # ── Graduados: eran Tipo C ayer, hoy ya tienen categoría real ───────
        df_graduados = None
        df_graduados_visibles = None
        if yesterday_raw is not None:
            try:
                ayer_nivel1 = safe_get(yesterday_raw, "NIVEL 1").str.strip().str.lower()
                mask_era_tipo_c = ayer_nivel1 == "catalogo completo"
                skus_era_tipo_c = set(yesterday_raw[mask_era_tipo_c]["SKU"])

                grad_cols = ["SKU", "NOMBRE DE PRODUCTO", "NIVEL 1", "VISIBLE", "TIENE STOCK"]
                hoy_tipo_c_skus = set(today_raw[nivel1_lower == "catalogo completo"]["SKU"])
                graduated_skus = skus_era_tipo_c - hoy_tipo_c_skus

                df_graduados = today_raw[today_raw["SKU"].isin(graduated_skus)][
                    [c for c in grad_cols if c in today_raw.columns]
                ].reset_index(drop=True)

                df_graduados_visibles = df_graduados[
                    yesno(safe_get(df_graduados, "VISIBLE")) == 1
                ].reset_index(drop=True) if len(df_graduados) else df_graduados
            except Exception:
                pass

        # ── Display ─────────────────────────────────────────────────────────
        if df_graduados is not None:
            g1, g2, g3 = st.columns(3)
            g1.metric("📋 Pendientes (Tipo C con Stock)", len(df_pendientes))
            g2.metric("🎓 Graduados hoy", len(df_graduados), delta=f"+{len(df_graduados)}" if len(df_graduados) else None)
            g3.metric("✅ Graduados + Ya Visibles", len(df_graduados_visibles) if df_graduados_visibles is not None else 0)
        else:
            st.metric("📋 Pendientes (Tipo C con Stock)", len(df_pendientes))
            st.caption("Carga el catálogo de ayer para ver graduados del día.")

        if len(df_pendientes):
            st.markdown("**Pendientes:**")
            st.dataframe(df_pendientes, use_container_width=True, hide_index=True)
        else:
            st.success("No hay SKUs Tipo C con inventario pendientes.")

        if df_graduados is not None and len(df_graduados):
            st.markdown("**Graduados hoy** (eran Tipo C ayer, hoy tienen categoría):")
            st.dataframe(df_graduados, use_container_width=True, hide_index=True)
    except Exception as exc:
        st.warning(f"Sección 2 no disponible — columna faltante: {exc}")

    st.divider()

    # ── Section 3 — Productos No Físicos ─────────────────────────────────────
    st.subheader("🎁 3. Productos No Físicos (Mesa de Regalo / Certificado de Regalo)")
    try:
        s3_cols = ["SKU", "NOMBRE DE PRODUCTO", "NIVEL 1", "TIENE STOCK", "VISIBLE"]
        mask_s3 = nivel1_lower.str.contains("mesa de regalos|certificado de regalo", na=False)
        df_s3 = today_raw[mask_s3][
            [c for c in s3_cols if c in today_raw.columns]
        ].reset_index(drop=True)

        st.info(f"**{len(df_s3)} productos no físicos** encontrados — excluir de análisis de inventario.")
        if len(df_s3):
            st.dataframe(df_s3, use_container_width=True, hide_index=True)
    except Exception as exc:
        st.warning(f"Sección 3 no disponible — columna faltante: {exc}")

    st.divider()

    # ── Section 4 — Long Tail Proveedor sin Modal ─────────────────────────────
    st.subheader("🚚 4. Long Tail Proveedor sin Modal")
    try:
        s4_cols = ["SKU", "NOMBRE DE PRODUCTO", "NIVEL 1", "TEMPORADA ERP",
                   "MODAL", "VISIBLE", "HABILITADO/DESHABILITADO"]
        mask_s4 = (temp_lower == "long tail proveedor") & modal_empty & is_physical
        df_s4 = today_raw[mask_s4][
            [c for c in s4_cols if c in today_raw.columns]
        ].reset_index(drop=True)

        if len(df_s4):
            st.warning(f"**{len(df_s4)} SKUs** Long Tail Proveedor sin modal asignado.")
        else:
            st.success("Todos los SKUs Long Tail Proveedor tienen modal.")
        st.caption("ℹ️ Long Tail Cemaco = proveedor externo almacenado en bodega CEMACO — no requiere modal.")
        if len(df_s4):
            st.dataframe(df_s4, use_container_width=True, hide_index=True)
    except Exception as exc:
        st.warning(f"Sección 4 no disponible — columna faltante: {exc}")

    st.divider()

    # ── Section 5a — Deshabilitados con Inventario ───────────────────────────
    st.subheader("⛔ 5. Deshabilitados con Inventario")
    try:
        s5a_cols = ["SKU", "NOMBRE DE PRODUCTO", "NIVEL 1", "TEMPORADA ERP",
                    "HABILITADO/DESHABILITADO", "TIENE STOCK", "VISIBLE"]
        mask_s5a = disabled_mask & has_stock_m & is_physical
        df_s5a = today_raw[mask_s5a][
            [c for c in s5a_cols if c in today_raw.columns]
        ].reset_index(drop=True)

        if len(df_s5a):
            st.error(f"**{len(df_s5a)} SKUs** deshabilitados pero con inventario — hay ventas bloqueadas.")
            st.dataframe(df_s5a, use_container_width=True, hide_index=True)
        else:
            st.success("No se encontraron SKUs deshabilitados con inventario.")
    except Exception as exc:
        st.warning(f"Sección 5 no disponible — columna faltante: {exc}")

    st.divider()

    # ── Section 6 — Sin Imagen + Deshabilitado ────────────────────────────────
    st.subheader("🖼️ 6. Sin Imagen + Deshabilitado (Integración no recibió imagen)")
    try:
        s5_cols = ["SKU", "NOMBRE DE PRODUCTO", "NIVEL 1", "TEMPORADA ERP",
                   "TIENE IMAGEN", "URL IMAGEN", "HABILITADO/DESHABILITADO"]
        mask_s5 = no_img_mask & disabled_mask & is_physical
        df_s5 = today_raw[mask_s5][
            [c for c in s5_cols if c in today_raw.columns]
        ].reset_index(drop=True)

        if len(df_s5):
            st.error(f"**{len(df_s5)} SKUs** deshabilitados porque la integración nunca recibió imagen.")
            st.dataframe(df_s5, use_container_width=True, hide_index=True)
        else:
            st.success("No se encontraron SKUs deshabilitados por falta de imagen.")
    except Exception as exc:
        st.warning(f"Sección 6 no disponible — columna faltante: {exc}")

    st.divider()

    # ── Section 7 — URL Imagen no actualizada ──────────────────────────────── ────────────────────────────────
    st.subheader("🔗 7. URL Imagen no actualizada (posible cambio de categoría)")
    if yesterday_raw is None:
        st.info("Carga el catálogo de ayer para habilitar esta verificación.")
    else:
        try:
            t_cols = [c for c in ["SKU", "NOMBRE DE PRODUCTO", "NIVEL 1", "URL IMAGEN"] if c in today_raw.columns]
            y_cols = [c for c in ["SKU", "NIVEL 1", "URL IMAGEN"] if c in yesterday_raw.columns]
            merged_raw = today_raw[t_cols].merge(
                yesterday_raw[y_cols].rename(columns={"NIVEL 1": "NIVEL 1_AYER", "URL IMAGEN": "URL IMAGEN_AYER"}),
                on="SKU",
                how="inner",
            )
            n1_hoy  = merged_raw["NIVEL 1"].str.strip().str.lower() if "NIVEL 1" in merged_raw.columns else pd.Series([""] * len(merged_raw))
            n1_ayer = merged_raw["NIVEL 1_AYER"].str.strip().str.lower() if "NIVEL 1_AYER" in merged_raw.columns else pd.Series([""] * len(merged_raw))
            url_hoy  = merged_raw.get("URL IMAGEN",  pd.Series([""] * len(merged_raw)))
            url_ayer = merged_raw.get("URL IMAGEN_AYER", pd.Series([""] * len(merged_raw)))

            mask_s6 = (
                (n1_ayer == "catalogo completo") &
                (n1_hoy  != "catalogo completo") &
                (url_hoy == url_ayer)
            )
            s6_cols = ["SKU", "NOMBRE DE PRODUCTO", "NIVEL 1", "NIVEL 1_AYER", "URL IMAGEN", "URL IMAGEN_AYER"]
            df_s6 = merged_raw[mask_s6][
                [c for c in s6_cols if c in merged_raw.columns]
            ].reset_index(drop=True)

            if len(df_s6):
                st.warning(f"**{len(df_s6)} SKUs** cambiaron de categoría pero mantienen la misma URL de imagen.")
                st.dataframe(df_s6, use_container_width=True, hide_index=True)
            else:
                st.success("No se encontraron SKUs con URL de imagen desactualizada.")
        except Exception as exc:
            st.warning(f"Sección 6 no disponible — columna faltante: {exc}")


def render_productivity_tab(diseno_df: pd.DataFrame | None, edicion_df: pd.DataFrame | None) -> None:
    """Render the 👥 Team Productivity tab content (sections A–F)."""

    both = diseno_df is not None and edicion_df is not None
    diseno_ids: set = set(diseno_df["<ID>"]) if diseno_df is not None else set()
    edicion_ids: set = set(edicion_df["<ID>"]) if edicion_df is not None else set()

    # ── A. SKUs por Usuario ──────────────────────────────────────────────────
    st.subheader("A. SKUs por Usuario")
    try:
        frames = []
        if diseno_df is not None:
            frames.append(diseno_df[["Usuario", "<ID>"]].copy())
            if "Usuario Promueve desde Catalogo" in diseno_df.columns:
                frames.append(
                    diseno_df[["Usuario Promueve desde Catalogo", "<ID>"]]
                    .rename(columns={"Usuario Promueve desde Catalogo": "Usuario"})
                    .dropna(subset=["Usuario"])
                )
        if edicion_df is not None:
            frames.append(edicion_df[["Usuario", "<ID>"]].copy())
            if "Usuario Promueve desde Compras" in edicion_df.columns:
                frames.append(
                    edicion_df[["Usuario Promueve desde Compras", "<ID>"]]
                    .rename(columns={"Usuario Promueve desde Compras": "Usuario"})
                    .dropna(subset=["Usuario"])
                )

        if frames:
            combined = pd.concat(frames).drop_duplicates(["Usuario", "<ID>"])
            by_user = (
                combined.groupby("Usuario")["<ID>"]
                .count()
                .reset_index(name="SKUs Únicos")
                .sort_values("SKUs Únicos", ascending=False)
            )
            st.metric("Total SKUs únicos trabajados", int(combined["<ID>"].nunique()))
            st.dataframe(by_user, hide_index=True, use_container_width=True)
            st.bar_chart(by_user.set_index("Usuario")["SKUs Únicos"])
    except Exception as exc:
        st.warning(f"No se pudo calcular SKUs por Usuario: {exc}")

    st.divider()

    # ── B. SKUs Repetidos entre Equipos ────────────────────────────────────
    st.subheader("B. SKUs Repetidos entre Equipos")
    if both:
        try:
            repeated_ids = diseno_ids & edicion_ids
            st.metric("SKUs en ambos equipos", len(repeated_ids))
            if repeated_ids:
                rep_df = (
                    diseno_df[diseno_df["<ID>"].isin(repeated_ids)][["<ID>", "<Name>", "Categoría"]]
                    .drop_duplicates("<ID>")
                    .reset_index(drop=True)
                )
                st.dataframe(rep_df, hide_index=True, use_container_width=True)
        except Exception as exc:
            st.warning(f"No se pudo calcular SKUs repetidos: {exc}")
    else:
        st.info("ℹ️ Sube ambos archivos de productividad para ver SKUs repetidos.")

    st.divider()

    # ── C. SKUs que NO pasaron por ambos equipos ───────────────────────────
    st.subheader("C. SKUs que NO pasaron por ambos equipos")
    if both:
        try:
            solo_d_ids = diseno_ids - edicion_ids
            solo_e_ids = edicion_ids - diseno_ids
            col_c1, col_c2 = st.columns(2)
            with col_c1:
                st.caption(f"Solo Diseño — {len(solo_d_ids):,} SKUs")
                solo_d = diseno_df[diseno_df["<ID>"].isin(solo_d_ids)]
                cat_d = (
                    solo_d.groupby("Categoría")["<ID>"]
                    .count()
                    .reset_index(name="Cantidad")
                    .sort_values("Cantidad", ascending=False)
                )
                st.dataframe(cat_d, hide_index=True, use_container_width=True)
            with col_c2:
                st.caption(f"Solo Edición — {len(solo_e_ids):,} SKUs")
                solo_e = edicion_df[edicion_df["<ID>"].isin(solo_e_ids)]
                cat_e = (
                    solo_e.groupby("Categoría")["<ID>"]
                    .count()
                    .reset_index(name="Cantidad")
                    .sort_values("Cantidad", ascending=False)
                )
                st.dataframe(cat_e, hide_index=True, use_container_width=True)
        except Exception as exc:
            st.warning(f"No se pudo calcular SKUs por equipo: {exc}")
    else:
        st.info("ℹ️ Sube ambos archivos de productividad para ver esta sección.")

    st.divider()

    # ── D. SKUs con Inventario Omnicanal ────────────────────────────────────
    st.subheader("D. SKUs con Inventario Omnicanal")
    try:
        omni_frames = []
        for df, team in [(diseno_df, "Diseño"), (edicion_df, "Edición")]:
            if df is not None:
                omni = df[pd.to_numeric(df["Total Omnicanal"], errors="coerce") > 0].copy()
                omni = omni.assign(Team=team)[["<ID>", "<Name>", "Team", "Total Omnicanal"]]
                omni_frames.append(omni)
        if omni_frames:
            all_omni = pd.concat(omni_frames).drop_duplicates("<ID>").reset_index(drop=True)
            st.metric("SKUs únicos con inventario omnicanal", len(all_omni))
            st.dataframe(all_omni, hide_index=True, use_container_width=True)
        else:
            st.info("Sin SKUs con inventario omnicanal.")
    except Exception as exc:
        st.warning(f"No se pudo calcular inventario omnicanal: {exc}")

    st.divider()

    # ── E. SKUs Únicos del Día ───────────────────────────────────────────────
    st.subheader("E. SKUs Únicos del Día")
    try:
        all_ids = diseno_ids | edicion_ids
        st.metric("Total SKUs únicos trabajados hoy", len(all_ids))
        if both:
            only_d = len(diseno_ids - edicion_ids)
            both_t = len(diseno_ids & edicion_ids)
            only_e = len(edicion_ids - diseno_ids)
            m1, m2, m3 = st.columns(3)
            m1.metric("Solo Diseño", only_d)
            m2.metric("Ambos Equipos", both_t)
            m3.metric("Solo Edición", only_e)
    except Exception as exc:
        st.warning(f"No se pudo calcular SKUs únicos: {exc}")

    st.divider()

    # ── F. SKUs que Salieron del Flujo ──────────────────────────────────────
    st.subheader("F. SKUs que Salieron del Flujo")
    try:
        from datetime import timedelta
        yesterday_date = (datetime.now() - timedelta(days=1)).date()
        flujo_col = "Fecha de Salida del Flujo de trabajo"
        flujo_frames = []
        for df, team in [(diseno_df, "Diseño"), (edicion_df, "Edición")]:
            if df is not None:
                parsed = pd.to_datetime(df[flujo_col], errors="coerce").dt.date
                mask = parsed == yesterday_date
                flujo = df[mask].copy()
                flujo["Team"] = team
                cols = ["<ID>", "<Name>", "Categoría", "Usuario", flujo_col, "Total Omnicanal", "Team"]
                flujo_frames.append(flujo[[c for c in cols if c in flujo.columns]])
        if flujo_frames:
            flujo_all = (
                pd.concat(flujo_frames)
                .sort_values(flujo_col, ascending=False)
                .reset_index(drop=True)
            )
            con_inv = int((pd.to_numeric(flujo_all.get("Total Omnicanal", pd.Series([])), errors="coerce") > 0).sum())
            st.caption(f"Fecha filtrada: {yesterday_date}")
            col_f1, col_f2 = st.columns(2)
            col_f1.metric("SKUs que salieron del flujo ayer", len(flujo_all))
            col_f2.metric("Con inventario omnicanal", con_inv)
            st.dataframe(flujo_all, hide_index=True, use_container_width=True)
        else:
            st.info(f"Ningún SKU salió del flujo el {yesterday_date}.")
    except Exception as exc:
        st.warning(f"No se pudo calcular salida del flujo: {exc}")


# --- RESOLVE EFFECTIVE SOURCES (manual upload overrides OneDrive) ---
effective_today = today_file or (
    NamedBytesIO(od_data["today"], od_data["today_name"]) if od_data.get("today") else None
)
effective_yesterday = yesterday_file or (
    NamedBytesIO(od_data["yesterday"], od_data["yesterday_name"]) if od_data.get("yesterday") else None
)

# Load productivity DataFrames now so they're available for sidebar status + productivity tab
diseno_df: pd.DataFrame | None = None
edicion_df: pd.DataFrame | None = None
try:
    if diseno_file:
        diseno_df = load_productivity_file(diseno_file)
    elif od_data.get("diseno"):
        diseno_df = load_productivity_file(od_data["diseno"])
except Exception:
    diseno_df = None
try:
    if edicion_file:
        edicion_df = load_productivity_file(edicion_file)
    elif od_data.get("edicion"):
        edicion_df = load_productivity_file(od_data["edicion"])
except Exception:
    edicion_df = None

has_productivity = diseno_df is not None or edicion_df is not None

# --- PROCESSING ---
if effective_today and effective_yesterday:
    try:
        # Load data
        with st.spinner("Loading files..."):
            today_raw = load_uploaded_file(effective_today)
            yesterday_raw = load_uploaded_file(effective_yesterday)

        # Validate
        validate_dataframe(today_raw, effective_today.name)
        validate_dataframe(yesterday_raw, effective_yesterday.name)

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
        tab_names.append("📦 Inventario Omnicanal")
        if has_productivity:
            tab_names.append("👥 Team Productivity")
        tabs = st.tabs(tab_names)

        for tab, name in zip(tabs, tab_names):
            with tab:
                if name == "📦 Inventario Omnicanal":
                    render_inventory_tab(today_raw, today, yesterday_raw)
                elif name == "👥 Team Productivity":
                    render_productivity_tab(diseno_df, edicion_df)
                else:
                    df = sheets[name]
                    if len(df) > 0:
                        st.dataframe(df, use_container_width=True, hide_index=True)
                    else:
                        st.info(f"No {name.lower()} found.")

        # --- BUILD PRODUCTIVITY SHEETS FOR EXCEL (separate from tab sheets) ---
        prod_sheets: dict[str, pd.DataFrame] = {}
        if has_productivity:
            try:
                xl_user_frames = []
                if diseno_df is not None:
                    xl_user_frames.append(diseno_df[["Usuario", "<ID>"]].copy())
                    if "Usuario Promueve desde Catalogo" in diseno_df.columns:
                        xl_user_frames.append(
                            diseno_df[["Usuario Promueve desde Catalogo", "<ID>"]]
                            .rename(columns={"Usuario Promueve desde Catalogo": "Usuario"})
                            .dropna(subset=["Usuario"])
                        )
                if edicion_df is not None:
                    xl_user_frames.append(edicion_df[["Usuario", "<ID>"]].copy())
                    if "Usuario Promueve desde Compras" in edicion_df.columns:
                        xl_user_frames.append(
                            edicion_df[["Usuario Promueve desde Compras", "<ID>"]]
                            .rename(columns={"Usuario Promueve desde Compras": "Usuario"})
                            .dropna(subset=["Usuario"])
                        )
                if xl_user_frames:
                    xl_combined = pd.concat(xl_user_frames).drop_duplicates(["Usuario", "<ID>"])
                    uf = (
                        xl_combined.groupby("Usuario")["<ID>"]
                        .count()
                        .reset_index(name="SKUs Únicos")
                        .sort_values("SKUs Únicos", ascending=False)
                    )
                    prod_sheets["SKUs por Usuario"] = uf.reset_index(drop=True)

                if diseno_df is not None and edicion_df is not None:
                    _diseno_ids = set(diseno_df["<ID>"])
                    _edicion_ids = set(edicion_df["<ID>"])
                    repeated_ids = _diseno_ids & _edicion_ids
                    prod_sheets["SKUs Repetidos"] = (
                        diseno_df[diseno_df["<ID>"].isin(repeated_ids)][["<ID>", "<Name>", "Categoría"]]
                        .drop_duplicates("<ID>")
                        .reset_index(drop=True)
                    )
                    prod_sheets["Solo Diseño"] = diseno_df[~diseno_df["<ID>"].isin(_edicion_ids)].reset_index(drop=True)
                    prod_sheets["Solo Edición"] = edicion_df[~edicion_df["<ID>"].isin(_diseno_ids)].reset_index(drop=True)

                flujo_col = "Fecha de Salida del Flujo de trabajo"
                flujo_frames_xl = []
                for _df, _team in [(diseno_df, "Diseño"), (edicion_df, "Edición")]:
                    if _df is not None:
                        try:
                            mask = _df[flujo_col].notna() & (_df[flujo_col].astype(str).str.strip() != "")
                            fl = _df[mask].copy()
                            fl["Team"] = _team
                            flujo_frames_xl.append(fl[["<ID>", "<Name>", "Categoría", "Usuario", flujo_col, "Team"]])
                        except Exception:
                            pass
                if flujo_frames_xl:
                    prod_sheets["Salieron del Flujo"] = pd.concat(flujo_frames_xl).reset_index(drop=True)
            except Exception:
                pass  # Never block the download due to productivity errors

        # --- INVENTORY SHEETS FOR EXCEL ─────────────────────────────────────
        inv_sheets: dict[str, pd.DataFrame] = {}
        try:
            _has_stock   = yesno(safe_get(today_raw, "TIENE STOCK")) == 1
            _is_visible  = yesno(safe_get(today_raw, "VISIBLE")) == 1
            _nivel1      = safe_get(today_raw, "NIVEL 1").str.strip().str.lower()
            _temp        = safe_get(today_raw, "TEMPORADA ERP").str.strip().str.lower()
            _modal_empty = ~nonempty(safe_get(today_raw, "MODAL")).astype(bool)
            _no_img      = (
                ~nonempty(safe_get(today_raw, "TIENE IMAGEN")).astype(bool) |
                (safe_get(today_raw, "TIENE IMAGEN").str.strip().str.lower() == "no")
            )
            _disabled    = safe_get(today_raw, "HABILITADO/DESHABILITADO").str.lower().str.contains("deshab", na=False)

            inv_sheets["Stock No Visible"] = today_raw[_has_stock & ~_is_visible].reset_index(drop=True)
            inv_sheets["Tipo C con Stock"]  = today_raw[(_nivel1 == "catalogo completo") & _has_stock].reset_index(drop=True)
            inv_sheets["No Fisicos"]        = today_raw[_nivel1.str.contains("mesa de regalos|certificado de regalo", na=False)].reset_index(drop=True)
            inv_sheets["Long Tail Sin Modal"] = today_raw[(_temp == "long tail proveedor") & _modal_empty].reset_index(drop=True)
            inv_sheets["Sin Imagen Deshabilitado"] = today_raw[_no_img & _disabled].reset_index(drop=True)

            if yesterday_raw is not None:
                try:
                    t_xl = [c for c in ["SKU", "NOMBRE DE PRODUCTO", "NIVEL 1", "URL IMAGEN"] if c in today_raw.columns]
                    y_xl = [c for c in ["SKU", "NIVEL 1", "URL IMAGEN"] if c in yesterday_raw.columns]
                    _merged_xl = today_raw[t_xl].merge(
                        yesterday_raw[y_xl].rename(columns={"NIVEL 1": "NIVEL 1_AYER", "URL IMAGEN": "URL IMAGEN_AYER"}),
                        on="SKU", how="inner",
                    )
                    _n1h = _merged_xl["NIVEL 1"].str.strip().str.lower() if "NIVEL 1" in _merged_xl.columns else pd.Series([""] * len(_merged_xl))
                    _n1a = _merged_xl["NIVEL 1_AYER"].str.strip().str.lower() if "NIVEL 1_AYER" in _merged_xl.columns else pd.Series([""] * len(_merged_xl))
                    _url_h = _merged_xl.get("URL IMAGEN",     pd.Series([""] * len(_merged_xl)))
                    _url_a = _merged_xl.get("URL IMAGEN_AYER", pd.Series([""] * len(_merged_xl)))
                    _m6 = (_n1a == "catalogo completo") & (_n1h != "catalogo completo") & (_url_h == _url_a)
                    inv_sheets["URL No Actualizada"] = _merged_xl[_m6].reset_index(drop=True)
                except Exception:
                    pass
        except Exception:
            pass  # Never block the download due to inventory sheet errors

        # --- DOWNLOAD BUTTON ---
        st.header("📥 Download Report")

        excel_data = generate_excel_download({**sheets, **prod_sheets, **inv_sheets})
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

elif has_productivity:
    # Catalog not loaded yet, but productivity files are — show productivity tab standalone
    st.info("👆 Upload both catalog files above to generate the delta report.")
    st.header("👥 Team Productivity")
    render_productivity_tab(diseno_df, edicion_df)

else:
    # Show instructions when no files uploaded
    st.info("👆 Upload both catalog files above to generate the report.")

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
