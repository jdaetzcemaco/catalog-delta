"""
Catalog Delta — web app.

    streamlit run streamlit_app.py

Reads the results produced by the processing job (python -m pipeline). Locally:
    STORAGE=local LOCAL_ROOT=data/onedrive streamlit run streamlit_app.py
"""

from __future__ import annotations

import hmac
import os

import streamlit as st

st.set_page_config(page_title="Catalog Delta · Cemaco", page_icon="📊", layout="wide")

from app import data, ui  # noqa: E402


def _password() -> str:
    try:
        secret = st.secrets.get("app_password", "")
    except Exception:  # no secrets.toml
        secret = ""
    return os.environ.get("APP_PASSWORD") or secret


def login() -> None:
    expected = _password()
    if not expected or st.session_state.get("authenticated"):
        st.session_state.authenticated = True
        return
    _, mid, _ = st.columns([1, 1.2, 1])
    with mid, st.container(border=True):
        st.markdown("### 📊 Catalog Delta")
        st.caption("Salud del catálogo Cemaco")
        with st.form("login"):
            pwd = st.text_input("Contraseña", type="password")
            if st.form_submit_button("Entrar", type="primary", width="stretch"):
                if hmac.compare_digest(pwd.encode(), expected.encode()):
                    st.session_state.authenticated = True
                    st.rerun()
                st.error("Contraseña incorrecta")
    st.stop()


def sidebar() -> None:
    days = data.run_days()
    with st.sidebar:
        if days:
            if st.session_state.get("day") not in days:
                st.session_state.day = days[0]
            st.selectbox("Día del catálogo", days, key="day", format_func=ui.fmt_day)
            m = data.manifest(st.session_state.day) or {}
            prev = m.get("previous_day")
            st.caption(f"Comparado con **{ui.fmt_day(prev)}**" if prev else "Primer día: sin comparación")
        st.button("↻ Actualizar datos", on_click=data.refresh, width="stretch")


login()
pages = {
    "Catálogo": [
        st.Page("app/pages/resumen.py", title="Resumen", icon="🏠", default=True),
        st.Page("app/pages/cambios.py", title="Cambios del día", icon="🔄"),
        st.Page("app/pages/inventario.py", title="Inventario Omnicanal", icon="📦"),
        st.Page("app/pages/historial.py", title="Historial", icon="📈"),
    ],
    "Equipos": [st.Page("app/pages/productividad.py", title="Productividad", icon="👥")],
    "Datos": [st.Page("app/pages/cargar.py", title="Cargar archivos", icon="📤")],
}
nav = st.navigation(pages)
sidebar()
nav.run()
