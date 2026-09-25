import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CATALOG_COLS = [
    "SKU", "NOMBRE DE PRODUCTO", "NOMBRE DE SKU", "DESCRIPCION ERP", "MARCA",
    "TIENE PRECIO", "PRECIO", "STOCK", "TIENE STOCK", "VISIBLE",
    "HABILITADO/DESHABILITADO", "NIVEL 1", "NIVEL 2", "NIVEL 3",
    "TIENE IMAGEN", "IMAGEN PRIMARIA", "URL IMAGEN", "TEMPORADA ERP", "MODAL",
]


def sku(sku_id: str, **overrides) -> dict:
    """A complete, visible, in-stock, enabled SKU (score 95) with overrides."""
    row = {
        "SKU": sku_id,
        "NOMBRE DE PRODUCTO": f"Producto {sku_id}",
        "NOMBRE DE SKU": f"Producto {sku_id}",
        "DESCRIPCION ERP": "DESC",
        "MARCA": "Marca",
        "TIENE PRECIO": "Si",
        "PRECIO": "10.5",
        "STOCK": "5",
        "TIENE STOCK": "Si",
        "VISIBLE": "Si",
        "HABILITADO/DESHABILITADO": "Habilitado",
        "NIVEL 1": "Hogar",
        "NIVEL 2": "Sala",
        "NIVEL 3": "Cojines",
        "TIENE IMAGEN": "Si",
        "IMAGEN PRIMARIA": "Si",
        "URL IMAGEN": f"https://img/{sku_id}.jpg",
        "TEMPORADA ERP": "Línea Regular",
        "MODAL": "",
    }
    row.update(overrides)
    return row


def catalog(*rows: dict) -> pd.DataFrame:
    df = pd.DataFrame(list(rows), columns=CATALOG_COLS)
    # Blank cells arrive as NaN from the Excel reader
    return df.replace("", np.nan)


@pytest.fixture
def make_sku():
    return sku


@pytest.fixture
def make_catalog():
    return catalog
