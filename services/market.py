from typing import Optional, Tuple

import pandas as pd

from db_utils import fetch_market_data, save_market_data
from market_data import descargar_datos_mercado


def load_market_data() -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """Carga datos de mercado desde SQLite."""
    return fetch_market_data()


def update_market_data(data_dir: str) -> Tuple[bool, str]:
    """Descarga y guarda datos de mercado en SQLite."""
    try:
        df = descargar_datos_mercado(data_dir)
        if df is not None:
            save_market_data(df)
            return True, "Datos actualizados correctamente"
        return False, "Error en la descarga"
    except Exception as e:
        return False, f"Error: {str(e)}"
