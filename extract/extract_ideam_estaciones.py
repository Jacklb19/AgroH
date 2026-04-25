import requests
import pandas as pd
import logging
from config.settings import SOURCES, DATA_RAW

logger = logging.getLogger(__name__)

def extract_estaciones() -> pd.DataFrame:
    """Descarga el catálogo de estaciones meteorológicas del IDEAM.
    Si ya existe localmente, lo carga desde caché. Borra el CSV para forzar re-descarga."""
    out = DATA_RAW / "ideam_estaciones_raw.csv"
    if out.exists():
        logger.info("IDEAM Estaciones: cargando desde caché local (%s)", out)
        return pd.read_csv(out, dtype=str)

    url = SOURCES["estaciones_ideam"]
    logger.info("Descargando catálogo de estaciones IDEAM...")
    rows, offset, limit = [], 0, 50000
    while True:
        params = {"$limit": limit, "$offset": offset}
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        rows.extend(batch)
        offset += limit

    df = pd.DataFrame(rows)
    df.to_csv(out, index=False)
    logger.info("IDEAM Estaciones: %s registros -> %s", len(df), out)
    return df
