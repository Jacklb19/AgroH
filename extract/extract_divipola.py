import requests
import pandas as pd
import logging
from config.settings import SOURCES, DATA_RAW

logger = logging.getLogger(__name__)

def extract_divipola() -> pd.DataFrame:
    """Descarga el catálogo DIVIPOLA completo desde datos.gov.co (Socrata).
    Si ya existe localmente, lo carga desde caché. Borra el CSV para forzar re-descarga."""
    out = DATA_RAW / "divipola.csv"
    if out.exists():
        logger.info("DIVIPOLA: cargando desde caché local (%s)", out)
        return pd.read_csv(out, dtype=str)

    url = SOURCES["divipola"]
    rows, offset, limit = [], 0, 1000
    logger.info("Descargando DIVIPOLA...")
    while True:
        r = requests.get(url, params={"$limit": limit, "$offset": offset}, timeout=30)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        rows.extend(batch)
        offset += limit
    df = pd.DataFrame(rows)
    df.to_csv(out, index=False)
    logger.info("DIVIPOLA: %s municipios -> %s", len(df), out)
    return df
