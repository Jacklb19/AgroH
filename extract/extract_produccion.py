import requests
import pandas as pd
import logging
from config.settings import SOURCES, DATA_RAW, YEAR_START, YEAR_END

logger = logging.getLogger(__name__)

def extract_produccion() -> pd.DataFrame:
    """Descarga A04/A05 desde datos.gov.co usando la API Socrata con paginación."""
    url = SOURCES["produccion_datosgov"]
    rows, offset, limit = [], 0, 50000
    logger.info("Descargando producción agrícola A04/A05...")
    while True:
        params = {
            "$limit": limit,
            "$offset": offset,
            "$where": f"a_o >= {YEAR_START} AND a_o <= {YEAR_END}",
        }
        r = requests.get(url, params=params, timeout=60)
        if r.status_code != 200:
            logger.error(f"Error de la API de Socrata (producción): {r.text}")
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        rows.extend(batch)
        offset += limit
        logger.info(f"  {len(rows)} registros descargados...")
    df = pd.DataFrame(rows)
    out = DATA_RAW / "produccion_agricola_raw.csv"
    df.to_csv(out, index=False)
    logger.info(f"A04/A05: {len(df)} registros -> {out}")
    return df
