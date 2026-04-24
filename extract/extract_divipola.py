import requests
import pandas as pd
import logging
from config.settings import SOURCES, DATA_RAW

logger = logging.getLogger(__name__)

def extract_divipola() -> pd.DataFrame:
    """Descarga el catálogo DIVIPOLA completo desde datos.gov.co (Socrata)."""
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
    out = DATA_RAW / "divipola.csv"
    df.to_csv(out, index=False)
    logger.info(f"DIVIPOLA: {len(df)} municipios → {out}")
    return df
