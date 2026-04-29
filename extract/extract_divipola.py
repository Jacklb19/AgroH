import requests
import pandas as pd
import logging
from config.settings import SOURCES, DATA_RAW, SOCRATA_TOKEN

logger = logging.getLogger(__name__)

def extract_divipola() -> pd.DataFrame:
    """
    Descarga el catálogo DIVIPOLA completo desde datos.gov.co (Socrata).
    # FIX v1: Token Socrata, timeouts, verificación de respuesta y manejo de errores granular.
    """
    out_dir = DATA_RAW / "divipola"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "divipola_raw.parquet"
    
    url = SOURCES["divipola"]
    rows, offset, limit = [], 0, 1000
    headers = {"X-App-Token": SOCRATA_TOKEN} if SOCRATA_TOKEN else {}
    
    logger.info("DIVIPOLA: iniciando descarga desde %s", url)
    
    try:
        while True:
            try:
                r = requests.get(url, params={"$limit": limit, "$offset": offset}, 
                                 headers=headers, timeout=120)
                r.raise_for_status()
                batch = r.json()
            except requests.exceptions.Timeout:
                logger.error("DIVIPOLA: timeout en offset %s", offset)
                break
            except requests.exceptions.HTTPError as e:
                logger.error("DIVIPOLA: HTTP %s — %s", e.response.status_code, e.response.text[:200])
                break

            if not isinstance(batch, list):
                logger.warning("DIVIPOLA: respuesta inesperada (no es lista): %s", str(batch)[:200])
                break
                
            if not batch:
                break
            rows.extend(batch)
            offset += limit
            
        if rows:
            df = pd.DataFrame(rows)
            df.to_parquet(out_file, index=False)
            logger.info("DIVIPOLA: %s municipios -> %s", len(df), out_file)
            return df
        else:
            raise ConnectionError("No se obtuvieron registros de DIVIPOLA")

    except Exception as e:
        if out_file.exists():
            logger.warning("DIVIPOLA: fallo de red (%s), usando cache local: %s", e, out_file)
            return pd.read_parquet(out_file)
        
        # Fallback a CSV si existe (transición)
        out_csv = DATA_RAW / "divipola.csv"
        if out_csv.exists():
            logger.warning("DIVIPOLA: usando cache local (CSV): %s", out_csv)
            return pd.read_csv(out_csv, dtype=str)
            
        logger.error("DIVIPOLA: error fatal en descarga y no hay cache: %s", e)
        return pd.DataFrame()
