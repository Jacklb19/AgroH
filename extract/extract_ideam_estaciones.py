import requests
import pandas as pd
import logging
from config.settings import SOURCES, DATA_RAW, SOCRATA_TOKEN

logger = logging.getLogger(__name__)

def extract_estaciones() -> pd.DataFrame:
    """
    Descarga el catálogo de estaciones meteorológicas del IDEAM.
    # FIX v1: Manejo de errores granular, token Socrata y cache local.
    """
    url = SOURCES["estaciones_ideam"]
    out_dir = DATA_RAW / "ideam"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "ideam_estaciones_raw.parquet"
    
    rows, offset, limit = [], 0, 50000
    headers = {"X-App-Token": SOCRATA_TOKEN} if SOCRATA_TOKEN else {}
    
    logger.info("IDEAM Estaciones: iniciando descarga desde %s", url)
    
    while True:
        params = {"$limit": limit, "$offset": offset}
        try:
            r = requests.get(url, params=params, headers=headers, timeout=120)
            r.raise_for_status()
            batch = r.json()
        except requests.exceptions.Timeout:
            logger.error("IDEAM Estaciones: timeout en offset %s", offset)
            break
        except requests.exceptions.HTTPError as e:
            logger.error("IDEAM Estaciones: HTTP %s — %s", e.response.status_code, e.response.text[:200])
            break
        except Exception as e:
            logger.error("IDEAM Estaciones: error inesperado en offset %s: %s", offset, e)
            break

        if not isinstance(batch, list):
            logger.warning("IDEAM Estaciones: respuesta inesperada (no es lista) en offset %s: %s", offset, str(batch)[:200])
            break
            
        if not batch:
            break
            
        rows.extend(batch)
        offset += limit
        logger.info("IDEAM Estaciones: %s registros descargados...", len(rows))
    
    if not rows:
        if out_file.exists():
            logger.warning("IDEAM Estaciones: descarga fallida, usando cache local: %s", out_file)
            return pd.read_parquet(out_file)
        # Fallback a CSV si el parquet no existe pero el CSV sí (transición)
        out_csv = out_dir / "ideam_estaciones_raw.csv"
        if out_csv.exists():
            logger.warning("IDEAM Estaciones: usando cache local (CSV): %s", out_csv)
            return pd.read_csv(out_csv)
        logger.error("IDEAM Estaciones: no se obtuvieron datos y no hay cache.")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    try:
        df.to_parquet(out_file, index=False)
        logger.info("IDEAM Estaciones: %s registros -> %s", len(df), out_file)
    except Exception as e:
        logger.error("IDEAM Estaciones: error guardando parquet: %s", e)
        df.to_csv(out_file.with_suffix(".csv"), index=False)
        
    return df
