import requests
import pandas as pd
import logging
from config.settings import SOURCES, DATA_RAW, YEAR_START, YEAR_END, SOCRATA_TOKEN

logger = logging.getLogger(__name__)

def extract_produccion() -> pd.DataFrame:
    """
    Descarga A04/A05 desde datos.gov.co usando la API Socrata con paginación.
    # FIX v1: Manejo de errores, token Socrata, cache incremental y validación de respuesta.
    """
    url = SOURCES["produccion_datosgov"]
    out_dir = DATA_RAW / "produccion"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "produccion_agricola_raw.parquet"
    
    rows, offset, limit = [], 0, 50000
    headers = {"X-App-Token": SOCRATA_TOKEN} if SOCRATA_TOKEN else {}
    
    logger.info("Producción: iniciando descarga desde %s", url)
    
    while True:
        params = {
            "$limit": limit,
            "$offset": offset,
            "$where": f"a_o >= {YEAR_START} AND a_o <= {YEAR_END}",
        }
        
        try:
            r = requests.get(url, params=params, headers=headers, timeout=120)
            r.raise_for_status()
            batch = r.json()
        except requests.exceptions.Timeout:
            logger.error("Producción: timeout en offset %s", offset)
            break
        except requests.exceptions.HTTPError as e:
            logger.error("Producción: HTTP %s — %s", e.response.status_code, e.response.text[:200])
            break
        except Exception as e:
            logger.error("Producción: error inesperado en offset %s: %s", offset, e)
            break

        if not isinstance(batch, list):
            logger.warning("Producción: respuesta inesperada (no es lista) en offset %s: %s", offset, str(batch)[:200])
            break
            
        if not batch:
            break
            
        rows.extend(batch)
        offset += limit
        logger.info("Producción: %s registros descargados...", len(rows))

    if not rows:
        if out_file.exists():
            logger.warning("Producción: descarga fallida, usando cache local: %s", out_file)
            return pd.read_parquet(out_file)
        logger.error("Producción: no se obtuvieron datos y no hay cache.")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    
    try:
        df.to_parquet(out_file, index=False)
        logger.info("Producción: %s registros -> %s", len(df), out_file)
    except Exception as e:
        logger.error("Producción: error guardando parquet: %s", e)
        # Fallback a CSV si parquet falla por alguna razón
        out_file_csv = out_file.with_suffix(".csv")
        df.to_csv(out_file_csv, index=False)
        logger.info("Producción: %s registros -> %s (fallback CSV)", len(df), out_file_csv)

    return df
