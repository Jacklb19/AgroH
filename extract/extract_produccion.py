"""
extract_produccion.py — Extracción de datos de producción agrícola A04/A05.

Correcciones aplicadas:
  - Corrección 1.5 (2026-04-29): Retry con backoff exponencial via utils/http.py.
"""
import logging

import pandas as pd
import requests

from config.settings import SOURCES, DATA_RAW, YEAR_START, YEAR_END, SOCRATA_TOKEN
from utils.http import fetch_with_retry

logger = logging.getLogger(__name__)


def _fetch_page(url, params, headers, timeout=120):
    """Descarga una página de la API Socrata."""
    r = requests.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()


def extract_produccion() -> pd.DataFrame:
    """
    Descarga A04/A05 desde datos.gov.co usando la API Socrata con paginación.
    Corrección 1.5: Usa fetch_with_retry para reintentos con backoff exponencial.
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
            batch = fetch_with_retry(
                _fetch_page,
                url,
                params,
                headers,
                max_retries=3,
                backoff_base=2.0,
            )
        except (requests.exceptions.Timeout, requests.exceptions.HTTPError,
                requests.exceptions.ConnectionError) as e:
            logger.error("Producción: error de red en offset %s después de reintentos: %s", offset, e)
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
