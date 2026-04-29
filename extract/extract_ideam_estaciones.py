"""
extract_ideam_estaciones.py — Descarga del catálogo de estaciones meteorológicas IDEAM.

Correcciones aplicadas:
  - Corrección 1.5 (2026-04-29): Retry con backoff exponencial via utils/http.py.
"""
import logging

import pandas as pd
import requests

from config.settings import SOURCES, DATA_RAW, SOCRATA_TOKEN
from utils.http import fetch_with_retry

logger = logging.getLogger(__name__)


def _fetch_page(url, params, headers, timeout=120):
    """Descarga una página de la API Socrata."""
    r = requests.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()


def extract_estaciones() -> pd.DataFrame:
    """
    Descarga el catálogo de estaciones meteorológicas del IDEAM.
    Corrección 1.5: Usa fetch_with_retry para reintentos con backoff exponencial.
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
            logger.error("IDEAM Estaciones: error de red en offset %s después de reintentos: %s", offset, e)
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
