"""
extract_divipola.py — Descarga del catálogo DIVIPOLA desde datos.gov.co.

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
    """Descarga una página de la API Socrata. Wrapper para fetch_with_retry."""
    r = requests.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()


def extract_divipola() -> pd.DataFrame:
    """
    Descarga el catálogo DIVIPOLA completo desde datos.gov.co (Socrata).
    Corrección 1.5: Usa fetch_with_retry para reintentos con backoff exponencial.
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
                batch = fetch_with_retry(
                    _fetch_page,
                    url,
                    {"$limit": limit, "$offset": offset},
                    headers,
                    max_retries=3,
                    backoff_base=2.0,
                )
            except (requests.exceptions.Timeout, requests.exceptions.HTTPError,
                    requests.exceptions.ConnectionError) as e:
                logger.error("DIVIPOLA: error de red en offset %s después de reintentos: %s", offset, e)
                break
            except Exception as e:
                logger.error("DIVIPOLA: error inesperado en offset %s: %s", offset, e)
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
