"""
utils/http.py — Utilidades HTTP compartidas para el pipeline AgroIA.

Corrección 1.5 (2026-04-29): Función fetch_with_retry con backoff exponencial.
"""
import logging
import time
from typing import Any, Callable

import requests

logger = logging.getLogger(__name__)


def fetch_with_retry(
    func: Callable,
    *args,
    max_retries: int = 3,
    backoff_base: float = 2.0,
    **kwargs
) -> Any:
    """
    Ejecuta func(*args, **kwargs) con reintentos y backoff exponencial.
    Espera backoff_base^intento segundos entre reintentos.
    Lanza la última excepción si se agotan los intentos.
    """
    last_exc = None
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except (requests.Timeout, requests.HTTPError,
                requests.ConnectionError) as e:
            last_exc = e
            wait = backoff_base ** attempt
            logger.warning(
                "Intento %s/%s fallido: %s. "
                "Reintentando en %.1fs...",
                attempt + 1, max_retries, e, wait
            )
            time.sleep(wait)
    raise last_exc
