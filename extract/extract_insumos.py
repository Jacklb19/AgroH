"""
extract_insumos.py — Fuente A07: Precios de Insumos Agrícolas (DANE / UPRA)
Prioridad: API datos.gov.co -> archivos manuales -> datos sintéticos IPIA.
"""
import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests

from config.settings import MANUAL_DATA_DIR, SOURCES

logger = logging.getLogger(__name__)

_API_ENDPOINTS = [
    SOURCES.get("insumos_ipia", "https://www.datos.gov.co/resource/y5zy-x4ky.json"),
    "https://www.datos.gov.co/resource/4td6-4v3h.json",
    "https://www.datos.gov.co/resource/t4ep-xtez.json",
]
_LIMIT = 50_000


def _fetch_api() -> pd.DataFrame:
    for url in _API_ENDPOINTS:
        try:
            params = {"$limit": _LIMIT, "$offset": 0}
            rows: list = []
            while True:
                resp = requests.get(url, params=params, timeout=30)
                resp.raise_for_status()
                batch = resp.json()
                if not batch or not isinstance(batch, list):
                    break
                rows.extend(batch)
                if len(batch) < _LIMIT:
                    break
                params["$offset"] += _LIMIT
            if rows:
                df = pd.DataFrame(rows)
                df["fuente_origen"] = url
                df["es_sintetico"] = False
                logger.info("Insumos API: %s registros desde %s", len(df), url)
                return df
        except Exception as exc:
            logger.debug("Insumos endpoint %s no disponible: %s", url, exc)
    return pd.DataFrame()


def _load_manual_files() -> pd.DataFrame:
    base = MANUAL_DATA_DIR / "insumos"
    if not base.exists():
        return pd.DataFrame()
    files: list[Path] = []
    for pattern in ("*.csv", "*.xlsx", "*.xls", "*.parquet"):
        files.extend(sorted(base.glob(pattern)))
    if not files:
        return pd.DataFrame()

    frames = []
    for path in files:
        logger.info("Leyendo archivo de insumos: %s", path.name)
        suffix = path.suffix.lower()
        if suffix == ".csv":
            frames.append(pd.read_csv(path))
        elif suffix == ".parquet":
            frames.append(pd.read_parquet(path))
        else:
            frames.append(pd.read_excel(path))

    result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not result.empty:
        result["fuente_origen"] = "manual"
        result["es_sintetico"] = False
    logger.info("Insumos manuales: %s registros", len(result))
    return result


def _generate_synthetic_ipia() -> pd.DataFrame:
    """
    Genera serie temporal sintética de precios de insumos agrícolas (2020-2025).
    Basada en tendencias históricas del IPIA-DANE: inflación ~10-15% anual.
    Usado como último recurso cuando no hay datos reales disponibles.
    """
    dates = pd.date_range("2020-01", f"{datetime.now().year}-12", freq="MS")
    regiones = ["Nacional", "Andina", "Caribe", "Pacífico", "Orinoquía", "Amazonía"]

    insumos = [
        ("fertilizante",  "Urea (46-0-0)",           1_300_000, "ton"),
        ("fertilizante",  "DAP (18-46-0)",            1_800_000, "ton"),
        ("fertilizante",  "Cloruro de Potasio KCl",   1_500_000, "ton"),
        ("fertilizante",  "Cal Dolomita",                180_000, "ton"),
        ("agroquimico",   "Glifosato herbicida",          42_000, "litro"),
        ("agroquimico",   "Fungicida clorotalonil",        85_000, "litro"),
        ("agroquimico",   "Insecticida clorpirifos",       68_000, "litro"),
        ("mano_de_obra",  "Jornal rural",                  40_000, "jornal"),
        ("semilla",       "Semilla Maiz",                   8_000, "kg"),
        ("semilla",       "Semilla Arroz",                  3_500, "kg"),
        ("semilla",       "Semilla Papa",                   1_200, "kg"),
        ("combustible",   "ACPM",                           3_800, "litro"),
        ("indice",        "IPIA Nacional",                  100.0, "indice"),
    ]

    rng = np.random.default_rng(42)
    rows = []

    for tipo, nombre, precio_base, unidad in insumos:
        target_regiones = ["Nacional"] if tipo == "indice" else regiones
        for region in target_regiones:
            precio = float(precio_base)
            for fecha in dates:
                annual_growth = rng.uniform(0.08, 0.16)
                monthly_factor = (1 + annual_growth) ** (1 / 12)
                noise = rng.normal(1.0, 0.015)
                precio = precio * monthly_factor * noise
                rows.append({
                    "fecha":             fecha,
                    "tipo_insumo":       tipo,
                    "nombre_insumo":     nombre,
                    "precio_cop_unidad": round(precio, 2),
                    "unidad_medida":     unidad,
                    "region":            region,
                })

    df = pd.DataFrame(rows)
    df["fuente_origen"] = "IPIA sintetico"
    df["es_sintetico"] = True
    logger.info(
        "Insumos A07: %s registros sintéticos generados (API y archivos manuales no disponibles)",
        len(df),
    )
    return df


def extract_insumos() -> pd.DataFrame:
    """
    Extrae precios de insumos agrícolas A07.
    Prioridad: API datos.gov.co -> archivos en data/raw/manual/insumos/ -> serie sintética IPIA.
    """
    df = _fetch_api()
    if df.empty:
        df = _load_manual_files()
    if df.empty:
        logger.warning(
            "Insumos A07: sin datos reales disponibles — usando serie sintética IPIA. "
            "Para datos reales coloca archivos CSV/Excel en data/raw/manual/insumos/"
        )
        df = _generate_synthetic_ipia()

    if df.empty:
        return pd.DataFrame()

    out = MANUAL_DATA_DIR.parent / "insumos_raw_consolidado.parquet"
    df.to_parquet(out, index=False)
    logger.info("Insumos raw -> %s (%s registros)", out, len(df))
    return df
