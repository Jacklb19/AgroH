"""
extract_insumos.py — Fuente A07: Precios de Insumos Agrícolas (DANE / UPRA)
Prioridad: API datos.gov.co -> archivos manuales -> datos sintéticos IPIA.

CORRECCIONES v2:
  - FIX: token de Socrata incluido en cada request para evitar throttling.
  - FIX: paginador detecta respuestas de error JSON (dict) y las loggea correctamente.
  - FIX: datos sintéticos ya no generan fechas futuras; se limitan al mes actual.
  - FIX: tasa de crecimiento anual fija por insumo/región para tendencia coherente.
  - FIX: archivo de salida usa DATA_RAW en lugar de MANUAL_DATA_DIR.parent.
  - FIX: todos los endpoints se consultan y sus resultados se fusionan y deduplicán.
  - FIX: _load_manual_files deduplica el resultado final.
  - FIX: timeout subido a 120s; errores en archivos Excel individuales no abortan
         la carga de los demás.
"""
import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests

from config.settings import DATA_RAW, MANUAL_DATA_DIR, SOURCES, SOCRATA_TOKEN

logger = logging.getLogger(__name__)

_API_ENDPOINTS = [
    SOURCES.get("insumos_ipia", "https://www.datos.gov.co/resource/y5zy-x4ky.json"),
    "https://www.datos.gov.co/resource/4td6-4v3h.json",
    "https://www.datos.gov.co/resource/t4ep-xtez.json",
]
_LIMIT   = 50_000
_TIMEOUT = 120


def _fetch_single_endpoint(url: str) -> pd.DataFrame:
    """
    Descarga todos los registros de un endpoint Socrata con paginación.
    Retorna DataFrame vacío si el endpoint no está disponible o devuelve error.
    """
    headers = {"X-App-Token": SOCRATA_TOKEN} if SOCRATA_TOKEN else {}
    params  = {"$limit": _LIMIT, "$offset": 0}
    rows: list = []

    while True:
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=_TIMEOUT)
            resp.raise_for_status()
            batch = resp.json()
        except requests.exceptions.Timeout:
            logger.warning("Insumos: timeout en %s (offset %s)", url, params["$offset"])
            break
        except requests.exceptions.HTTPError as e:
            logger.warning("Insumos: HTTP %s en %s", e.response.status_code, url)
            break
        except Exception as exc:
            logger.debug("Insumos endpoint %s no disponible: %s", url, exc)
            break

        # El servidor a veces devuelve un dict de error en lugar de lista
        if not isinstance(batch, list):
            logger.warning(
                "Insumos: respuesta inesperada en %s (offset %s): %s",
                url, params["$offset"], str(batch)[:200],
            )
            break

        if not batch:
            break

        rows.extend(batch)

        if len(batch) < _LIMIT:
            break
        params["$offset"] += _LIMIT

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["fuente_origen"] = url
    df["es_sintetico"]  = False
    logger.info("Insumos API: %s registros desde %s", len(df), url)
    return df


def _fetch_api() -> pd.DataFrame:
    """
    Consulta todos los endpoints disponibles y fusiona los resultados,
    deduplicando por todas las columnas para evitar registros repetidos
    entre fuentes que comparten datos.
    """
    frames = []
    for url in _API_ENDPOINTS:
        df = _fetch_single_endpoint(url)
        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    # Deduplicar ignorando las columnas de metadatos que difieren por fuente
    data_cols = [c for c in combined.columns if c not in ("fuente_origen", "es_sintetico")]
    before    = len(combined)
    combined  = combined.drop_duplicates(subset=data_cols, keep="first")
    dropped   = before - len(combined)
    if dropped:
        logger.info("Insumos API: %s registros duplicados eliminados entre endpoints", dropped)

    logger.info("Insumos API: total consolidado %s registros", len(combined))
    return combined


def _load_manual_files() -> pd.DataFrame:
    """
    Carga todos los archivos CSV/Excel/Parquet del directorio manual.
    Los archivos corruptos o ilegibles se omiten con un warning, sin
    abortar la carga de los demás.
    """
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
        try:
            suffix = path.suffix.lower()
            if suffix == ".csv":
                df = pd.read_csv(path)
            elif suffix == ".parquet":
                df = pd.read_parquet(path)
            else:
                df = pd.read_excel(path)
            logger.info("Insumos manual: %s registros desde %s", len(df), path.name)
            frames.append(df)
        except Exception as exc:
            logger.warning("Insumos manual: no se pudo leer %s — %s", path.name, exc)

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)

    # Deduplicar para el caso en que varios archivos se solapen
    before = len(result)
    result = result.drop_duplicates(keep="first")
    dropped = before - len(result)
    if dropped:
        logger.info("Insumos manual: %s filas duplicadas eliminadas entre archivos", dropped)

    result["fuente_origen"] = "manual"
    result["es_sintetico"]  = False
    logger.info("Insumos manuales: %s registros en total", len(result))
    return result


def _generate_synthetic_ipia() -> pd.DataFrame:
    """
    Genera serie temporal sintética de precios de insumos agrícolas.
    Basada en tendencias históricas del IPIA-DANE: inflación ~10-15% anual.

    Correcciones respecto a la versión anterior:
    - Las fechas se limitan al mes actual (no genera datos futuros).
    - La tasa de crecimiento anual es fija por insumo y región para producir
      una tendencia coherente; solo el ruido mensual es aleatorio.
    """
    now       = datetime.now()
    end_month = f"{now.year}-{now.month:02d}"
    dates     = pd.date_range("2020-01", end_month, freq="MS")

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

    rng  = np.random.default_rng(42)
    rows = []

    for tipo, nombre, precio_base, unidad in insumos:
        target_regiones = ["Nacional"] if tipo == "indice" else regiones
        for region in target_regiones:
            # Tasa anual fija para este insumo+región: tendencia coherente
            annual_growth  = rng.uniform(0.08, 0.16)
            monthly_factor = (1 + annual_growth) ** (1 / 12)

            precio = float(precio_base)
            for fecha in dates:
                # Solo el ruido es aleatorio mes a mes
                noise  = rng.normal(1.0, 0.015)
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
    df["es_sintetico"]  = True
    logger.info(
        "Insumos A07: %s registros sintéticos generados hasta %s "
        "(API y archivos manuales no disponibles)",
        len(df), end_month,
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

    # Guardar en DATA_RAW para consistencia con el resto del pipeline
    out_dir = DATA_RAW / "insumos"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "insumos_raw_consolidado.parquet"
    df.to_parquet(out, index=False)
    logger.info("Insumos raw -> %s (%s registros)", out, len(df))
    return df