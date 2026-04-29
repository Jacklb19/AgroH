"""config/settings.py — Configuración central del pipeline AgroIA."""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


# --- PATHS ---
BASE_DIR        = Path(__file__).resolve().parent.parent
DATA_RAW        = BASE_DIR / "data" / "raw"
DATA_PROCESSED  = BASE_DIR / "data" / "processed"
LOGS_DIR        = BASE_DIR / "logs"
MANUAL_DATA_DIR = DATA_RAW / "manual"


def ensure_dirs() -> None:
    """Crea carpetas base si no existen."""
    for d in [DATA_RAW, DATA_PROCESSED, LOGS_DIR, MANUAL_DATA_DIR]:
        d.mkdir(parents=True, exist_ok=True)


# --- BASE DE DATOS ---
def _parse_db_port(value: Optional[str], default: int = 5432) -> int:
    if not value:
        return default
    try:
        return int(value.strip())
    except (ValueError, AttributeError):
        logger.warning("settings: port '%s' inválido, usando %s.", value, default)
        return default


DB = {
    "host":     os.getenv("SUPABASE_DB_HOST"),
    "port":     _parse_db_port(os.getenv("SUPABASE_DB_PORT")),
    "dbname":   os.getenv("SUPABASE_DB_NAME", "postgres"),
    "user":     os.getenv("SUPABASE_DB_USER", "postgres"),
    "password": os.getenv("SUPABASE_DB_PASSWORD") or os.getenv("SUPABASE_DB_PASS"),
}

_db_missing = [k for k in ("host", "password") if not DB[k]]
if _db_missing:
    logger.warning("settings: credenciales de BD no configuradas (%s).", ", ".join(_db_missing))


# --- SOCRATA ---
SOCRATA_TOKEN: Optional[str] = os.getenv("SOCRATA_TOKEN") or None
if not SOCRATA_TOKEN:
    logger.warning("settings: SOCRATA_TOKEN no configurado (throttling posible).")


# --- FUENTES ---
SOURCES = {
    "produccion_datosgov":   "https://www.datos.gov.co/resource/uejq-wxrr.json",
    "sipsa_mayoristas":      "https://microdatos.dane.gov.co/index.php/catalog/776",
    "insumos_ipia":          "https://www.datos.gov.co/resource/y5zy-x4ky.json",
    "estaciones_ideam":      "https://www.datos.gov.co/resource/hp9r-jxuu.json",
    "precipitacion_ideam":   "https://www.datos.gov.co/resource/s54a-sgyg.json",
    "clima_combinado_ideam": "https://www.datos.gov.co/resource/57sv-p2fu.json",
    "sipra_geojson":         "https://sipra.upra.gov.co/geoserver/ows",
    "divipola":              "https://www.datos.gov.co/resource/gdxc-w37w.json",
}


# --- PERÍODO HISTÓRICO ---
YEAR_START   = 2007
_anio_actual = datetime.now().year

_year_end_env = os.getenv("PIPELINE_YEAR_END")
if _year_end_env:
    try:
        YEAR_END = min(max(int(_year_end_env.strip()), YEAR_START), _anio_actual)
    except ValueError:
        YEAR_END = _anio_actual
else:
    YEAR_END = _anio_actual

CLIMA_YEAR_START = 2018
if CLIMA_YEAR_START > YEAR_END:
    logger.warning("settings: CLIMA_YEAR_START > YEAR_END (%s > %s)", CLIMA_YEAR_START, YEAR_END)


# --- EXTRACCIÓN ---
IDEAM_CHUNK_DAYS       = 10  # reducir si hay timeouts en Socrata IDEAM
SPATIAL_JOIN_RADIUS_KM = 50  # radio join estación ↔ municipio (km)


# --- DOMINIO ---
# Orden fijo = id_region en BD (incluye región Insular).
REGIONES_NATURALES = ["Andina", "Caribe", "Pacífico", "Orinoquía", "Amazonía", "Insular"]


# --- UTILIDADES ---
def _split_env_list(value: Optional[str]) -> list:
    """Convierte una env var separada por comas en lista de strings limpios."""
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


ENSO_BOLETIN_URLS = _split_env_list(os.getenv("ENSO_BOLETIN_URLS"))