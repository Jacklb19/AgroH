import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW = BASE_DIR / "data" / "raw"
DATA_PROCESSED = BASE_DIR / "data" / "processed"
LOGS_DIR = BASE_DIR / "logs"

# Crear carpetas si no existen
for d in [DATA_RAW, DATA_PROCESSED, LOGS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

DB = {
    "host":     os.getenv("SUPABASE_DB_HOST"),
    "port":     int(os.getenv("SUPABASE_DB_PORT", 5432)),
    "dbname":   os.getenv("SUPABASE_DB_NAME", "postgres"),
    "user":     os.getenv("SUPABASE_DB_USER", "postgres"),
    "password": os.getenv("SUPABASE_DB_PASSWORD"),
}

# URLs de fuentes
SOURCES = {
    # A04 — datos.gov.co Socrata
    "produccion_datosgov": "https://www.datos.gov.co/resource/2pnw-mmge.json",
    # A06 — SIPSA mayoristas microdatos
    "sipsa_mayoristas": "https://microdatos.dane.gov.co/index.php/catalog/776",
    # A08 — Catálogo estaciones IDEAM Socrata
    "estaciones_ideam": "https://www.datos.gov.co/resource/hp9r-jxuu.json",
    # A09 — Precipitación IDEAM (cada 10 min)
    "precipitacion_ideam": "https://www.datos.gov.co/resource/s54a-sgyg.json",
    # A09b — Datos estaciones IDEAM y terceros (temperatura + variables combinadas)
    "clima_combinado_ideam": "https://www.datos.gov.co/resource/57sv-p2fu.json",
    # A11 — SIPRA GeoJSON (aptitud suelo arroz ejemplo)
    "sipra_geojson": "https://sipra.upra.gov.co/geoserver/ows",
    # DIVIPOLA
    "divipola": "https://www.datos.gov.co/resource/gdxc-w37w.json",
}

# Radio máximo join espacial clima-municipio (km)
SPATIAL_JOIN_RADIUS_KM = 50

# Período histórico producción
YEAR_START = 2007
YEAR_END   = 2025

# Período histórico clima (más corto para descargas rápidas, ampliar después)
CLIMA_YEAR_START = 2020

# Regiones naturales (orden fijo = id_region)
REGIONES_NATURALES = ["Andina", "Caribe", "Pacífico", "Orinoquía", "Amazonía"]
