import logging
from pathlib import Path

import geopandas as gpd

from config.settings import MANUAL_DATA_DIR

logger = logging.getLogger(__name__)


def extract_sipra() -> gpd.GeoDataFrame:
    """
    Carga capas SIPRA desde archivos locales.
    Se espera que el usuario coloque GeoJSON/Shapefile en data/raw/manual/sipra/.
    """
    base = MANUAL_DATA_DIR / "sipra"
    if not base.exists():
        logger.info("No existe directorio manual para SIPRA: %s", base)
        return gpd.GeoDataFrame()

    candidates = list(base.glob("*.geojson")) + list(base.glob("*.json")) + list(base.glob("*.gpkg"))
    if not candidates:
        logger.info("No se encontraron capas SIPRA en %s", base)
        return gpd.GeoDataFrame()

    path = candidates[0]
    logger.info("Leyendo capa SIPRA manual: %s", path.name)
    return gpd.read_file(path)
