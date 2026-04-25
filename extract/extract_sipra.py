import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd

from config.settings import MANUAL_DATA_DIR

logger = logging.getLogger(__name__)


def extract_sipra() -> gpd.GeoDataFrame:
    """
    Carga y concatena todas las capas de aptitud agrícola SIPRA en data/raw/manual/sipra/.
    Excluye la capa de Frontera Agrícola (tiene esquema diferente).
    Soporta GeoJSON, JSON, GeoPackage y Shapefile.
    """
    base = MANUAL_DATA_DIR / "sipra"
    if not base.exists():
        logger.info("No existe directorio manual para SIPRA: %s", base)
        return gpd.GeoDataFrame()

    candidates = (
        list(base.glob("*.geojson"))
        + list(base.glob("*.json"))
        + list(base.glob("*.gpkg"))
        + list(base.glob("*.shp"))
    )
    aptitud_files = [p for p in candidates if "frontera" not in p.stem.lower()]
    if not aptitud_files:
        logger.info("No se encontraron capas de aptitud SIPRA en %s", base)
        return gpd.GeoDataFrame()

    frames = []
    for path in aptitud_files:
        logger.info("Leyendo capa SIPRA: %s", path.name)
        try:
            gdf = gpd.read_file(path)
            gdf["_source_file"] = path.stem
            frames.append(gdf)
        except Exception as exc:
            logger.warning("No se pudo leer %s: %s", path.name, exc)

    if not frames:
        return gpd.GeoDataFrame()

    combined = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True))
    logger.info("SIPRA: %s registros cargados desde %s archivos", len(combined), len(frames))
    return combined


def extract_frontera_agricola() -> gpd.GeoDataFrame:
    """
    Carga la capa de Frontera Agrícola de UPRA (Jun 2025) si existe.
    Retorna GeoDataFrame con geometría de polígonos o vacío.
    """
    base = MANUAL_DATA_DIR / "sipra"
    if not base.exists():
        return gpd.GeoDataFrame()

    candidates = []
    for pattern in [
        "Frontera_Agricola*.shp", "frontera_agricola*.shp",
        "Frontera_Agricola*.geojson", "frontera_agricola*.gpkg",
    ]:
        candidates.extend(base.glob(pattern))

    if not candidates:
        return gpd.GeoDataFrame()

    path = candidates[0]
    logger.info("Leyendo Frontera Agrícola: %s", path.name)
    try:
        gdf = gpd.read_file(path)
        logger.info("Frontera Agrícola: %s features cargados", len(gdf))
        return gdf
    except Exception as exc:
        logger.warning("No se pudo leer Frontera Agrícola: %s", exc)
        return gpd.GeoDataFrame()
