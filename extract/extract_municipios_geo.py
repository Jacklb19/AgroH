"""
extract_municipios_geo.py — Genera polígonos aproximados de municipios de Colombia.

Usa tesselación de Voronoi sobre los centroides del DIVIPOLA para crear polígonos
aproximados de cada municipio, recortados a la caja delimitadora de Colombia.
El resultado se guarda en data/raw/municipios/municipios_colombia.geojson
y se reutiliza en ejecuciones posteriores sin regenerar.
"""
import logging
from pathlib import Path
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import MultiPoint, box, Point
from shapely.ops import voronoi_diagram
from config.settings import DATA_RAW

logger = logging.getLogger(__name__)

_OUTPUT_PATH = DATA_RAW / "municipios" / "municipios_colombia.geojson"
_COLOMBIA_BBOX = box(-82.5, -5.5, -64.0, 14.5)

def extract_municipios_geo(df_divipola: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Carga polígonos municipales existentes o los genera desde centroides DIVIPOLA.
    # FIX v1: Validación de cache corrupto/vacío y fallback a puntos si Voronoi falla.
    """
    _OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    if _OUTPUT_PATH.exists():
        try:
            gdf = gpd.read_file(_OUTPUT_PATH)
            if not gdf.empty:
                logger.info("Municipios geo: cargados desde cache: %s", _OUTPUT_PATH)
                return gdf
            logger.warning("Municipios geo: cache vacío, regenerando...")
        except Exception as e:
            logger.warning("Municipios geo: cache corrupto (%s), regenerando...", e)

    logger.info("Municipios geo: iniciando generación desde centroides DIVIPOLA...")
    gdf = _create_voronoi(df_divipola)

    if not gdf.empty:
        try:
            gdf.to_file(_OUTPUT_PATH, driver="GeoJSON")
            logger.info("Municipios geo: %s geometrías guardadas -> %s", len(gdf), _OUTPUT_PATH)
        except Exception as e:
            logger.error("Municipios geo: error guardando GeoJSON: %s", e)
    else:
        logger.warning("Municipios geo: no se pudieron generar geometrías.")

    return gdf

def _create_point_fallback(df: pd.DataFrame, 
                            id_col: str, nom_col: str,
                            lon_col: str, lat_col: str) -> gpd.GeoDataFrame:
    """
    Fallback: crea un GeoDataFrame de puntos (centroides) cuando 
    Voronoi no puede generarse. Los joins espaciales funcionarán 
    con menor precisión pero no romperán el pipeline.
    """
    logger.warning(
        "Municipios geo: usando centroides como fallback. "
        "Los joins espaciales tendrán menor precisión geográfica."
    )
    geometry = gpd.points_from_xy(df[lon_col], df[lat_col])
    return gpd.GeoDataFrame(
        {
            "id_municipio":     df[id_col].values,
            "nombre_municipio": df[nom_col].values if nom_col else [""] * len(df),
        },
        geometry=geometry,
        crs="EPSG:4326",
    )

def _create_voronoi(df_divipola: pd.DataFrame) -> gpd.GeoDataFrame:
    lat_col = next((c for c in df_divipola.columns if "latitud" in c.lower()), None)
    lon_col = next((c for c in df_divipola.columns if "longitud" in c.lower()), None)
    id_col  = next((c for c in df_divipola.columns if "cod_mpio" in c.lower()), None)
    nom_col = next((c for c in df_divipola.columns if "nom_mpio" in c.lower()), None)

    if not (lat_col and lon_col and id_col):
        logger.warning("Municipios geo: DIVIPOLA incompleto (faltan coordenadas/códigos)")
        return gpd.GeoDataFrame()

    df = df_divipola.copy()
    # Limpieza de coordenadas
    df[lat_col] = pd.to_numeric(df[lat_col].astype(str).str.replace(",", ".", regex=False), errors="coerce")
    df[lon_col] = pd.to_numeric(df[lon_col].astype(str).str.replace(",", ".", regex=False), errors="coerce")
    df = df.dropna(subset=[lat_col, lon_col])
    df[id_col] = df[id_col].astype(str).str.zfill(5)
    df = df.drop_duplicates(subset=[id_col])

    if len(df) < 4:
        logger.warning("Municipios geo: insuficientes datos para Voronoi.")
        return _create_point_fallback(df, id_col, nom_col, lon_col, lat_col) if not df.empty else gpd.GeoDataFrame()

    coords = list(zip(df[lon_col].values, df[lat_col].values))
    multi_point = MultiPoint(coords)

    try:
        regions = voronoi_diagram(multi_point, envelope=_COLOMBIA_BBOX, tolerance=0.001)
    except Exception as exc:
        logger.error("Municipios geo: error en Voronoi: %s", exc)
        return _create_point_fallback(df, id_col, nom_col, lon_col, lat_col)

    centroid_pts = [Point(lon, lat) for lon, lat in coords]
    ids   = df[id_col].tolist()
    names = df[nom_col].tolist() if nom_col else [""] * len(df)

    rows = []
    for region in regions.geoms:
        if region.is_empty: continue
        rc = region.centroid
        distances = [rc.distance(pt) for pt in centroid_pts]
        best = int(np.argmin(distances))
        clipped = region.intersection(_COLOMBIA_BBOX)
        if clipped.is_empty: continue
        rows.append({
            "id_municipio":     ids[best],
            "nombre_municipio": names[best],
            "geometry":         clipped,
        })

    if not rows:
        return _create_point_fallback(df, id_col, nom_col, lon_col, lat_col)

    return gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")
