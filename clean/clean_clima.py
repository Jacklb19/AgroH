import pandas as pd
import geopandas as gpd
import numpy as np
import logging
from shapely.geometry import Point
from config.settings import DATA_RAW, DATA_PROCESSED, SPATIAL_JOIN_RADIUS_KM

logger = logging.getLogger(__name__)

def agregar_clima_mensual(df_diario: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega datos climáticos diarios del DHIME a granularidad mensual.
    Precipitación: suma. Temperatura y humedad: promedio. Brillo solar: promedio.
    """
    df = df_diario.copy()
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df = df.dropna(subset=["fecha"])
    df["anio"] = df["fecha"].dt.year
    df["mes"]  = df["fecha"].dt.month

    agg = df.groupby(["id_estacion", "anio", "mes"]).agg(
        precipitacion_mm       = ("precipitacion_mm", "sum"),
        temperatura_media_c    = ("temperatura_c", "mean"),
        temperatura_max_c      = ("temperatura_max_c", "max"),
        temperatura_min_c      = ("temperatura_min_c", "min"),
        humedad_relativa_pct   = ("humedad_relativa_pct", "mean"),
        brillo_solar_horas_dia = ("brillo_solar_horas", "mean"),
    ).reset_index()

    # Crear fecha = primer día del mes
    agg["fecha_mes"] = pd.to_datetime(
        agg["anio"].astype(str) + "-" + agg["mes"].astype(str).str.zfill(2) + "-01"
    )
    out = DATA_PROCESSED / "clima_mensual.parquet"
    agg.to_parquet(out, index=False)
    logger.info(f"Clima mensual: {len(agg)} registros → {out}")
    return agg


def join_espacial_estacion_municipio(df_estaciones: pd.DataFrame,
                                      df_municipios: pd.DataFrame) -> pd.DataFrame:
    """
    Para cada estación IDEAM calcula el municipio más cercano dentro de SPATIAL_JOIN_RADIUS_KM.
    df_estaciones: columnas latitud, longitud, id_estacion
    df_municipios: columnas latitud_centroide, longitud_centroide, id_municipio
    Retorna df_estaciones con columna id_municipio añadida.
    """
    gdf_est = gpd.GeoDataFrame(
        df_estaciones,
        geometry=gpd.points_from_xy(df_estaciones["longitud"], df_estaciones["latitud"]),
        crs="EPSG:4326"
    ).to_crs("EPSG:3116")  # Colombia MAGNA-SIRGAS

    gdf_mun = gpd.GeoDataFrame(
        df_municipios,
        geometry=gpd.points_from_xy(
            df_municipios["longitud_centroide"],
            df_municipios["latitud_centroide"]
        ),
        crs="EPSG:4326"
    ).to_crs("EPSG:3116")

    radio_m = SPATIAL_JOIN_RADIUS_KM * 1000
    resultados = []
    for _, est in gdf_est.iterrows():
        distancias = gdf_mun.geometry.distance(est.geometry)
        idx_min = distancias.idxmin()
        dist_min = distancias[idx_min]
        if dist_min <= radio_m:
            id_mun = gdf_mun.loc[idx_min, "id_municipio"]
        else:
            id_mun = None
            logger.debug(f"Estación {est['id_estacion']}: sin municipio en {SPATIAL_JOIN_RADIUS_KM}km")
        resultados.append({"id_estacion": est["id_estacion"], "id_municipio": id_mun})

    df_resultado = pd.DataFrame(resultados)
    sin_cobertura = df_resultado["id_municipio"].isna().sum()
    logger.info(
        f"Join espacial: {len(df_resultado)} estaciones, "
        f"{sin_cobertura} sin municipio dentro de {SPATIAL_JOIN_RADIUS_KM}km"
    )
    return df_estaciones.merge(df_resultado, on="id_estacion", how="left")
