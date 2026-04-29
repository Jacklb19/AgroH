import pandas as pd
import logging
from datetime import date
from config.settings import REGIONES_NATURALES, YEAR_START, YEAR_END
from .db import upsert

logger = logging.getLogger(__name__)

MESES = ["enero","febrero","marzo","abril","mayo","junio",
         "julio","agosto","septiembre","octubre","noviembre","diciembre"]

def load_dim_region_natural(engine):
    """Carga las regiones naturales definidas en settings."""
    df = pd.DataFrame([{"nombre_region": r} for r in REGIONES_NATURALES])
    upsert(engine, "dim_region_natural", df, ["nombre_region"])

def load_dim_tiempo(engine, anios_nino: list = None):
    """
    Genera dim_tiempo con un registro por mes desde YEAR_START hasta YEAR_END.
    # FIX v1: Validación de periodos y logging estandarizado.
    """
    # Lista de referencia histórica de años El Niño (fuente: NOAA ONI)
    anios_nino = anios_nino or [1997,1998,2002,2003,2009,2010,2015,2016,2018,2019,2023,2024]
    rows = []
    
    logger.info("DIM_TIEMPO: Generando periodos %s a %s", YEAR_START, YEAR_END)
    
    for anio in range(YEAR_START, YEAR_END + 1):
        for mes in range(1, 13):
            trimestre = (mes - 1) // 3 + 1
            semestre  = "A" if mes <= 6 else "B"
            rows.append({
                "fecha":        date(anio, mes, 1),
                "anio":         anio,
                "mes":          mes,
                "trimestre":    trimestre,
                "semestre":     semestre,
                "nombre_mes":   MESES[mes - 1],
                "es_anio_nino": anio in anios_nino,
            })
    df = pd.DataFrame(rows)
    upsert(engine, "dim_tiempo", df, ["fecha"])

def load_dim_municipio(engine, df_divipola: pd.DataFrame, df_region_map: pd.DataFrame):
    """
    Carga la dimensión municipio cruzando DIVIPOLA con el mapa de regiones.
    # FIX v1: Limpieza robusta de coordenadas y manejo de mapeos.
    """
    if df_divipola is None or df_divipola.empty:
        logger.warning("DIM_MUNICIPIO: No hay datos de DIVIPOLA para cargar.")
        return

    df = df_divipola.rename(columns={
        "cod_mpio":   "id_municipio",
        "nom_mpio":   "nombre_municipio",
        "cod_dpto":   "id_departamento",
        "dpto":       "nombre_departamento",
        "latitud":    "latitud_centroide",
        "longitud":   "longitud_centroide",
    }).copy()

    # Selección de columnas necesarias
    needed = ["id_municipio","nombre_municipio","id_departamento","nombre_departamento","latitud_centroide","longitud_centroide"]
    df = df[[c for c in needed if c in df.columns]]

    # Limpieza de tipos y formatos
    df["id_municipio"] = df["id_municipio"].astype(str).str.zfill(5)
    
    for col in ["latitud_centroide", "longitud_centroide"]:
        if col in df.columns:
            # Reemplazo de coma por punto y conversión a float
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", ".", regex=False), 
                errors="coerce"
            ).fillna(0.0)

    # Cruce con mapa de regiones si existe
    if df_region_map is not None:
        try:
            if "nombre_region" in df_region_map.columns and "id_region" not in df_region_map.columns:
                dim_region_db = pd.read_sql("SELECT id_region, nombre_region FROM dim_region_natural", engine)
                df_region_map = df_region_map.merge(dim_region_db, on="nombre_region", how="left")
            
            df = df.merge(df_region_map[["id_municipio", "id_region"]], on="id_municipio", how="left")
        except Exception as e:
            logger.error("DIM_MUNICIPIO: Error al mapear regiones: %s", e)

    expected_cols = ["id_municipio", "nombre_municipio", "id_departamento", "nombre_departamento", 
                     "latitud_centroide", "longitud_centroide", "id_region"]
    df = df[[col for col in expected_cols if col in df.columns]]
    
    upsert(engine, "dim_municipio", df, ["id_municipio"])

def load_dim_cultivo(engine, df_cultivos: pd.DataFrame):
    """Carga catálogo de cultivos."""
    upsert(engine, "dim_cultivo", df_cultivos, ["nombre_normalizado"])

def load_dim_estacion_ideam(engine, df_estaciones: pd.DataFrame):
    """Carga catálogo de estaciones IDEAM."""
    upsert(engine, "dim_estacion_ideam", df_estaciones, ["id_estacion"])

def load_dim_central_abastos(engine, df_centrales: pd.DataFrame):
    """
    Carga centrales de abastos (SIPSA).
    # FIX v1: Manejo de nulos en FK id_municipio.
    """
    if df_centrales is None or df_centrales.empty: return
    
    df = df_centrales.copy()
    if "id_municipio" in df.columns:
        # Asegurar formato 5 dígitos o None
        df["id_municipio"] = df["id_municipio"].astype(str).str.zfill(5)
        df.loc[df["id_municipio"].isin(["00nan", "00000", "None"]), "id_municipio"] = None
        
    upsert(engine, "dim_central_abastos", df, ["nombre_central", "ciudad"])

