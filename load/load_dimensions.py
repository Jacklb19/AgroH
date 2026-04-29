"""
load/load_dimensions.py — Carga de tablas de dimensiones en la BD.

Correcciones aplicadas:
  - Corrección 3.3.a (2026-04-29): Calcular es_anio_nino dinámicamente desde ONI real.
    Lista hardcodeada como fallback. Función _calcular_anios_nino_from_oni.
  - Corrección 3.3.b (2026-04-29): NULL en lugar de 0.0 para coordenadas faltantes.
  - Corrección 3.4.a (2026-04-29): Agregado campo es_cierre_anual a dim_tiempo.
"""
import logging
from datetime import date

import numpy as np
import pandas as pd

from config.settings import REGIONES_NATURALES, YEAR_START, YEAR_END, MES_CIERRE_CAMPANA
from .db import upsert

logger = logging.getLogger(__name__)

MESES = ["enero","febrero","marzo","abril","mayo","junio",
         "julio","agosto","septiembre","octubre","noviembre","diciembre"]

# Fallback: lista histórica de años El Niño (fuente: NOAA ONI)
ANIOS_NINO_FALLBACK = {1997, 1998, 2002, 2003, 2009, 2010, 2015, 2016, 2018, 2019, 2023, 2024}


def _calcular_anios_nino_from_oni(engine) -> set[int]:
    """
    Calcula años El Niño usando criterio oficial NOAA:
    ONI >= 0.5 durante al menos 5 meses consecutivos.

    Corrección 3.3.a: Reemplaza lista hardcodeada.
    """
    try:
        query = """
            SELECT dt.anio, dt.mes, ae.indice_oni
            FROM fact_alerta_enso ae
            JOIN dim_tiempo dt ON dt.id_tiempo = ae.id_tiempo
            WHERE ae.fuente_origen NOT LIKE '%%sintetico%%'
            ORDER BY dt.anio, dt.mes
        """
        df = pd.read_sql(query, engine)
    except Exception as e:
        logger.warning("fact_alerta_enso no disponible para calcular años Niño: %s", e)
        return ANIOS_NINO_FALLBACK

    if df.empty:
        logger.warning("fact_alerta_enso vacía, usando lista manual de años Niño")
        return ANIOS_NINO_FALLBACK

    # Determinar meses con ONI >= 0.5
    df["es_nino_mes"] = df["indice_oni"] >= 0.5

    # Calcular rachas consecutivas de meses Niño
    df["grupo"] = (df["es_nino_mes"] != df["es_nino_mes"].shift()).cumsum()
    df["streak"] = df.groupby("grupo")["es_nino_mes"].cumsum()

    # Años con al menos 5 meses consecutivos con ONI >= 0.5
    anios_nino = set(df[df["streak"] >= 5]["anio"].unique())

    if not anios_nino:
        logger.warning("Cálculo dinámico no encontró años Niño, usando fallback")
        return ANIOS_NINO_FALLBACK

    logger.info("Años Niño calculados desde ONI real: %s", sorted(anios_nino))
    return anios_nino


def load_dim_region_natural(engine):
    """Carga las regiones naturales definidas en settings."""
    df = pd.DataFrame([{"nombre_region": r} for r in REGIONES_NATURALES])
    upsert(engine, "dim_region_natural", df, ["nombre_region"])

def load_dim_tiempo(engine, anios_nino: set = None):
    """
    Genera dim_tiempo con un registro por mes desde YEAR_START hasta YEAR_END.

    Corrección 3.3.a: anios_nino se calcula dinámicamente si no se provee.
    Corrección 3.4.a: Agrega campo es_cierre_anual.
    """
    if anios_nino is None:
        anios_nino = ANIOS_NINO_FALLBACK

    rows = []

    logger.info("DIM_TIEMPO: Generando periodos %s a %s", YEAR_START, YEAR_END)

    for anio in range(YEAR_START, YEAR_END + 1):
        for mes in range(1, 13):
            trimestre = (mes - 1) // 3 + 1
            semestre  = "A" if mes <= 6 else "B"
            rows.append({
                "fecha":            date(anio, mes, 1),
                "anio":             anio,
                "mes":              mes,
                "trimestre":        trimestre,
                "semestre":         semestre,
                "nombre_mes":       MESES[mes - 1],
                "es_anio_nino":     anio in anios_nino,
                "es_cierre_anual":  mes == MES_CIERRE_CAMPANA,
            })
    df = pd.DataFrame(rows)
    upsert(engine, "dim_tiempo", df, ["fecha"])


def update_dim_tiempo_with_oni(engine):
    """
    Actualiza dim_tiempo con años Niño calculados dinámicamente desde ONI real.
    Debe llamarse DESPUÉS de que fact_alerta_enso esté cargada.

    Corrección 3.3.a.
    """
    anios_nino = _calcular_anios_nino_from_oni(engine)
    if anios_nino and anios_nino != ANIOS_NINO_FALLBACK:
        try:
            from sqlalchemy import text
            with engine.begin() as conn:
                # Reset all
                conn.execute(text("UPDATE dim_tiempo SET es_anio_nino = FALSE"))
                # Set Niño years
                for anio in anios_nino:
                    conn.execute(
                        text("UPDATE dim_tiempo SET es_anio_nino = TRUE WHERE anio = :anio"),
                        {"anio": anio}
                    )
            logger.info("DIM_TIEMPO: es_anio_nino actualizado con %s años Niño desde ONI", len(anios_nino))
        except Exception as e:
            logger.error("DIM_TIEMPO: Error actualizando es_anio_nino: %s", e)


def load_dim_municipio(engine, df_divipola: pd.DataFrame, df_region_map: pd.DataFrame):
    """
    Carga la dimensión municipio cruzando DIVIPOLA con el mapa de regiones.

    Corrección 3.3.b: No rellena coordenadas con 0.0 — deja NULL.
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
            # Corrección 3.3.b: No rellenar — dejar NULL
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", ".", regex=False),
                errors="coerce"
            )

    # Corrección 3.3.b: Loguear municipios sin coordenadas
    if "latitud_centroide" in df.columns:
        sin_coords = df["latitud_centroide"].isna().sum()
        if sin_coords > 0:
            logger.info(
                "%s municipios sin coordenadas. "
                "Se insertarán con latitud/longitud NULL.",
                sin_coords
            )

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

    # Reemplazar NaN con None explícitamente
    df = df.replace({np.nan: None})

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
    """
    if df_centrales is None or df_centrales.empty: return

    df = df_centrales.copy()
    if "id_municipio" in df.columns:
        # Asegurar formato 5 dígitos o None
        df["id_municipio"] = df["id_municipio"].astype(str).str.zfill(5)
        df.loc[df["id_municipio"].isin(["00nan", "00000", "None"]), "id_municipio"] = None

    upsert(engine, "dim_central_abastos", df, ["nombre_central", "ciudad"])
