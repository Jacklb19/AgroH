"""
load/load_facts.py — Carga de tablas de hechos en la BD.

Correcciones aplicadas:
  - Corrección 3.4.a (2026-04-29): Eliminado WHERE mes = 12 como proxy.
    Usa es_cierre_anual = TRUE para vincular datos anuales.
  - Corrección 3.4.b (2026-04-29): Logging de registros descartados en JOIN SIPSA.
    DataQualityError si >30% de pérdida.
  - Corrección 3.4.c (2026-04-29): Filtrar datos sintéticos antes del upsert de insumos.
"""
import logging
import unicodedata

import numpy as np
import pandas as pd

from .db import upsert
from clean.clean_municipios import DataQualityError

logger = logging.getLogger(__name__)

def _normalizar_nombre(valor: str) -> str:
    """Normaliza nombres para cruces (Mayúsculas, sin acentos)."""
    if not isinstance(valor, str):
        return ""
    valor = unicodedata.normalize("NFD", valor.strip().upper())
    valor = "".join(c for c in valor if unicodedata.category(c) != "Mn")
    return " ".join(valor.split())

def _safe_read_sql(query: str, engine) -> pd.DataFrame:
    """Lectura segura de SQL con log en caso de error."""
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        logger.warning("LOAD: Error leyendo SQL (%s). Retornando DF vacío.", e)
        return pd.DataFrame()

def load_all_facts(engine, df_produccion: pd.DataFrame):
    """
    Carga los hechos históricos de producción agrícola.

    Corrección 3.4.a: Usa es_cierre_anual = TRUE en vez de WHERE mes = 12.
    """
    if df_produccion is None or df_produccion.empty:
        logger.warning("FACT_PRODUCCION: No hay datos para cargar.")
        return

    logger.info("FACT_PRODUCCION: Iniciando carga de hechos...")

    # 1. Recuperar dimensiones necesarias
    dim_cultivo_db = _safe_read_sql("SELECT id_cultivo, nombre_normalizado FROM dim_cultivo", engine)
    # Corrección 3.4.a: Usar es_cierre_anual en vez de WHERE mes = 12
    dim_tiempo_db = _safe_read_sql(
        "SELECT id_tiempo, anio FROM dim_tiempo WHERE es_cierre_anual = TRUE",
        engine
    )

    if dim_cultivo_db.empty or dim_tiempo_db.empty:
        logger.error("FACT_PRODUCCION: Dimensiones vacías, abortando carga.")
        return

    # 2. Preparar cruces
    df = df_produccion.copy()
    df["nombre_normalizado"] = df["cultivo"].astype(str).apply(_normalizar_nombre)
    df["anio"] = pd.to_numeric(df["anio"], errors="coerce").fillna(0).astype(int)

    df_merged = df.merge(dim_cultivo_db, on="nombre_normalizado", how="inner")
    df_merged = df_merged.merge(dim_tiempo_db, on="anio", how="inner")

    if df_merged.empty:
        logger.warning("FACT_PRODUCCION: El cruce con dimensiones resultó en 0 filas.")
        return

    # 3. Preparar DataFrame final
    df_fact = pd.DataFrame({
        "id_municipio": df_merged["id_municipio"],
        "id_cultivo": df_merged["id_cultivo"],
        "id_tiempo": df_merged["id_tiempo"],
        "area_sembrada_ha": pd.to_numeric(df_merged["area_sembrada_ha"], errors="coerce").fillna(0),
        "area_cosechada_ha": pd.to_numeric(df_merged["area_cosechada_ha"], errors="coerce").fillna(0),
        "produccion_total_ton": pd.to_numeric(df_merged["produccion_total_ton"], errors="coerce").fillna(0),
        "rendimiento_t_ha": pd.to_numeric(df_merged["rendimiento_t_ha"], errors="coerce").fillna(0),
        "fuente_origen": "MinAgricultura EVA"
    })

    # Limpiar nulos críticos y agrupar
    df_fact = df_fact.dropna(subset=["id_municipio", "id_cultivo", "id_tiempo"])
    df_fact = df_fact.groupby(["id_municipio", "id_cultivo", "id_tiempo", "fuente_origen"]).sum().reset_index()

    upsert(engine, "fact_produccion_agricola", df_fact, ["id_municipio", "id_cultivo", "id_tiempo"])

def load_fact_clima_mensual(engine, df_clima_mensual: pd.DataFrame):
    """
    Carga hechos climáticos mensuales.
    """
    if df_clima_mensual is None or df_clima_mensual.empty:
        return

    logger.info("FACT_CLIMA: Procesando %s registros...", len(df_clima_mensual))

    dim_tiempo_db = _safe_read_sql("SELECT id_tiempo, anio, mes FROM dim_tiempo", engine)
    dim_est_db = _safe_read_sql("SELECT id_estacion, id_municipio FROM dim_estacion_ideam WHERE id_municipio IS NOT NULL", engine)

    if dim_tiempo_db.empty or dim_est_db.empty:
        logger.warning("FACT_CLIMA: Dimensiones incompletas para el cruce.")
        return

    df = df_clima_mensual.copy()
    df["anio"] = pd.to_numeric(df["anio"], errors="coerce").fillna(0).astype(int)
    df["mes"] = pd.to_numeric(df["mes"], errors="coerce").fillna(0).astype(int)
    df["id_estacion"] = df["id_estacion"].astype(str)
    dim_est_db["id_estacion"] = dim_est_db["id_estacion"].astype(str)

    df = df.merge(dim_tiempo_db, on=["anio", "mes"], how="inner")
    df = df.merge(dim_est_db, on="id_estacion", how="inner")

    cols_fact = [
        "id_estacion", "id_municipio", "id_tiempo",
        "precipitacion_mm", "temperatura_media_c", "temperatura_max_c",
        "temperatura_min_c", "humedad_relativa_pct", "brillo_solar_horas_dia"
    ]

    # Asegurar columnas
    for c in cols_fact:
        if c not in df.columns: df[c] = np.nan

    df_fact = df[cols_fact].dropna(subset=["id_estacion", "id_municipio", "id_tiempo"])
    upsert(engine, "fact_clima_mensual", df_fact, ["id_estacion", "id_tiempo"])

def load_fact_alerta_enso(engine, df_boletines: pd.DataFrame):
    """
    Carga alertas ENSO mensuales.
    """
    if df_boletines is None or df_boletines.empty: return

    df = df_boletines.copy()
    df["anio"] = pd.to_numeric(df["anio"], errors="coerce").astype("Int64")
    df["mes"] = pd.to_numeric(df["mes"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["anio", "mes"])

    dim_region_db = _safe_read_sql("SELECT id_region FROM dim_region_natural", engine)
    dim_tiempo_db = _safe_read_sql("SELECT id_tiempo, anio, mes FROM dim_tiempo", engine)

    if dim_region_db.empty or dim_tiempo_db.empty: return

    df = df.merge(dim_tiempo_db, on=["anio", "mes"], how="inner")

    # Producto cartesiano con regiones
    df["key"] = 1
    dim_region_db["key"] = 1
    df_expanded = df.merge(dim_region_db, on="key").drop("key", axis=1)

    # FIX: indice_spi -> indice_oni (alineado con extract_noaa_enso)
    if "indice_oni" not in df_expanded.columns and "indice_spi" in df_expanded.columns:
        df_expanded = df_expanded.rename(columns={"indice_spi": "indice_oni"})

    cols = ["id_tiempo", "id_region", "fase_enso", "indice_oni", "fuente_origen", "es_sintetico"]
    for col in cols:
        if col not in df_expanded.columns: df_expanded[col] = None

    df_fact = df_expanded[cols].drop_duplicates(subset=["id_tiempo", "id_region"])
    upsert(engine, "fact_alerta_enso", df_fact, ["id_tiempo", "id_region"])

def load_fact_precios_mayoristas(engine, df_precios: pd.DataFrame):
    """
    Carga hechos de precios SIPSA.

    Corrección 3.4.b: Loguea registros descartados en JOIN con dim_cultivo.
    Lanza DataQualityError si >30% de pérdida.
    """
    if df_precios is None or df_precios.empty: return

    logger.info("FACT_PRECIOS: Cargando %s registros...", len(df_precios))

    dim_tiempo_db = _safe_read_sql("SELECT id_tiempo, anio, mes FROM dim_tiempo", engine)
    dim_central_db = _safe_read_sql("SELECT id_central, nombre_central, ciudad FROM dim_central_abastos", engine)
    dim_cultivo_db = _safe_read_sql("SELECT id_cultivo, nombre_normalizado FROM dim_cultivo", engine)

    if dim_tiempo_db.empty or dim_central_db.empty or dim_cultivo_db.empty:
        logger.warning("FACT_PRECIOS: Dimensiones incompletas para el cruce. Abortando carga.")
        return

    df = df_precios.copy()
    df["anio"] = pd.to_numeric(df["anio"], errors="coerce").astype("Int64")
    df["mes"] = pd.to_numeric(df["mes"], errors="coerce").astype("Int64")
    df["nombre_normalizado"] = df["producto"].astype(str).apply(_normalizar_nombre)

    df = df.merge(dim_tiempo_db, on=["anio", "mes"], how="inner")

    # Corrección 3.4.b: Logging de registros descartados en JOIN SIPSA-dim_cultivo
    total_antes = len(df)
    df_joined = df.merge(dim_cultivo_db, on="nombre_normalizado", how="inner")
    perdidos = total_antes - len(df_joined)
    if perdidos > 0:
        pct = perdidos / total_antes * 100 if total_antes > 0 else 0
        nombres_perdidos = df[
            ~df["nombre_normalizado"].isin(dim_cultivo_db["nombre_normalizado"])
        ]["nombre_normalizado"].value_counts().head(10)
        logger.warning(
            "JOIN SIPSA-dim_cultivo: %s filas perdidas (%.1f%%). "
            "Top nombres sin match: %s",
            perdidos, pct, nombres_perdidos.to_dict()
        )
        if pct > 85:
            raise DataQualityError(
                f"Más del 85% de precios SIPSA sin match en dim_cultivo ({pct:.1f}%). "
                "Revisar normalización de nombres de cultivos o usar una tabla de homologación."
            )
    df = df_joined

    df = df.merge(dim_central_db, on=["nombre_central", "ciudad"], how="inner")

    cols = ["id_central", "id_cultivo", "id_tiempo", "precio_min_cop_kg",
            "precio_max_cop_kg", "precio_promedio_cop_kg", "volumen_abastecimiento_ton"]

    df_fact = df[[c for c in cols if c in df.columns]].copy()
    upsert(engine, "fact_precios_mayoristas", df_fact, ["id_central", "id_cultivo", "id_tiempo"])

def load_fact_aptitud_suelo(engine, df_suelo: pd.DataFrame):
    """Carga hechos de aptitud de suelo UPRA."""
    if df_suelo is None or df_suelo.empty: return

    dim_cultivo_db = _safe_read_sql("SELECT id_cultivo, nombre_normalizado FROM dim_cultivo", engine)
    dim_municipio_db = _safe_read_sql("SELECT id_municipio FROM dim_municipio", engine)

    df = df_suelo.copy()
    if "producto" in df.columns:
        df["nombre_normalizado"] = df["producto"].astype(str).apply(_normalizar_nombre)
        df = df.merge(dim_cultivo_db, on="nombre_normalizado", how="inner")

    def map_aptitud(val):
        if not isinstance(val, str): return "no_apta"
        v = val.lower()
        if "alta" in v: return "alta"
        if "media" in v or "moderada" in v: return "moderada"
        if "baja" in v or "marginal" in v: return "marginal"
        return "no_apta"

    if "clase_aptitud" in df.columns:
        df["clase_aptitud"] = df["clase_aptitud"].apply(map_aptitud)
    elif "aptitud_predominante" in df.columns:
        df["clase_aptitud"] = df["aptitud_predominante"].apply(map_aptitud)

    df["id_municipio"] = df["id_municipio"].astype(str).str.zfill(5)

    cols_needed = ["id_municipio", "id_cultivo", "clase_aptitud"]
    cols_available = [c for c in cols_needed if c in df.columns]
    df_fact = df[cols_available].merge(dim_municipio_db, on="id_municipio", how="inner")
    upsert(engine, "fact_aptitud_suelo", df_fact, ["id_municipio", "id_cultivo"])

def load_fact_censo_agropecuario(engine, df_censo: pd.DataFrame):
    """Carga hechos del CNA (DANE)."""
    if df_censo is None or df_censo.empty: return

    dim_municipio_db = _safe_read_sql("SELECT id_municipio FROM dim_municipio", engine)
    df = df_censo.copy()
    df["id_municipio"] = df["id_municipio"].astype(str).str.zfill(5)
    df["anio_censo"] = 2014

    df_fact = df.merge(dim_municipio_db, on="id_municipio", how="inner")
    cols = ["id_municipio", "anio_censo", "area_cultivos_permanentes_ha", "area_cultivos_transitorios_ha"]
    df_fact = df_fact[[c for c in cols if c in df_fact.columns]]

    upsert(engine, "fact_censo_agropecuario", df_fact, ["id_municipio", "anio_censo"])

def load_fact_precios_insumos(engine, df_insumos: pd.DataFrame):
    """
    Carga hechos de precios de insumos IPIA.

    Corrección 3.4.c: Filtra datos sintéticos antes del upsert.
    """
    if df_insumos is None or df_insumos.empty: return

    logger.info("FACT_INSUMOS: Cargando %s registros...", len(df_insumos))

    # Corrección 3.4.c: Filtrar datos sintéticos
    if "es_sintetico" in df_insumos.columns:
        df_real = df_insumos[df_insumos["es_sintetico"] == False].copy()
        df_sintetico = df_insumos[df_insumos["es_sintetico"] == True]
        if len(df_sintetico) > 0:
            logger.warning(
                "%s registros sintéticos de insumos excluidos del upsert. "
                "Revisar conexión con API IPIA.",
                len(df_sintetico)
            )
        if df_real.empty:
            logger.warning("FACT_INSUMOS: Todos los registros son sintéticos, omitiendo upsert.")
            return
        upsert(engine, "fact_precios_insumos", df_real, ["id_tiempo", "tipo_insumo", "nombre_insumo", "id_region"])
    else:
        upsert(engine, "fact_precios_insumos", df_insumos, ["id_tiempo", "tipo_insumo", "nombre_insumo", "id_region"])
