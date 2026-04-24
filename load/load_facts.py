import pandas as pd
import logging

logger = logging.getLogger(__name__)

def load_all_facts(engine, df_produccion: pd.DataFrame, df_boletines: pd.DataFrame):
    """
    Carga los hechos históricos en la base de datos.
    """
    from .db import upsert
    logger.info("load_all_facts: Cargando fact_produccion_agricola...")
    
    # 1. Recuperar id_cultivo y id_tiempo de la base de datos
    dim_cultivo_db = pd.read_sql("SELECT id_cultivo, nombre_normalizado FROM dim_cultivo", engine)
    dim_tiempo_db = pd.read_sql("SELECT id_tiempo, anio FROM dim_tiempo WHERE mes = 12", engine)
    
    # 2. Unir df_produccion con dim_cultivo y dim_tiempo
    df_produccion["nombre_normalizado"] = df_produccion["cultivo"].astype(str).str.upper().str.strip()
    df_produccion["anio"] = df_produccion["a_o"].astype(int)
    
    df_merged = df_produccion.merge(dim_cultivo_db, on="nombre_normalizado", how="inner")
    df_merged = df_merged.merge(dim_tiempo_db, on="anio", how="inner")
    
    # 3. Preparar el DataFrame para la tabla de hechos
    fact_cols = {
        "id_municipio": df_merged["id_municipio"],
        "id_cultivo": df_merged["id_cultivo"],
        "id_tiempo": df_merged["id_tiempo"],
        "area_sembrada_ha": df_merged["rea_sembrada_ha"],
        "area_cosechada_ha": df_merged["rea_cosechada_ha"],
        "produccion_total_ton": df_merged["producci_n_t"],
        "rendimiento_t_ha": df_merged["rendimiento_t_ha"],
        "fuente_origen": "MinAgricultura A04/A05"
    }
    df_fact = pd.DataFrame(fact_cols)
    
    # Limpiar nulos y asegurar tipos correctos
    df_fact = df_fact.dropna(subset=["id_municipio", "id_cultivo", "id_tiempo"])
    for col in ["area_sembrada_ha", "area_cosechada_ha", "produccion_total_ton", "rendimiento_t_ha"]:
        df_fact[col] = pd.to_numeric(df_fact[col], errors='coerce').fillna(0)
        
    # Agrupar por llaves primarias en caso de duplicados en la fuente
    df_fact = df_fact.groupby(["id_municipio", "id_cultivo", "id_tiempo", "fuente_origen"]).sum().reset_index()

    # 4. Upsert a fact_produccion_agricola
    upsert(engine, "fact_produccion_agricola", df_fact, ["id_municipio", "id_cultivo", "id_tiempo"])
    logger.info(f"fact_produccion_agricola: {len(df_fact)} registros cargados")


def load_fact_clima_mensual(engine, df_clima_mensual: pd.DataFrame):
    """
    Carga fact_clima_mensual cruzando con dim_tiempo y dim_estacion_ideam.
    df_clima_mensual debe tener: id_estacion, anio, mes, precipitacion_mm, etc.
    """
    from .db import upsert

    if df_clima_mensual.empty:
        logger.warning("Sin datos climáticos mensuales para cargar")
        return

    logger.info(f"load_fact_clima_mensual: procesando {len(df_clima_mensual)} registros...")

    # 1. Recuperar id_tiempo (fecha = primer día del mes)
    dim_tiempo_db = pd.read_sql("SELECT id_tiempo, anio, mes FROM dim_tiempo", engine)

    # 2. Recuperar estaciones con su id_municipio
    dim_est_db = pd.read_sql(
        "SELECT id_estacion, id_municipio FROM dim_estacion_ideam WHERE id_municipio IS NOT NULL",
        engine
    )

    # 3. Join con dim_tiempo
    df_clima_mensual["anio"] = df_clima_mensual["anio"].astype(int)
    df_clima_mensual["mes"] = df_clima_mensual["mes"].astype(int)
    df = df_clima_mensual.merge(dim_tiempo_db, on=["anio", "mes"], how="inner")

    # 4. Join con dim_estacion_ideam para obtener id_municipio
    df["id_estacion"] = df["id_estacion"].astype(str)
    dim_est_db["id_estacion"] = dim_est_db["id_estacion"].astype(str)
    df = df.merge(dim_est_db, on="id_estacion", how="inner")

    # 5. Seleccionar columnas del schema
    cols_fact = [
        "id_estacion", "id_municipio", "id_tiempo",
        "precipitacion_mm", "temperatura_media_c", "temperatura_max_c",
        "temperatura_min_c", "humedad_relativa_pct", "brillo_solar_horas_dia"
    ]
    # Asegurar que existen todas las columnas (pueden faltar si no hubo datos de ese sensor)
    for c in cols_fact:
        if c not in df.columns:
            df[c] = None

    df_fact = df[cols_fact].copy()
    df_fact = df_fact.dropna(subset=["id_estacion", "id_municipio", "id_tiempo"])

    import numpy as np
    df_fact = df_fact.replace({np.nan: None})

    if df_fact.empty:
        logger.warning("Tras filtrar, no quedan registros de clima para cargar")
        return

    logger.info(f"  Insertando {len(df_fact)} registros en fact_clima_mensual...")
    upsert(engine, "fact_clima_mensual", df_fact, ["id_estacion", "id_tiempo"])
    logger.info(f"fact_clima_mensual: {len(df_fact)} registros cargados")
