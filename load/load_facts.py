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
