import pandas as pd
import logging
from load.db import get_engine

logger = logging.getLogger(__name__)

def build_ml_features(engine=None):
    """
    Construye la vista/tabla Feature Store para entrenar XGBoost.
    Evita la duplicación de datos resolviendo la granularidad temporal anual vs mensual.
    """
    logger.info("Construyendo Feature Store para Machine Learning...")
    if engine is None:
        engine = get_engine()
        
    query = """
    WITH clima_agrupado AS (
        SELECT 
            fc.id_municipio,
            dt.anio,
            SUM(fc.precipitacion_mm) AS lluvia_acumulada_anual,
            AVG(fc.temperatura_media_c) AS temp_promedio_anual,
            MAX(fc.temperatura_max_c) AS temp_maxima_anual
        FROM fact_clima_mensual fc
        JOIN dim_tiempo dt ON dt.id_tiempo = fc.id_tiempo
        GROUP BY fc.id_municipio, dt.anio
    ),
    produccion AS (
        SELECT 
            fp.id_municipio,
            fp.id_tiempo,
            dt.anio,
            fp.id_cultivo,
            fp.rendimiento_t_ha,
            fp.area_sembrada_ha
        FROM fact_produccion_agricola fp
        JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
    )
    SELECT 
        p.id_municipio,
        p.id_tiempo,
        p.anio,
        p.id_cultivo,
        p.area_sembrada_ha,
        p.rendimiento_t_ha,
        c.lluvia_acumulada_anual,
        c.temp_promedio_anual,
        c.temp_maxima_anual
    FROM produccion p
    LEFT JOIN clima_agrupado c 
        ON p.id_municipio = c.id_municipio 
        AND p.anio = c.anio;
    """
    try:
        df_features = pd.read_sql(query, engine)
        logger.info(f"Feature Store construido: {len(df_features)} filas (Una fila = Un año de cosecha).")
        return df_features
    except Exception as e:
        logger.error(f"Error construyendo Feature Store: {e}")
        return pd.DataFrame()
