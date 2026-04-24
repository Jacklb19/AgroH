import pandas as pd
import logging

logger = logging.getLogger(__name__)

def load_all_facts(engine, df_produccion: pd.DataFrame, df_boletines: pd.DataFrame):
    """
    Función stub para cargar los hechos históricos en la base de datos.
    Debe implementar la lógica para cargar:
    - fact_produccion_agricola
    - fact_alerta_enso
    Y otras tablas de hechos.
    """
    logger.info("load_all_facts: Funcionalidad de carga de hechos (stub ejecutado)")
    pass
