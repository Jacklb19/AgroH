
import sys
import logging
from pathlib import Path
from sqlalchemy import text

# Añadir el directorio raíz al path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from load.db import get_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("fix_data")

def fix_database_data():
    engine = get_engine()
    if engine is None:
        logger.error("No se pudo conectar a la base de datos.")
        return

    logger.info("Iniciando corrección de datos existentes en la base de datos...")

    with engine.begin() as conn:
        # 1. Corregir fact_produccion_agricola: area_cosechada <= area_sembrada
        logger.info("Corrigiendo inconsistencias de área en fact_produccion_agricola...")
        sql_area = """
            UPDATE fact_produccion_agricola 
            SET area_cosechada_ha = area_sembrada_ha 
            WHERE area_cosechada_ha > area_sembrada_ha
        """
        res_area = conn.execute(text(sql_area))
        logger.info(f"-> {res_area.rowcount} filas actualizadas (area_cosechada).")

        # 2. Corregir fact_produccion_agricola: rendimiento outliers
        logger.info("Corrigiendo outliers de rendimiento en fact_produccion_agricola...")
        sql_rend = """
            UPDATE fact_produccion_agricola 
            SET rendimiento_t_ha = 150 
            WHERE rendimiento_t_ha > 150
        """
        res_rend = conn.execute(text(sql_rend))
        logger.info(f"-> {res_rend.rowcount} filas actualizadas (rendimiento real).")

        # 3. Corregir pred_rendimiento: rendimiento outliers
        logger.info("Corrigiendo outliers en pred_rendimiento...")
        sql_pred = """
            UPDATE pred_rendimiento 
            SET rendimiento_predicho_t_ha = 150 
            WHERE rendimiento_predicho_t_ha > 150
        """
        res_pred = conn.execute(text(sql_pred))
        logger.info(f"-> {res_pred.rowcount} filas actualizadas (predicciones).")
        
        # También corregir negativos si los hubiera (clip inferior a 0)
        sql_neg = """
            UPDATE pred_rendimiento 
            SET rendimiento_predicho_t_ha = 0 
            WHERE rendimiento_predicho_t_ha < 0
        """
        res_neg = conn.execute(text(sql_neg))
        logger.info(f"-> {res_neg.rowcount} filas actualizadas (predicciones negativas).")

    logger.info("Corrección finalizada exitosamente.")

if __name__ == "__main__":
    fix_database_data()
