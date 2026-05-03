
import sys
import logging
import urllib.parse
from pathlib import Path
from sqlalchemy import create_engine, text

# Añadir el directorio raíz al path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from load.db import get_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("drop_columns")

# La configuración se toma automáticamente de .env a través de load.db
# Para ejecutar contra Supabase, asegúrese de que su .env tenga las credenciales del host remoto.

def drop_columns_logic(engine, label):
    if engine is None:
        logger.error(f"No se pudo obtener el motor para {label}")
        return
    logger.info(f"Eliminando columnas vacías en {label}...")
    drops = [
        ("fact_clima_mensual", ["brillo_solar_horas_dia"]),
        ("fact_precios_mayoristas", ["volumen_abastecimiento_ton"]),
        ("fact_alerta_enso", ["anomalia_precipitacion_pct", "probabilidad_deficit_hidrico", "probabilidad_exceso_hidrico"]),
        ("fact_precios_insumos", ["unidad_medida", "fuente_origen"])
    ]
    
    with engine.begin() as conn:
        for table, cols in drops:
            for col in cols:
                try:
                    sql = f"ALTER TABLE {table} DROP COLUMN IF EXISTS {col}"
                    conn.execute(text(sql))
                    logger.info(f"-> {table}.{col} eliminada.")
                except Exception as e:
                    logger.warning(f"-> Error eliminando {table}.{col}: {e}")

if __name__ == "__main__":
    try:
        engine = get_engine()
        drop_columns_logic(engine, "BASE DE DATOS CONFIGURADA")
    except Exception as e:
        logger.error(f"Error durante el proceso: {e}")

    logger.info("Proceso de adelgazamiento finalizado.")
