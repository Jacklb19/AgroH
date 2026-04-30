
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

# Credenciales de Supabase (capturadas anteriormente)
SB_USER = "postgres.snkgncnteijzyobofzis"
SB_PASS = "U/C8KcPC-#m,e&c"
SB_HOST = "aws-1-us-west-2.pooler.supabase.com"
SB_PORT = 5432
SB_NAME = "postgres"

def get_supabase_engine():
    encoded_password = urllib.parse.quote_plus(SB_PASS)
    url = f"postgresql+psycopg2://{SB_USER}:{encoded_password}@{SB_HOST}:{SB_PORT}/{SB_NAME}"
    return create_engine(url, connect_args={"sslmode": "require"})

def drop_columns_logic(engine, label):
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
    # 1. Local
    try:
        local_engine = get_engine()
        drop_columns_logic(local_engine, "LOCAL")
    except Exception as e:
        logger.error(f"Error en LOCAL: {e}")

    # 2. Supabase
    try:
        sb_engine = get_supabase_engine()
        drop_columns_logic(sb_engine, "SUPABASE")
    except Exception as e:
        logger.error(f"Error en SUPABASE: {e}")

    logger.info("Proceso de adelgazamiento finalizado.")
