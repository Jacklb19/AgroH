import os
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

import urllib.parse

def get_engine():
    password = os.getenv('SUPABASE_DB_PASSWORD', '')
    encoded_password = urllib.parse.quote_plus(password)
    url = (
        f"postgresql+psycopg2://{os.getenv('SUPABASE_DB_USER')}:"
        f"{encoded_password}@"
        f"{os.getenv('SUPABASE_DB_HOST')}:"
        f"{os.getenv('SUPABASE_DB_PORT', 5432)}/"
        f"{os.getenv('SUPABASE_DB_NAME', 'postgres')}"
    )
    return create_engine(url, connect_args={"sslmode": "require"}, pool_pre_ping=True)


def init_schema(engine):
    """Ejecuta schema.sql para crear todas las tablas si no existen."""
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with engine.begin() as conn:
        conn.execute(text(sql))
    logger.info("Schema inicializado correctamente")


def upsert(engine, table: str, df, conflict_cols: list):
    """
    Inserta filas de un DataFrame en `table`.
    Si ya existe el registro (por conflict_cols), lo actualiza (ON CONFLICT DO UPDATE).
    """
    if df.empty:
        logger.warning(f"DataFrame vacío para tabla {table}, se omite")
        return

    cols = list(df.columns)
    placeholders = ", ".join([f":{c}" for c in cols])
    update_set = ", ".join([
        f"{c} = EXCLUDED.{c}" for c in cols if c not in conflict_cols
    ])
    conflict_str = ", ".join(conflict_cols)

    if update_set:
        stmt = f"""
            INSERT INTO {table} ({', '.join(cols)})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_str})
            DO UPDATE SET {update_set}
        """
    else:
        stmt = f"""
            INSERT INTO {table} ({', '.join(cols)})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_str})
            DO NOTHING
        """
    records = df.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(text(stmt), records)
    logger.info(f"{table}: {len(records)} filas insertadas/actualizadas")
