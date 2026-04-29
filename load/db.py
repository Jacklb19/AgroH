import logging
import math
import urllib.parse
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from config.settings import DB

logger = logging.getLogger(__name__)

def get_engine() -> Engine:
    """
    Crea el motor SQLAlchemy usando la configuración centralizada en settings.DB.
    # FIX v1: Uso de configuración centralizada y manejo robusto de SSL.
    """
    password = DB.get("password") or ""
    encoded_password = urllib.parse.quote_plus(password)
    host = DB.get("host", "localhost")
    port = DB.get("port", 5432)
    dbname = DB.get("dbname", "postgres")
    user = DB.get("user", "postgres")

    url = f"postgresql+psycopg2://{user}:{encoded_password}@{host}:{port}/{dbname}"
    
    # SSL require para conexiones remotas (Supabase), disable para local
    ssl_mode = "disable" if host in ["localhost", "127.0.0.1"] else "require"
    
    try:
        engine = create_engine(url, connect_args={"sslmode": ssl_mode}, pool_pre_ping=True)
        # Test de conexión rápido
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except Exception as e:
        logger.error("DB: Error al conectar a la base de datos: %s", e)
        # No levantamos excepción para permitir que el pipeline siga (ej. modo offline)
        return None

def init_schema(engine: Engine):
    """
    Ejecuta schema.sql para crear todas las tablas si no existen.
    # FIX v1: Manejo de errores en inicialización de schema.
    """
    if engine is None:
        logger.error("DB: No se puede inicializar schema sin motor de base de datos.")
        return

    import os
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    try:
        with open(schema_path, "r", encoding="utf-8") as f:
            sql = f.read()
        with engine.begin() as conn:
            conn.execute(text(sql))
        logger.info("DB: Schema inicializado correctamente")
    except Exception as e:
        logger.error("DB: Error al inicializar schema: %s", e)

def upsert(engine: Engine, table: str, df, conflict_cols: list):
    """
    Inserta filas de un DataFrame en `table` con lógica ON CONFLICT.
    # FIX v1: Optimización de limpieza de NaNs y manejo de errores granular.
    """
    if engine is None:
        logger.warning("DB: Ignorando upsert en %s (sin motor de base de datos)", table)
        return

    if df is None or df.empty:
        logger.warning("DB: DataFrame vacío para tabla %s, se omite", table)
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
    
    # Preparar registros limpiando NaNs para Postgres
    records = df.to_dict(orient="records")
    
    # Reemplazar NaN con None (NULL en SQL) de forma eficiente
    # FIX: Solo procesar si el valor es float nan para evitar overhead innecesario
    for row in records:
        for k, v in row.items():
            if v is not None and isinstance(v, float) and math.isnan(v):
                row[k] = None

    try:
        with engine.begin() as conn:
            conn.execute(text(stmt), records)
        logger.info("DB: %s -> %s filas insertadas/actualizadas", table, len(records))
    except Exception as e:
        logger.error("DB: Error en upsert para tabla %s: %s", table, e)
