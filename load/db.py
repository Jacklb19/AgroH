"""
load/db.py — Motor de base de datos y operaciones de upsert.

Correcciones aplicadas:
  - Corrección 3.1.a (2026-04-29): get_engine lanza DatabaseConnectionError
    en vez de retornar None. upsert lanza RuntimeError si engine es None.
    Parámetro fail_silently para compatibilidad.
  - Corrección 3.1.b (2026-04-29): upsert_batch en batch usando
    sqlalchemy.dialects.postgresql.insert para tablas de hechos.
"""
import logging
import math
import urllib.parse

from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.engine import Engine

from config.settings import DB

logger = logging.getLogger(__name__)


class DatabaseConnectionError(Exception):
    """Excepción lanzada cuando la conexión a la base de datos falla."""
    pass


def get_engine(fail_silently: bool = False) -> Engine:
    """
    Crea el motor SQLAlchemy usando la configuración centralizada en settings.DB.

    Corrección 3.1.a: Lanza DatabaseConnectionError en vez de retornar None.
    Si fail_silently=True, retorna None como antes para modo offline.
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
        if fail_silently:
            return None
        raise DatabaseConnectionError(
            f"No se pudo conectar a la base de datos ({host}:{port}/{dbname}): {e}"
        ) from e


def init_schema(engine: Engine):
    """
    Ejecuta schema.sql para crear todas las tablas si no existen.
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
    Para tablas de dimensiones (pequeñas).

    Corrección 3.1.a: Lanza RuntimeError si engine es None.
    """
    if engine is None:
        raise RuntimeError(
            f"DB: No se puede ejecutar upsert en '{table}' sin motor de base de datos. "
            "Verifique la conexión a la BD."
        )

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


def upsert_batch(engine: Engine, table_name: str, records: list[dict],
                 conflict_cols: list[str], batch_size: int = 1000):
    """
    Upsert en batch usando PostgreSQL INSERT ... ON CONFLICT.

    Corrección 3.1.b: Operación en batch para tablas de hechos con millones de registros.
    Usa sqlalchemy.dialects.postgresql.insert para eficiencia.
    """
    if engine is None:
        raise RuntimeError(
            f"DB: No se puede ejecutar upsert_batch en '{table_name}' sin motor de base de datos."
        )

    if not records:
        logger.warning("DB: Lista de registros vacía para tabla %s, se omite", table_name)
        return

    # Limpiar NaNs en todos los registros
    for row in records:
        for k, v in row.items():
            if v is not None and isinstance(v, float) and math.isnan(v):
                row[k] = None

    try:
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        meta = MetaData()
        table = Table(table_name, meta, autoload_with=engine)

        total_inserted = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            stmt = pg_insert(table).values(batch)
            update_cols = {
                c.name: stmt.excluded[c.name]
                for c in table.columns
                if c.name not in conflict_cols
            }
            if update_cols:
                stmt = stmt.on_conflict_do_update(
                    index_elements=conflict_cols,
                    set_=update_cols
                )
            else:
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=conflict_cols
                )
            with engine.begin() as conn:
                conn.execute(stmt)
            total_inserted += len(batch)

        logger.info("DB: %s -> %s filas procesadas en batch", table_name, total_inserted)
    except ImportError:
        # Fallback si sqlalchemy.dialects.postgresql no está disponible
        logger.warning("DB: PostgreSQL dialect no disponible, usando upsert clásico para %s", table_name)
        import pandas as pd
        df = pd.DataFrame(records)
        upsert(engine, table_name, df, conflict_cols)
    except Exception as e:
        logger.error("DB: Error en upsert_batch para tabla %s: %s", table_name, e)
