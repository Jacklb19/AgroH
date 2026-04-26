"""Migración: Agregar columnas climáticas faltantes a fact_clima_mensual."""
import sys; sys.path.insert(0, ".")
from load.db import get_engine
from sqlalchemy import text

engine = get_engine()

ALTER_STATEMENTS = [
    "ALTER TABLE fact_clima_mensual ADD COLUMN IF NOT EXISTS temperatura_media_c DOUBLE PRECISION",
    "ALTER TABLE fact_clima_mensual ADD COLUMN IF NOT EXISTS temperatura_max_c DOUBLE PRECISION",
    "ALTER TABLE fact_clima_mensual ADD COLUMN IF NOT EXISTS temperatura_min_c DOUBLE PRECISION",
    "ALTER TABLE fact_clima_mensual ADD COLUMN IF NOT EXISTS humedad_relativa_pct DOUBLE PRECISION",
    "ALTER TABLE fact_clima_mensual ADD COLUMN IF NOT EXISTS brillo_solar_horas_dia DOUBLE PRECISION",
]

with engine.begin() as conn:
    for stmt in ALTER_STATEMENTS:
        conn.execute(text(stmt))
        print(f"OK: {stmt.split('ADD COLUMN IF NOT EXISTS ')[1]}")

print("\n✅ Migración completada.")
