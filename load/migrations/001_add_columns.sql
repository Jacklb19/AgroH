-- Migración 001: columnas agregadas después del schema inicial
-- Fecha: 2026-04-29
-- Aplicar con: psql -f migrations/001_add_columns.sql
-- Corrección 3.5.c: ALTER TABLE separados del DDL inicial.

-- Constraints en fact_precios_insumos
ALTER TABLE fact_precios_insumos
    DROP CONSTRAINT IF EXISTS fact_precios_insumos_id_tiempo_tipo_insumo_nombre_insumo_key;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fact_precios_insumos_unique_region'
    ) THEN
        ALTER TABLE fact_precios_insumos
            ADD CONSTRAINT fact_precios_insumos_unique_region
            UNIQUE (id_tiempo, tipo_insumo, nombre_insumo, id_region);
    END IF;
END $$;

-- Constraints en pred_rendimiento
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'pred_rendimiento_unique_natural_key'
    ) THEN
        ALTER TABLE pred_rendimiento
            ADD CONSTRAINT pred_rendimiento_unique_natural_key
            UNIQUE (id_municipio, id_cultivo, id_tiempo);
    END IF;
END $$;

-- Constraints en pred_alerta_climatica
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'pred_alerta_climatica_unique_natural_key'
    ) THEN
        ALTER TABLE pred_alerta_climatica
            ADD CONSTRAINT pred_alerta_climatica_unique_natural_key
            UNIQUE (id_municipio, id_tiempo);
    END IF;
END $$;

-- Columna es_cierre_anual en dim_tiempo
ALTER TABLE dim_tiempo ADD COLUMN IF NOT EXISTS es_cierre_anual BOOLEAN NOT NULL DEFAULT FALSE;

-- Columna id_tiempo FK en fact_censo_agropecuario (si no existe ya)
ALTER TABLE fact_censo_agropecuario ADD COLUMN IF NOT EXISTS id_tiempo INT REFERENCES dim_tiempo(id_tiempo);

-- Registrar migración
INSERT INTO schema_migrations(version, description)
VALUES (1, 'Constraints, es_cierre_anual, id_tiempo FK en CNA') ON CONFLICT DO NOTHING;
