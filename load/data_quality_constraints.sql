-- ══════════════════════════════════════════════
-- AgroIA Colombia — Reglas de Calidad (Capa Fact)
-- Estas reglas previenen inyección de datos físicamente
-- imposibles a nivel de base de datos.
-- ══════════════════════════════════════════════

-- 0. Tabla de Bitácora de Calidad de Datos
CREATE TABLE IF NOT EXISTS data_quality_log (
    id SERIAL PRIMARY KEY,
    fecha_ejecucion TIMESTAMP DEFAULT NOW(),
    fuente VARCHAR(50) NOT NULL,
    total_raw INT NOT NULL,
    total_validos INT NOT NULL,
    total_invalidos INT NOT NULL,
    error_summary_json JSONB
);

-- Indexing strategy for performance over time (Query por fuente y fecha)
CREATE INDEX IF NOT EXISTS idx_dql_fuente_fecha ON data_quality_log(fuente, fecha_ejecucion);

-- 1. Producción Agrícola (Restricciones Seguras y Estables)

-- El área sembrada debe ser positiva o cero
ALTER TABLE fact_produccion_agricola DROP CONSTRAINT IF EXISTS chk_prod_area_sembrada_positiva;
ALTER TABLE fact_produccion_agricola
ADD CONSTRAINT chk_prod_area_sembrada_positiva 
CHECK (area_sembrada_ha >= 0);

-- El área cosechada debe ser positiva o cero
ALTER TABLE fact_produccion_agricola DROP CONSTRAINT IF EXISTS chk_prod_area_cosechada_positiva;
ALTER TABLE fact_produccion_agricola
ADD CONSTRAINT chk_prod_area_cosechada_positiva 
CHECK (area_cosechada_ha >= 0);

-- Físicamente imposible cosechar más área de la que se sembró
ALTER TABLE fact_produccion_agricola DROP CONSTRAINT IF EXISTS chk_prod_cosechada_le_sembrada;
ALTER TABLE fact_produccion_agricola
ADD CONSTRAINT chk_prod_cosechada_le_sembrada 
CHECK (area_cosechada_ha <= area_sembrada_ha);

-- La producción no puede ser negativa
ALTER TABLE fact_produccion_agricola DROP CONSTRAINT IF EXISTS chk_prod_total_positiva;
ALTER TABLE fact_produccion_agricola
ADD CONSTRAINT chk_prod_total_positiva 
CHECK (produccion_total_ton >= 0);

-- El rendimiento no puede ser negativo
ALTER TABLE fact_produccion_agricola DROP CONSTRAINT IF EXISTS chk_prod_rendimiento_positivo;
ALTER TABLE fact_produccion_agricola
ADD CONSTRAINT chk_prod_rendimiento_positivo 
CHECK (rendimiento_t_ha >= 0);

-- Columnas NOT NULL críticas que no deben fallar jamás
ALTER TABLE fact_produccion_agricola
ALTER COLUMN area_sembrada_ha SET NOT NULL,
ALTER COLUMN area_cosechada_ha SET NOT NULL,
ALTER COLUMN produccion_total_ton SET NOT NULL;
