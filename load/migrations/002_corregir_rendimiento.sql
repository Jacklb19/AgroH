-- ════════════════════════════════════════════════════════════════════════
-- Migración 002 · Corrige el rendimiento inflado y actualiza las vistas de Power BI
-- ════════════════════════════════════════════════════════════════════════
--
-- Problema: load/load_facts.py sumaba rendimiento_t_ha de los semestres A y B
-- (y de las variedades de un mismo cultivo). ~36 % de las filas de
-- fact_produccion_agricola quedaron infladas (p. ej. arroz en Ibagué: 15,2 en
-- lugar de 7,6 t/ha). Área y producción sí estaban bien sumadas.
--
-- Qué hace:
--   1. Respalda fact_produccion_agricola en backup_002_fact_produccion_agricola.
--   2. Recalcula rendimiento_t_ha = producción ÷ área cosechada
--      (NULL cuando no hay área o producción: el rendimiento no es calculable).
--   3. Redirige v_predicciones_modelo (real vs. predicho en Power BI) al modelo
--      de pronóstico activo (pred_pronostico), con las mismas columnas y tipos.
--   4. Registra la migración en schema_migrations.
--
-- v_dashboard_agro no necesita cambios: lee rendimiento_t_ha, que queda corregido.
--
-- Cómo ejecutarla: SQL Editor de Supabase → pegar todo el archivo → Run.
-- Todo va en una sola transacción: si algo falla, no se aplica nada.
-- Después, en Power BI: Inicio → Actualizar (o esperar la actualización programada).
-- ════════════════════════════════════════════════════════════════════════

BEGIN;

-- 0. Evita aplicarla dos veces
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM schema_migrations WHERE version = 2) THEN
    RAISE EXCEPTION 'La migración 002 ya fue aplicada.';
  END IF;
END $$;

-- 1. Respaldo
CREATE TABLE backup_002_fact_produccion_agricola AS TABLE fact_produccion_agricola;

-- 2. Rendimiento correcto
UPDATE fact_produccion_agricola
SET rendimiento_t_ha = produccion_total_ton / area_cosechada_ha
WHERE area_cosechada_ha > 0 AND produccion_total_ton > 0;

UPDATE fact_produccion_agricola
SET rendimiento_t_ha = NULL
WHERE NOT (area_cosechada_ha > 0 AND produccion_total_ton > 0);

-- 3. Real vs. predicho desde el modelo de pronóstico activo.
--    Años 2022-2024: backtest (predicción hecha sin conocer ese año) con su valor real.
--    Años siguientes: pronóstico en escenario neutral (rendimiento_real vacío).
CREATE OR REPLACE VIEW v_predicciones_modelo AS
WITH v AS (
  SELECT id_version FROM model_version
  WHERE nombre_modelo = 'xgboost_pronostico' AND activo
  ORDER BY id_version DESC LIMIT 1
)
SELECT m.id_municipio                                            AS codigo_divipola,
       m.nombre_municipio,
       m.nombre_departamento,
       rn.nombre_region,
       m.latitud_centroide,
       m.longitud_centroide,
       c.nombre_cultivo,
       p.anio,
       p.rendimiento_real,
       GREATEST(p.rendimiento_predicho, 0::double precision)     AS rendimiento_predicho,
       abs(p.rendimiento_real - GREATEST(p.rendimiento_predicho, 0::double precision)) AS error_absoluto,
       GREATEST(p.limite_inferior, 0::double precision)          AS intervalo_confianza_inferior,
       GREATEST(p.limite_superior, 0::double precision)          AS intervalo_confianza_superior,
       mv.nombre_modelo,
       mv.metricas_json,
       mv.fecha_entrenamiento
FROM pred_pronostico p
JOIN v ON v.id_version = p.id_version
JOIN model_version mv ON mv.id_version = p.id_version
JOIN dim_municipio m  ON m.id_municipio = p.id_municipio
JOIN dim_cultivo c    ON c.id_cultivo   = p.id_cultivo
LEFT JOIN dim_region_natural rn ON rn.id_region = m.id_region
WHERE p.tipo = 'backtest' OR p.escenario = 'Neutral';

-- 4. Registro
INSERT INTO schema_migrations (version, description)
VALUES (2, 'Rendimiento = producción ÷ área cosechada; v_predicciones_modelo desde pred_pronostico');

-- Verificación (debe mostrar ~32.500 filas corregidas y arroz en Ibagué ≈ 7,6)
SELECT
  (SELECT COUNT(*) FROM fact_produccion_agricola f
     JOIN backup_002_fact_produccion_agricola b USING (id)
    WHERE b.rendimiento_t_ha > 1.3 * f.rendimiento_t_ha)             AS filas_que_estaban_infladas,
  (SELECT ROUND(AVG(rendimiento_t_ha)::numeric, 2) FROM fact_produccion_agricola f
     JOIN dim_municipio m USING (id_municipio) JOIN dim_cultivo c USING (id_cultivo)
    WHERE m.id_municipio = '73001' AND c.nombre_cultivo = 'Arroz')   AS arroz_ibague_t_ha,
  (SELECT COUNT(*) FROM v_predicciones_modelo)                        AS filas_vista_predicciones;

COMMIT;

-- ════════════════════════════════════════════════════════════════════════
-- Reversión (solo si hiciera falta): ejecutar load/migrations/002_revertir.sql
-- ════════════════════════════════════════════════════════════════════════
