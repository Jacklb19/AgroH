-- Reversión de la migración 002: restaura el rendimiento original y la vista anterior.
-- Ejecutar en el SQL Editor de Supabase solo si hiciera falta volver atrás.

BEGIN;

UPDATE fact_produccion_agricola f SET rendimiento_t_ha = b.rendimiento_t_ha
FROM backup_002_fact_produccion_agricola b WHERE b.id = f.id;

CREATE OR REPLACE VIEW v_predicciones_modelo AS
WITH pred_anual AS (
         SELECT ranked.id,
            ranked.id_municipio,
            ranked.id_cultivo,
            ranked.id_tiempo,
            ranked.rendimiento_predicho_t_ha,
            ranked.intervalo_confianza_inferior,
            ranked.intervalo_confianza_superior,
            ranked.id_version,
            ranked.anio,
            ranked.rn
           FROM ( SELECT pr_1.id,
                    pr_1.id_municipio,
                    pr_1.id_cultivo,
                    pr_1.id_tiempo,
                    pr_1.rendimiento_predicho_t_ha,
                    pr_1.intervalo_confianza_inferior,
                    pr_1.intervalo_confianza_superior,
                    pr_1.id_version,
                    t.anio,
                    row_number() OVER (PARTITION BY pr_1.id_municipio, pr_1.id_cultivo, t.anio ORDER BY t.es_cierre_anual DESC, pr_1.id_tiempo DESC) AS rn
                   FROM pred_rendimiento pr_1
                     JOIN dim_tiempo t ON t.id_tiempo = pr_1.id_tiempo) ranked
          WHERE ranked.rn = 1
        )
 SELECT m.id_municipio AS codigo_divipola,
    m.nombre_municipio,
    m.nombre_departamento,
    rn.nombre_region,
    m.latitud_centroide,
    m.longitud_centroide,
    c.nombre_cultivo,
    pr.anio,
    fp.rendimiento_t_ha AS rendimiento_real,
    GREATEST(pr.rendimiento_predicho_t_ha, 0::double precision) AS rendimiento_predicho,
    abs(fp.rendimiento_t_ha - GREATEST(pr.rendimiento_predicho_t_ha, 0::double precision)) AS error_absoluto,
    GREATEST(pr.intervalo_confianza_inferior, 0::double precision) AS intervalo_confianza_inferior,
    GREATEST(pr.intervalo_confianza_superior, GREATEST(pr.intervalo_confianza_inferior, 0::double precision), 0::double precision) AS intervalo_confianza_superior,
    mv.nombre_modelo,
    mv.metricas_json,
    mv.fecha_entrenamiento
   FROM pred_anual pr
     JOIN dim_municipio m ON m.id_municipio = pr.id_municipio
     JOIN dim_cultivo c ON c.id_cultivo = pr.id_cultivo
     JOIN model_version mv ON mv.id_version = pr.id_version AND mv.activo = true
     LEFT JOIN dim_region_natural rn ON rn.id_region = m.id_region
     LEFT JOIN fact_produccion_agricola fp ON fp.id_municipio = pr.id_municipio AND fp.id_cultivo = pr.id_cultivo AND fp.id_tiempo = pr.id_tiempo;

DELETE FROM schema_migrations WHERE version = 2;

COMMIT;
