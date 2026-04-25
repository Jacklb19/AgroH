from load.db import get_engine
from sqlalchemy import text

engine = get_engine()
with engine.begin() as conn:
    # 1. Borrar Vista existente
    conn.execute(text("DROP VIEW IF EXISTS v_dashboard_agro;"))
    
    # 2. Borrar columnas nulas de las tablas
    # fact_clima_mensual
    conn.execute(text("ALTER TABLE fact_clima_mensual DROP COLUMN IF EXISTS temperatura_max_c, DROP COLUMN IF EXISTS temperatura_media_c, DROP COLUMN IF EXISTS temperatura_min_c, DROP COLUMN IF EXISTS humedad_relativa_pct, DROP COLUMN IF EXISTS brillo_solar_horas_dia;"))
    # fact_censo_agropecuario
    conn.execute(text("ALTER TABLE fact_censo_agropecuario DROP COLUMN IF EXISTS pct_tenencia_propia, DROP COLUMN IF EXISTS pct_tenencia_arrendada, DROP COLUMN IF EXISTS pct_acceso_riego, DROP COLUMN IF EXISTS pct_asistencia_tecnica, DROP COLUMN IF EXISTS upa_promedio_ha;"))
    # fact_aptitud_suelo
    conn.execute(text("ALTER TABLE fact_aptitud_suelo DROP COLUMN IF EXISTS tipo_suelo, DROP COLUMN IF EXISTS textura_suelo, DROP COLUMN IF EXISTS pendiente_dominante, DROP COLUMN IF EXISTS drenaje, DROP COLUMN IF EXISTS limitante_principal;"))

    # 3. Re-crear Vista limpia
    conn.execute(text("""
    CREATE VIEW v_dashboard_agro AS
    WITH clima_anual AS (
        SELECT fc.id_municipio,
            tc.anio,
            avg(fc.precipitacion_mm) AS precip
        FROM fact_clima_mensual fc
        JOIN dim_tiempo tc ON tc.id_tiempo = fc.id_tiempo
        GROUP BY fc.id_municipio, tc.anio
    )
    SELECT m.id_municipio AS codigo_divipola,
        m.nombre_municipio,
        m.nombre_departamento,
        (m.latitud_centroide)::numeric AS latitud_centroide,
        (m.longitud_centroide)::numeric AS longitud_centroide,
        c.nombre_cultivo,
        t.anio,
        fp.area_sembrada_ha,
        fp.area_cosechada_ha,
        fp.produccion_total_ton,
        fp.rendimiento_t_ha,
        ca.precip AS precipitacion_mm
    FROM fact_produccion_agricola fp
    JOIN dim_municipio m ON m.id_municipio = fp.id_municipio
    JOIN dim_cultivo c ON c.id_cultivo = fp.id_cultivo
    JOIN dim_tiempo t ON t.id_tiempo = fp.id_tiempo
    LEFT JOIN clima_anual ca ON ca.id_municipio = fp.id_municipio AND ca.anio = t.anio;
    """))

print("Database cleaned and View updated successfully.")
