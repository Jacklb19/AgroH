from load.db import get_engine
from sqlalchemy import text

def update_view_numeric_coords():
    engine = get_engine()
    drop_sql = "DROP VIEW IF EXISTS v_dashboard_agro;"
    
    # Forzamos el casting a NUMERIC para que Power BI no tenga dudas
    create_sql = """
    CREATE VIEW v_dashboard_agro AS
    WITH clima_anual AS (
        SELECT 
            id_municipio, 
            anio,
            AVG(precipitacion_mm) as precip,
            AVG(temperatura_media_c) as temp,
            AVG(humedad_relativa_pct) as hum
        FROM fact_clima_mensual fc
        JOIN dim_tiempo tc ON tc.id_tiempo = fc.id_tiempo
        GROUP BY id_municipio, anio
    )
    SELECT 
        m.id_municipio as codigo_divipola,
        m.nombre_municipio,
        m.nombre_departamento,
        CAST(m.latitud_centroide AS NUMERIC) as latitud_centroide,
        CAST(m.longitud_centroide AS NUMERIC) as longitud_centroide,
        c.nombre_cultivo,
        t.anio,
        fp.area_sembrada_ha,
        fp.area_cosechada_ha,
        fp.produccion_total_ton,
        fp.rendimiento_t_ha,
        ca.precip as precipitacion_mm,
        ca.temp as temperatura_media_c,
        ca.hum as humedad_relativa_pct
    FROM fact_produccion_agricola fp
    JOIN dim_municipio m ON m.id_municipio = fp.id_municipio
    JOIN dim_cultivo c ON c.id_cultivo = fp.id_cultivo
    JOIN dim_tiempo t ON t.id_tiempo = fp.id_tiempo
    LEFT JOIN clima_anual ca ON ca.id_municipio = fp.id_municipio AND ca.anio = t.anio;
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(drop_sql))
            conn.execute(text(create_sql))
            conn.commit()
        print("VIEW UPDATED WITH CASTED NUMERIC COORDINATES")
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    update_view_numeric_coords()
