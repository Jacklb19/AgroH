import logging

import pandas as pd

from load.db import get_engine

logger = logging.getLogger(__name__)


def build_ml_features(engine=None):
    """
    Construye un feature store anual por municipio-cultivo con senales
    climaticas, economicas, territoriales y de memoria historica.
    """
    logger.info("Construyendo Feature Store para Machine Learning...")
    if engine is None:
        engine = get_engine()

    query = """
    WITH clima_agrupado AS (
        SELECT
            fc.id_municipio,
            dt.anio,
            SUM(fc.precipitacion_mm) AS lluvia_acumulada_anual,
            AVG(fc.temperatura_media_c) AS temp_promedio_anual,
            MAX(fc.temperatura_max_c) AS temp_maxima_anual,
            AVG(fc.temperatura_min_c) AS temp_minima_anual,
            AVG(fc.humedad_relativa_pct) AS humedad_relativa_anual,
            AVG(fc.brillo_solar_horas_dia) AS brillo_solar_anual
        FROM fact_clima_mensual fc
        JOIN dim_tiempo dt ON dt.id_tiempo = fc.id_tiempo
        GROUP BY fc.id_municipio, dt.anio
    ),
    precios_agrupados AS (
        SELECT
            fpm.id_cultivo,
            dt.anio,
            AVG(fpm.precio_promedio_cop_kg) AS precio_promedio_anual_cop_kg,
            SUM(COALESCE(fpm.volumen_abastecimiento_ton, 0)) AS volumen_total_anual_ton
        FROM fact_precios_mayoristas fpm
        JOIN dim_tiempo dt ON dt.id_tiempo = fpm.id_tiempo
        GROUP BY fpm.id_cultivo, dt.anio
    ),
    insumos_agrupados AS (
        SELECT
            fpi.id_region,
            dt.anio,
            AVG(fpi.precio_cop_unidad) AS costo_promedio_insumos_region,
            SUM(CASE WHEN COALESCE(fpi.es_sintetico, FALSE) THEN 1 ELSE 0 END) AS n_insumos_sinteticos
        FROM fact_precios_insumos fpi
        JOIN dim_tiempo dt ON dt.id_tiempo = fpi.id_tiempo
        GROUP BY fpi.id_region, dt.anio
    ),
    aptitud_suelo_cultivo AS (
        SELECT
            fas.id_municipio,
            fas.id_cultivo,
            CASE fas.clase_aptitud
                WHEN 'alta' THEN 3
                WHEN 'moderada' THEN 2
                WHEN 'marginal' THEN 1
                WHEN 'no_apta' THEN 0
                ELSE NULL
            END AS aptitud_score
        FROM fact_aptitud_suelo fas
    ),
    aptitud_suelo_municipio AS (
        SELECT
            fas.id_municipio,
            MAX(
                CASE fas.clase_aptitud
                    WHEN 'alta' THEN 3
                    WHEN 'moderada' THEN 2
                    WHEN 'marginal' THEN 1
                    WHEN 'no_apta' THEN 0
                    ELSE NULL
                END
            ) AS aptitud_score
        FROM fact_aptitud_suelo fas
        GROUP BY fas.id_municipio
    ),
    censo_agregado AS (
        SELECT
            fca.id_municipio,
            MAX(fca.area_cultivos_permanentes_ha) AS area_cultivos_permanentes_ha,
            MAX(fca.area_cultivos_transitorios_ha) AS area_cultivos_transitorios_ha
        FROM fact_censo_agropecuario fca
        GROUP BY fca.id_municipio
    ),
    produccion AS (
        SELECT
            fp.id_municipio,
            fp.id_tiempo,
            dt.anio,
            fp.id_cultivo,
            dm.id_region,
            dm.id_departamento,
            fp.area_sembrada_ha,
            fp.area_cosechada_ha,
            fp.rendimiento_t_ha
        FROM fact_produccion_agricola fp
        JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
        JOIN dim_municipio dm ON dm.id_municipio = fp.id_municipio
    )
    SELECT
        p.id_municipio,
        p.id_tiempo,
        p.anio,
        p.id_cultivo,
        p.id_region,
        p.id_departamento,
        p.area_sembrada_ha,
        p.area_cosechada_ha,
        p.rendimiento_t_ha,
        c.lluvia_acumulada_anual,
        c.temp_promedio_anual,
        c.temp_maxima_anual,
        c.temp_minima_anual,
        c.humedad_relativa_anual,
        c.brillo_solar_anual,
        pr.precio_promedio_anual_cop_kg,
        pr.volumen_total_anual_ton,
        i.costo_promedio_insumos_region,
        i.n_insumos_sinteticos,
        COALESCE(ae.aptitud_score, am.aptitud_score) AS aptitud_score,
        ca.area_cultivos_permanentes_ha,
        ca.area_cultivos_transitorios_ha
    FROM produccion p
    LEFT JOIN clima_agrupado c
        ON p.id_municipio = c.id_municipio
       AND p.anio = c.anio
    LEFT JOIN precios_agrupados pr
        ON p.id_cultivo = pr.id_cultivo
       AND p.anio = pr.anio
    LEFT JOIN insumos_agrupados i
        ON p.id_region = i.id_region
       AND p.anio = i.anio
    LEFT JOIN aptitud_suelo_cultivo ae
        ON p.id_municipio = ae.id_municipio
       AND p.id_cultivo = ae.id_cultivo
    LEFT JOIN aptitud_suelo_municipio am
        ON p.id_municipio = am.id_municipio
    LEFT JOIN censo_agregado ca
        ON p.id_municipio = ca.id_municipio;
    """
    try:
        df_features = pd.read_sql(query, engine)
        if df_features.empty:
            logger.warning("Feature Store vacio")
            return df_features

        df_features["id_departamento_num"] = pd.to_numeric(
            df_features["id_departamento"], errors="coerce"
        )
        df_features["id_municipio_num"] = pd.to_numeric(
            df_features["id_municipio"], errors="coerce"
        )
        df_features = df_features.sort_values(["id_municipio", "id_cultivo", "anio"])

        grupo_cultivo = df_features.groupby(["id_municipio", "id_cultivo"], sort=False)
        grupo_municipio = df_features.groupby(["id_municipio"], sort=False)

        df_features["rendimiento_lag_1"] = grupo_cultivo["rendimiento_t_ha"].shift(1)
        df_features["rendimiento_promedio_3"] = grupo_cultivo["rendimiento_t_ha"].transform(
            lambda serie: serie.shift(1).rolling(window=3, min_periods=1).mean()
        )
        df_features["area_sembrada_lag_1"] = grupo_cultivo["area_sembrada_ha"].shift(1)
        df_features["lluvia_lag_1"] = grupo_municipio["lluvia_acumulada_anual"].shift(1)
        df_features["temp_promedio_lag_1"] = grupo_municipio["temp_promedio_anual"].shift(1)
        df_features["precio_lag_1"] = grupo_cultivo["precio_promedio_anual_cop_kg"].shift(1)
        df_features["costo_insumos_lag_1"] = grupo_municipio["costo_promedio_insumos_region"].shift(1)
        df_features["variacion_lluvia_interanual"] = (
            df_features["lluvia_acumulada_anual"] - df_features["lluvia_lag_1"]
        )
        df_features["variacion_precio_interanual"] = (
            df_features["precio_promedio_anual_cop_kg"] - df_features["precio_lag_1"]
        )

        logger.info(
            "Feature Store construido: %s filas (una fila = un ano de cosecha).",
            len(df_features),
        )
        return df_features
    except Exception as exc:
        logger.error("Error construyendo Feature Store: %s", exc)
        return pd.DataFrame()
