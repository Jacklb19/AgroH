"""
clean_clima.py — Limpieza y unificación de datos climáticos IDEAM.

Recibe DataFrames ya agregados a nivel mensual desde el extractor
(que usa SoQL del servidor Socrata) y los unifica en un solo DataFrame
alineado al schema de fact_clima_mensual.
"""
import pandas as pd
import numpy as np
import logging
from config.settings import DATA_PROCESSED

logger = logging.getLogger(__name__)


def unificar_clima_mensual(df_precip: pd.DataFrame,
                           df_combinado: pd.DataFrame) -> pd.DataFrame:
    """
    Unifica datos de precipitación y variables combinadas en un solo DataFrame
    con las columnas del schema de fact_clima_mensual.

    Ambos DataFrames vienen del extractor con columnas:
      codigoestacion, anio, mes, valor_agregado, num_lecturas

    Para precipitación: valor_agregado = suma mensual (mm)
    Para combinado: valor_agregado = promedio mensual (°C, %, horas)
    """
    result_dfs = []

    # --- Precipitación: ya viene como SUMA mensual ---
    if not df_precip.empty:
        df_p = df_precip.copy()
        df_p = df_p.rename(columns={
            "codigoestacion": "id_estacion",
            "valor_agregado": "precipitacion_mm"
        })
        df_p["anio"] = pd.to_numeric(df_p["anio"], errors="coerce").astype("Int64")
        df_p["mes"] = pd.to_numeric(df_p["mes"], errors="coerce").astype("Int64")
        df_p["precipitacion_mm"] = pd.to_numeric(df_p["precipitacion_mm"], errors="coerce")
        df_p = df_p[["id_estacion", "anio", "mes", "precipitacion_mm"]].dropna(
            subset=["id_estacion", "anio", "mes"]
        )
        result_dfs.append(df_p)
        logger.info(f"Precipitación mensual: {len(df_p)} registros")

    # --- Clima combinado: ya viene como PROMEDIO mensual ---
    # Este dataset mezcla variables (temperatura, humedad, etc.) diferenciadas por descripcionsensor.
    if not df_combinado.empty:
        df_c = df_combinado.copy()
        df_c["variable"] = "otro"
        sensor = df_c["descripcionsensor"].astype(str).str.lower()
        df_c.loc[sensor.str.contains("temperatura|temp", na=False), "variable"] = "temperatura_media_c"
        df_c.loc[sensor.str.contains("humedad", na=False), "variable"] = "humedad_relativa_pct"
        df_c.loc[sensor.str.contains("brillo|solar|radiaci", na=False), "variable"] = "brillo_solar_horas_dia"
        
        # Solo tomamos lo que nos interesa
        df_c = df_c[df_c["variable"] != "otro"]
        
        if not df_c.empty:
            df_c_pivot = df_c.pivot_table(
                index=["codigoestacion", "anio", "mes"],
                columns="variable",
                values="valor_agregado",
                aggfunc="mean"
            ).reset_index()
            
            df_c_pivot = df_c_pivot.rename(columns={"codigoestacion": "id_estacion"})
            df_c_pivot["anio"] = pd.to_numeric(df_c_pivot["anio"], errors="coerce").astype("Int64")
            df_c_pivot["mes"] = pd.to_numeric(df_c_pivot["mes"], errors="coerce").astype("Int64")
            
            # Limpiar nulos en llaves
            df_c_pivot = df_c_pivot.dropna(subset=["id_estacion", "anio", "mes"])
            result_dfs.append(df_c_pivot)
            logger.info(f"Clima combinado mensual: {len(df_c_pivot)} registros unificados")

    if not result_dfs:
        logger.warning("Sin datos climáticos para unificar")
        return pd.DataFrame()

    # Unir precipitación + temperatura por estación/año/mes
    if len(result_dfs) == 2:
        result = result_dfs[0].merge(result_dfs[1], on=["id_estacion", "anio", "mes"], how="outer")
    else:
        result = result_dfs[0]

    # Asegurar columnas del schema (rellenar con None las que no tenemos aún)
    for col in ["temperatura_max_c", "temperatura_min_c", "humedad_relativa_pct", "brillo_solar_horas_dia"]:
        if col not in result.columns:
            result[col] = None

    out = DATA_PROCESSED / "clima_mensual.parquet"
    result.to_parquet(out, index=False)
    logger.info(f"Clima mensual unificado: {len(result)} registros → {out}")
    return result
