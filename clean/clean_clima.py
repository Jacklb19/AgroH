"""
clean_clima.py — Limpieza y unificación de datos climáticos IDEAM.
# FIX v1: Estandarización de logs y filtros de calidad robustos.
"""
import pandas as pd
import numpy as np
import logging
from config.settings import DATA_PROCESSED

logger = logging.getLogger(__name__)


def unificar_clima_mensual(df_precip: pd.DataFrame,
                           df_combinado: pd.DataFrame) -> pd.DataFrame:
    """
    Unifica datos de precipitación y variables combinadas.
    """
    result_dfs = []

    # --- Precipitación ---
    if df_precip is not None and not df_precip.empty:
        df_p = df_precip.copy()
        if "total_valor" not in df_p.columns and "valor_agregado" in df_p.columns:
            df_p["total_valor"] = df_p["valor_agregado"]
            
        df_p = df_p.rename(columns={
            "codigoestacion": "id_estacion",
            "total_valor": "precipitacion_mm"
        })
        
        df_p["anio"] = pd.to_numeric(df_p["anio"], errors="coerce").astype("Int64")
        df_p["mes"] = pd.to_numeric(df_p["mes"], errors="coerce").astype("Int64")
        df_p["precipitacion_mm"] = pd.to_numeric(df_p["precipitacion_mm"], errors="coerce")
        
        # Filtro de calidad
        df_p.loc[df_p["precipitacion_mm"] < 0, "precipitacion_mm"] = 0
        df_p.loc[df_p["precipitacion_mm"] > 3000, "precipitacion_mm"] = np.nan
        
        df_p = df_p[["id_estacion", "anio", "mes", "precipitacion_mm"]].dropna(subset=["id_estacion", "anio", "mes"])
        result_dfs.append(df_p)
        logger.info("CLIMA: %s registros de precipitación unificados", len(df_p))

    # --- Clima combinado ---
    if df_combinado is not None and not df_combinado.empty:
        df_c = df_combinado.copy()
        if "promedio_valor" not in df_c.columns and "valor_agregado" in df_c.columns:
            df_c["promedio_valor"] = df_c["valor_agregado"]
            
        for col in ["promedio_valor", "max_valor", "min_valor"]:
            if col in df_c.columns:
                df_c[col] = pd.to_numeric(df_c[col], errors="coerce")
                
        df_c["variable"] = "otro"
        sensor = df_c["descripcionsensor"].astype(str).str.lower()
        df_c.loc[sensor.str.contains("temperatura|temp", na=False), "variable"] = "temperatura_media_c"
        df_c.loc[sensor.str.contains("humedad", na=False), "variable"] = "humedad_relativa_pct"
        df_c.loc[sensor.str.contains("brillo|solar|radiaci", na=False), "variable"] = "brillo_solar_horas_dia"
        
        df_c = df_c[df_c["variable"] != "otro"]
        
        if not df_c.empty:
            df_c_pivot = df_c.pivot_table(
                index=["codigoestacion", "anio", "mes"],
                columns="variable",
                values=["promedio_valor", "max_valor", "min_valor"],
                aggfunc="mean",
            ).reset_index()

            df_c_pivot.columns = ["_".join([str(p) for p in col if p]).strip("_") for col in df_c_pivot.columns]

            rename_cols = {
                "codigoestacion": "id_estacion",
                "promedio_valor_temperatura_media_c": "temperatura_media_c",
                "max_valor_temperatura_media_c": "temperatura_max_c",
                "min_valor_temperatura_media_c": "temperatura_min_c",
                "promedio_valor_humedad_relativa_pct": "humedad_relativa_pct",
                "promedio_valor_brillo_solar_horas_dia": "brillo_solar_horas_dia",
            }
            df_c_pivot = df_c_pivot.rename(columns=rename_cols)
            df_c_pivot["anio"] = pd.to_numeric(df_c_pivot["anio"], errors="coerce").astype("Int64")
            df_c_pivot["mes"] = pd.to_numeric(df_c_pivot["mes"], errors="coerce").astype("Int64")
            
            # Filtros de calidad
            for t_col in ["temperatura_media_c", "temperatura_max_c", "temperatura_min_c"]:
                if t_col in df_c_pivot.columns:
                    df_c_pivot.loc[(df_c_pivot[t_col] < -10) | (df_c_pivot[t_col] > 55), t_col] = np.nan
            
            if "humedad_relativa_pct" in df_c_pivot.columns:
                df_c_pivot.loc[(df_c_pivot["humedad_relativa_pct"] < 0) | (df_c_pivot["humedad_relativa_pct"] > 100), "humedad_relativa_pct"] = np.nan
                
            df_c_pivot = df_c_pivot.dropna(subset=["id_estacion", "anio", "mes"])
            result_dfs.append(df_c_pivot)
            logger.info("CLIMA: %s registros unificados (Temp/Hum/Brillo)", len(df_c_pivot))

    if not result_dfs:
        return pd.DataFrame()

    # Join final
    if len(result_dfs) == 2:
        result = result_dfs[0].merge(result_dfs[1], on=["id_estacion", "anio", "mes"], how="outer")
    else:
        result = result_dfs[0]

    out = DATA_PROCESSED / "clima_mensual.parquet"
    result.to_parquet(out, index=False)
    
    return result
