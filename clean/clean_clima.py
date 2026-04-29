"""
clean_clima.py — Limpieza y unificación de datos climáticos IDEAM.

Correcciones aplicadas:
  - Corrección 2.1.a (2026-04-29): Eliminado fallback a valor_agregado obsoleto.
    Lógica explícita: precipitación usa total_valor, temp/humedad usa promedio_valor.
  - Corrección 2.1.b (2026-04-29): Pivot robusto con _normalizar_tipo_sensor().
    No descarta silenciosamente columnas 'otro' — las loguea como 'sensor_desconocido'.
  - Corrección 2.1.c (2026-04-29): Propaga es_sintetico. Separa output real/sintético.
"""
import logging

import numpy as np
import pandas as pd

from config.settings import DATA_PROCESSED

logger = logging.getLogger(__name__)


import unicodedata

def _norm(s):
    if not isinstance(s, str):
        return ""
    s = s.lower().strip()
    s = unicodedata.normalize("NFD", s)
    return "".join(c for c in s if unicodedata.category(c) != "Mn")

def _normalizar_tipo_sensor(descripcion: str) -> str:
    """
    Normaliza la descripción del sensor IDEAM a un nombre estándar.
    """
    s = _norm(descripcion)
    if not s:
        return "sensor_desconocido"
    
    if "humedad" in s and "suelo" in s:
        return "sensor_desconocido"
    if "temp" in s and ("max" in s or "maxima" in s):
        return "temperatura_max_c"
    if "temp" in s and ("min" in s or "minima" in s):
        return "temperatura_min_c"
    if "temp" in s:
        return "temperatura_media_c"
    if "humedad del aire" in s or "hum relativa" in s or "relativa" in s:
        return "humedad_relativa_pct"
    if "brill" in s or "solar" in s or "radia" in s:
        return "brillo_solar_horas_dia"
    if "precipit" in s:
        return "precipitacion_mm"
        
    return "sensor_desconocido"


def unificar_clima_mensual(df_precip: pd.DataFrame,
                           df_combinado: pd.DataFrame) -> pd.DataFrame:
    """
    Unifica datos de precipitación y variables combinadas.

    Corrección 2.1.a: No usa valor_agregado (obsoleto). Usa total_valor para
    precipitación y promedio_valor para temperatura/humedad.
    Corrección 2.1.b: Normalización robusta de descripcionsensor.
    Corrección 2.1.c: Propaga es_sintetico y separa archivos real/sintético.
    """
    result_dfs = []
    has_synthetic = False

    # --- Precipitación ---
    if df_precip is not None and not df_precip.empty:
        df_p = df_precip.copy()

        # Corrección 2.1.a: Usar total_valor directamente, sin fallback a valor_agregado
        if "total_valor" not in df_p.columns:
            logger.warning(
                "CLIMA: columna 'total_valor' no encontrada en datos de precipitación. "
                "Columnas disponibles: %s", df_p.columns.tolist()
            )
            # Si no existe total_valor ni promedio_valor, registrar y continuar
            if "promedio_valor" in df_p.columns:
                logger.warning("CLIMA: Usando 'promedio_valor' como proxy para precipitación (no ideal).")
                df_p["total_valor"] = df_p["promedio_valor"]

        # Verificar es_sintetico
        if "es_sintetico" in df_p.columns and df_p["es_sintetico"].any():
            has_synthetic = True

        df_p = df_p.rename(columns={
            "codigoestacion": "id_estacion",
            "total_valor": "precipitacion_mm"
        })

        df_p["anio"] = pd.to_numeric(df_p["anio"], errors="coerce").astype("Int64")
        df_p["mes"] = pd.to_numeric(df_p["mes"], errors="coerce").astype("Int64")

        if "precipitacion_mm" in df_p.columns:
            df_p["precipitacion_mm"] = pd.to_numeric(df_p["precipitacion_mm"], errors="coerce")

            # Filtro de calidad
            df_p.loc[df_p["precipitacion_mm"] < 0, "precipitacion_mm"] = 0
            df_p.loc[df_p["precipitacion_mm"] > 3000, "precipitacion_mm"] = np.nan

        # Corrección 2.1.a: Advertir si ambas columnas son NaN
        if "precipitacion_mm" in df_p.columns:
            all_nan = df_p["precipitacion_mm"].isna().sum()
            if all_nan == len(df_p):
                logger.warning(
                    "CLIMA: TODOS los registros de precipitación tienen valor NaN. "
                    "Verificar columnas total_valor/promedio_valor en el extractor."
                )

        cols_keep = ["id_estacion", "anio", "mes", "precipitacion_mm"]
        cols_keep = [c for c in cols_keep if c in df_p.columns]
        df_p = df_p[cols_keep].dropna(subset=["id_estacion", "anio", "mes"])
        result_dfs.append(df_p)
        logger.info("CLIMA: %s registros de precipitación unificados", len(df_p))

    # --- Clima combinado ---
    if df_combinado is not None and not df_combinado.empty:
        df_c = df_combinado.copy()

        # Corrección 2.1.a: Usar promedio_valor directamente para temp/humedad
        if "promedio_valor" not in df_c.columns:
            logger.warning(
                "CLIMA: columna 'promedio_valor' no encontrada en datos combinados. "
                "Columnas disponibles: %s", df_c.columns.tolist()
            )

        # Verificar es_sintetico
        if "es_sintetico" in df_c.columns and df_c["es_sintetico"].any():
            has_synthetic = True

        for col in ["promedio_valor", "max_valor", "min_valor"]:
            if col in df_c.columns:
                df_c[col] = pd.to_numeric(df_c[col], errors="coerce")

        # Corrección 2.1.b: Normalización robusta de tipo de sensor
        if "descripcionsensor" in df_c.columns:
            df_c["variable"] = df_c["descripcionsensor"].apply(_normalizar_tipo_sensor)
        else:
            df_c["variable"] = "sensor_desconocido"
            logger.warning("CLIMA: columna 'descripcionsensor' no encontrada")

        # LOG CRÍTICO: ver qué quedó sin clasificar
        no_clasificados = df_c[df_c["variable"] == "sensor_desconocido"]
        if not no_clasificados.empty and "descripcionsensor" in df_c.columns:
            logger.warning("CLIMA: sensores sin clasificar (revisar patrones): %s", 
                           no_clasificados["descripcionsensor"].value_counts().to_dict())

        # Filtrar los desconocidos (no se pueden pivotar de forma útil)
        df_c = df_c[df_c["variable"] != "sensor_desconocido"]

        if not df_c.empty:
            value_cols = [c for c in ["promedio_valor", "max_valor", "min_valor"] if c in df_c.columns]
            df_c_pivot = df_c.pivot_table(
                index=["codigoestacion", "anio", "mes"],
                columns="variable",
                values=value_cols,
                aggfunc="mean",
            ).reset_index()

            df_c_pivot.columns = ["_".join([str(p) for p in col if p]).strip("_") for col in df_c_pivot.columns]
            
            logger.info("CLIMA: columnas post-pivot: %s", df_c_pivot.columns.tolist())

            rename_cols = {
                "codigoestacion": "id_estacion",
                "promedio_valor_temperatura_media_c": "temperatura_media_c",
                "promedio_valor_temperatura_max_c": "temperatura_max_c",
                "promedio_valor_temperatura_min_c": "temperatura_min_c",
                "max_valor_temperatura_media_c": "temperatura_max_c",
                "min_valor_temperatura_media_c": "temperatura_min_c",
                "promedio_valor_humedad_relativa_pct": "humedad_relativa_pct",
                "promedio_valor_brillo_solar_horas_dia": "brillo_solar_horas_dia",
            }
            
            # Rename defensivo: solo renombrar lo que existe
            rename_cols = {k: v for k, v in rename_cols.items() if k in df_c_pivot.columns}
            df_c_pivot = df_c_pivot.rename(columns=rename_cols)

            # Asegurar que las columnas esperadas existan aunque sean NaN
            for expected in ["temperatura_media_c", "temperatura_max_c", "temperatura_min_c",
                             "humedad_relativa_pct", "brillo_solar_horas_dia"]:
                if expected not in df_c_pivot.columns:
                    logger.warning("CLIMA: columna '%s' ausente del pivot — revisar sensores IDEAM", expected)
                    df_c_pivot[expected] = np.nan

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

    metric_cols = [
        "precipitacion_mm",
        "temperatura_media_c",
        "temperatura_max_c",
        "temperatura_min_c",
        "humedad_relativa_pct",
        "brillo_solar_horas_dia",
    ]
    metric_cols = [c for c in metric_cols if c in result.columns]
    if metric_cols:
        before = len(result)
        result = result.dropna(subset=metric_cols, how="all")
        dropped = before - len(result)
        if dropped:
            logger.warning(
                "CLIMA: %s filas sin ninguna metrica util fueron descartadas",
                dropped,
            )

    # Corrección 2.1.c: Propagar es_sintetico
    result["tiene_datos_sinteticos"] = has_synthetic

    if has_synthetic:
        # Separar en dos archivos: real y sintético
        # Como el flag es a nivel de dataset (no por fila individual), guardamos
        # el archivo con la marca correspondiente
        out_synth = DATA_PROCESSED / "clima_mensual_sintetico.parquet"
        result.to_parquet(out_synth, index=False)
        logger.warning(
            "CLIMA: Los datos climáticos contienen componentes sintéticos. "
            "Guardados en %s. Usar con precaución.", out_synth
        )
    else:
        out = DATA_PROCESSED / "clima_mensual_real.parquet"
        result.to_parquet(out, index=False)

    # Siempre guardar la versión unificada para compatibilidad downstream
    out_unified = DATA_PROCESSED / "clima_mensual.parquet"
    result.to_parquet(out_unified, index=False)

    return result
