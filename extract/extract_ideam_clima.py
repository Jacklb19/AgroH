"""
extract_ideam_clima.py — Descarga datos climáticos del IDEAM desde Socrata.

ESTRATEGIA ULTRA-OPTIMIZADA (V4):
  - Consulta MES POR MES.
  - No usamos funciones date_extract en el servidor (son muy lentas).
  - Agregamos el año y mes en Python después de recibir los datos.
"""
import requests
import pandas as pd
import logging
import time
from datetime import datetime
from pathlib import Path
from config.settings import SOURCES, DATA_RAW, CLIMA_YEAR_START, YEAR_END

logger = logging.getLogger(__name__)

TIMEOUT = 60  # Con la nueva lógica, debería responder en menos de 10s
MAX_RETRIES = 3

def _download_month_fast(url: str, agg_func: str, anio: int, mes: int, include_sensor: bool = False) -> pd.DataFrame:
    """Baja datos agregados de un mes de forma ultra-rápida."""
    rows = []
    offset = 0
    limit = 50000
    
    # Filtro de fecha
    next_mes = mes + 1 if mes < 12 else 1
    next_anio = anio if mes < 12 else anio + 1
    where = (
        f"fechaobservacion >= '{anio}-{mes:02d}-01T00:00:00' "
        f"AND fechaobservacion < '{next_anio}-{next_mes:02d}-01T00:00:00'"
    )

    # Solo agrupamos por lo estrictamente necesario
    if include_sensor:
        select = f"codigoestacion, descripcionsensor, {agg_func}(valorobservado) as valor_agregado, count(*) as num_lecturas"
        group = "codigoestacion, descripcionsensor"
    else:
        select = f"codigoestacion, {agg_func}(valorobservado) as valor_agregado, count(*) as num_lecturas"
        group = "codigoestacion"

    while True:
        params = {
            "$select": select,
            "$group": group,
            "$where": where,
            "$limit": limit,
            "$offset": offset
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                r = requests.get(url, params=params, timeout=TIMEOUT)
                r.raise_for_status()
                batch = r.json()
                break
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(5)
                else:
                    logger.error(f"      Error tras {MAX_RETRIES} intentos: {e}")
                    return pd.DataFrame(rows)
        
        if not batch: break
        rows.extend(batch)
        if len(batch) < limit: break
        offset += limit

    df = pd.DataFrame(rows)
    if not df.empty:
        # Añadimos el año y mes en Python para ahorrarle trabajo al servidor
        df["anio"] = anio
        df["mes"] = mes
        # Normalizamos nombres para clean_clima.py
        if "total_valor" not in df.columns:
            df["total_valor"] = df["valor_agregado"]
            df["promedio_valor"] = df["valor_agregado"]
            
    return df

def extract_precipitacion_mensual() -> pd.DataFrame:
    """Precipitación mes por mes."""
    url = SOURCES["precipitacion_ideam"]
    out_dir = DATA_RAW / "clima"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "precipitacion_mensual_total.parquet"

    logger.info("Descargando precipitación IDEAM (Optimización V4)...")
    all_dfs = []
    for anio in range(CLIMA_YEAR_START, YEAR_END + 1):
        for mes in range(1, 13):
            if anio == datetime.now().year and mes > datetime.now().month: break
            
            cache = out_dir / f"precip_v4_{anio}_{mes:02d}.parquet"
            if cache.exists():
                df_m = pd.read_parquet(cache)
            else:
                df_m = _download_month_fast(url, "sum", anio, mes, include_sensor=False)
                if not df_m.empty:
                    df_m.to_parquet(cache, index=False)
                    logger.info(f"  {anio}-{mes:02d}: {len(df_m)} estaciones con datos")
            if not df_m.empty: all_dfs.append(df_m)

    if all_dfs:
        df_all = pd.concat(all_dfs, ignore_index=True)
        df_all.to_parquet(out_file, index=False)
        return df_all
    return pd.DataFrame()

def extract_clima_combinado_mensual() -> pd.DataFrame:
    """Clima combinado (temp, hum) mes por mes."""
    url = SOURCES["clima_combinado_ideam"]
    out_dir = DATA_RAW / "clima"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "clima_combined_mensual_total.parquet"

    logger.info("Descargando variables climáticas (Optimización V4)...")
    all_dfs = []
    for anio in range(CLIMA_YEAR_START, YEAR_END + 1):
        for mes in range(1, 13):
            if anio == datetime.now().year and mes > datetime.now().month: break
            
            cache = out_dir / f"clima_v4_{anio}_{mes:02d}.parquet"
            if cache.exists():
                df_m = pd.read_parquet(cache)
            else:
                df_m = _download_month_fast(url, "avg", anio, mes, include_sensor=True)
                if not df_m.empty:
                    df_m.to_parquet(cache, index=False)
                    logger.info(f"  {anio}-{mes:02d}: {len(df_m)} variables/estaciones")
            if not df_m.empty: all_dfs.append(df_m)

    if all_dfs:
        df_all = pd.concat(all_dfs, ignore_index=True)
        df_all.to_parquet(out_file, index=False)
        return df_all
    return pd.DataFrame()

def extract_all_clima() -> tuple[pd.DataFrame, pd.DataFrame]:
    return extract_precipitacion_mensual(), extract_clima_combinado_mensual()
