"""
extract_ideam_clima.py — Descarga datos climáticos del IDEAM desde Socrata.

ESTRATEGIA OPTIMIZADA:
  En vez de descargar millones de lecturas cada 10 minutos y promediar en Python,
  usamos $select/$group de Socrata (SoQL) para que el SERVIDOR haga la agregación
  mensual. Esto reduce la descarga de ~5M filas/año a ~100K filas/año.
"""
import requests
import pandas as pd
import logging
import time
from config.settings import SOURCES, DATA_RAW, CLIMA_YEAR_START, YEAR_END

logger = logging.getLogger(__name__)

TIMEOUT = 120
MAX_RETRIES = 3


def _download_aggregated(
    url: str,
    variable_name: str,
    anio_start: int,
    anio_end: int,
    include_sensor: bool = True,
    limit: int = 50000,
) -> pd.DataFrame:
    """
    Descarga datos ya agregados por mes desde Socrata usando SoQL.
    
    Args:
        url: endpoint Socrata
        variable_name: nombre descriptivo ('precipitacion', 'temperatura')
        anio_start: año inicial
        anio_end: año final (inclusive)
        include_sensor: agrega descripcionsensor al resultado si existe en la fuente
        limit: registros por página
    """
    rows = []
    offset = 0

    select_cols = ["codigoestacion"]
    group_cols = ["codigoestacion"]
    if include_sensor:
        select_cols.append("descripcionsensor")
        group_cols.append("descripcionsensor")

    select_cols.extend([
        "date_extract_y(fechaobservacion) as anio",
        "date_extract_m(fechaobservacion) as mes",
        "sum(valorobservado) as total_valor",
        "avg(valorobservado) as promedio_valor",
        "max(valorobservado) as max_valor",
        "min(valorobservado) as min_valor",
        "count(*) as num_lecturas",
    ])
    group_cols.extend([
        "date_extract_y(fechaobservacion)",
        "date_extract_m(fechaobservacion)",
    ])

    select_clause = ",".join(select_cols)
    group_clause = ",".join(group_cols)
    where_clause = (
        f"fechaobservacion >= '{anio_start}-01-01T00:00:00' "
        f"AND fechaobservacion < '{anio_end + 1}-01-01T00:00:00'"
    )

    logger.info(f"  Descargando {variable_name} agregada ({anio_start}-{anio_end})...")

    while True:
        params = {
            "$select": select_clause,
            "$group": group_clause,
            "$where": where_clause,
            "$order": "anio,mes,codigoestacion",
            "$limit": limit,
            "$offset": offset,
        }

        for attempt in range(MAX_RETRIES):
            try:
                r = requests.get(url, params=params, timeout=TIMEOUT)
                r.raise_for_status()
                batch = r.json()
                break
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                logger.warning(f"  Intento {attempt + 1}/{MAX_RETRIES}: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(5 * (attempt + 1))
                else:
                    logger.error(f"  Falló definitivamente para {variable_name}")
                    return pd.DataFrame(rows)
            except requests.exceptions.HTTPError:
                if r.status_code == 400:
                    logger.warning(f"  HTTP 400 — posiblemente el dataset no soporta SoQL. Respuesta: {r.text[:200]}")
                    return pd.DataFrame(rows)
                raise

        if not batch:
            break

        rows.extend(batch)
        offset += limit
        if len(rows) % 100000 == 0:
            logger.info(f"    {variable_name}: {len(rows)} registros agregados...")

    logger.info(f"  {variable_name}: {len(rows)} registros mensuales descargados")
    return pd.DataFrame(rows)


def extract_precipitacion_mensual() -> pd.DataFrame:
    """Descarga precipitación ya agregada como SUMA mensual por estación, año por año."""
    url = SOURCES["precipitacion_ideam"]
    out_dir = DATA_RAW / "clima"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "precipitacion_mensual_total.parquet"

    logger.info("Descargando precipitación IDEAM (agregación mensual en servidor)...")
    
    all_dfs = []
    for anio in range(CLIMA_YEAR_START, YEAR_END + 1):
        cache = out_dir / f"precipitacion_mensual_{anio}.parquet"
        skip_marker = out_dir / f"precipitacion_mensual_{anio}.skip"
        if cache.exists():
            logger.info(f"  {anio}: usando caché local ({cache})")
            df_year = pd.read_parquet(cache)
        elif skip_marker.exists():
            logger.info(f"  {anio}: omitido (falló en run anterior — borra {skip_marker.name} para reintentar)")
            continue
        else:
            logger.info(f"  {anio}: consultando agregados en API...")
            df_year = _download_aggregated(
                url,
                f"precipitacion_{anio}",
                anio,
                anio,
                include_sensor=False,
            )
            if not df_year.empty:
                df_year.to_parquet(cache, index=False)
                logger.info(f"  {anio}: {len(df_year)} registros agregados -> {cache}")
            else:
                skip_marker.touch()
                logger.warning(f"  {anio}: sin datos — marcado para omitir en próximos runs")
                continue

        if not df_year.empty:
            all_dfs.append(df_year)

    if all_dfs:
        df_all = pd.concat(all_dfs, ignore_index=True)
        df_all.to_parquet(out_file, index=False)
        return df_all
    
    return pd.DataFrame()


def extract_clima_combinado_mensual() -> pd.DataFrame:
    """Descarga variables climáticas combinadas como PROMEDIO mensual por estación, año por año."""
    url = SOURCES["clima_combinado_ideam"]
    out_dir = DATA_RAW / "clima"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "clima_combinado_mensual_total.parquet"

    logger.info("Descargando variables climáticas combinadas IDEAM (agregación mensual)...")
    
    all_dfs = []
    for anio in range(CLIMA_YEAR_START, YEAR_END + 1):
        cache = out_dir / f"clima_combinado_mensual_{anio}.parquet"
        skip_marker = out_dir / f"clima_combinado_mensual_{anio}.skip"
        if cache.exists():
            logger.info(f"  {anio}: usando caché local ({cache})")
            df_year = pd.read_parquet(cache)
        elif skip_marker.exists():
            logger.info(f"  {anio}: omitido (borra {skip_marker.name} para reintentar)")
            continue
        else:
            logger.info(f"  {anio}: consultando agregados en API...")
            df_year = _download_aggregated(
                url,
                f"clima_combinado_{anio}",
                anio,
                anio,
                include_sensor=True,
            )
            if not df_year.empty:
                df_year.to_parquet(cache, index=False)
                logger.info(f"  {anio}: {len(df_year)} registros agregados -> {cache}")
            else:
                skip_marker.touch()
                logger.info(f"  {anio}: sin datos del endpoint — marcado para omitir")
                continue

        if not df_year.empty:
            all_dfs.append(df_year)

    if all_dfs:
        df_all = pd.concat(all_dfs, ignore_index=True)
        df_all.to_parquet(out_file, index=False)
        return df_all
    
    return pd.DataFrame()


def extract_all_clima() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Descarga ambas fuentes climáticas ya agregadas. Retorna DataFrames."""
    df_precip = extract_precipitacion_mensual()
    df_combinado = extract_clima_combinado_mensual()
    return df_precip, df_combinado
