"""
clean_clima.py — Limpieza y unificación de datos climáticos IDEAM.

Recibe DataFrames ya agregados a nivel mensual desde el extractor
(que usa SoQL del servidor Socrata) y los unifica en un solo DataFrame
alineado al schema de fact_clima_mensual.
"""
import pandas as pd
import numpy as np
import logging
import pandera.pandas as pa
from rich.console import Console
from rich.table import Table
from clean.validation_contracts_clima import clima_schema
from clean.quality_checks import (
    normalizar_texto, 
    parse_numeric_columns,
    parse_datetime_columns,
    categorize_error,
    log_data_quality
)
from config.settings import DATA_PROCESSED, DATA_RAW

logger = logging.getLogger(__name__)
console = Console()


def unificar_clima_mensual(df_precip: pd.DataFrame,
                           df_combinado: pd.DataFrame) -> pd.DataFrame:
    """
    Unifica datos de precipitación y variables combinadas en un solo DataFrame
    con las columnas del schema de fact_clima_mensual.

    Ambos DataFrames vienen del extractor con columnas explícitas:
      codigoestacion, anio, mes, total_valor, promedio_valor,
      max_valor, min_valor, num_lecturas
    """
    result_dfs = []

    # --- Precipitación: ya viene como SUMA mensual ---
    if not df_precip.empty:
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
        
        # Filtro de calidad: Precipitación en rango [0, 3000] mm/mes
        df_p.loc[df_p["precipitacion_mm"] < 0, "precipitacion_mm"] = 0
        df_p.loc[df_p["precipitacion_mm"] > 3000, "precipitacion_mm"] = np.nan
        
        df_p = df_p[["id_estacion", "anio", "mes", "precipitacion_mm"]].dropna(
            subset=["id_estacion", "anio", "mes"]
        )
        result_dfs.append(df_p)
        logger.info(f"Precipitación mensual: {len(df_p)} registros")

    # --- Clima combinado: ya viene como PROMEDIO mensual ---
    # Este dataset mezcla variables (temperatura, humedad, etc.) diferenciadas por descripcionsensor.
    if not df_combinado.empty:
        df_c = df_combinado.copy()
        if "promedio_valor" not in df_c.columns and "valor_agregado" in df_c.columns:
            df_c["promedio_valor"] = df_c["valor_agregado"]
        if "max_valor" not in df_c.columns:
            df_c["max_valor"] = df_c.get("promedio_valor")
        if "min_valor" not in df_c.columns:
            df_c["min_valor"] = df_c.get("promedio_valor")
        for col in ["promedio_valor", "max_valor", "min_valor"]:
            df_c[col] = pd.to_numeric(df_c[col], errors="coerce")
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
                values=["promedio_valor", "max_valor", "min_valor"],
                aggfunc="mean",
            ).reset_index()

            df_c_pivot.columns = [
                "_".join([str(part) for part in col if part]).strip("_")
                if isinstance(col, tuple) else col
                for col in df_c_pivot.columns
            ]

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
            
            # Limpiar nulos en llaves
            df_c_pivot = df_c_pivot.dropna(subset=["id_estacion", "anio", "mes"])
            
            # Filtros de calidad: Temperatura en [-10, 50], Humedad en [0, 100]
            for t_col in ["temperatura_media_c", "temperatura_max_c", "temperatura_min_c"]:
                if t_col in df_c_pivot.columns:
                    df_c_pivot.loc[(df_c_pivot[t_col] < -10) | (df_c_pivot[t_col] > 50), t_col] = np.nan
            
            if "humedad_relativa_pct" in df_c_pivot.columns:
                df_c_pivot.loc[(df_c_pivot["humedad_relativa_pct"] < 0) | (df_c_pivot["humedad_relativa_pct"] > 100), "humedad_relativa_pct"] = np.nan
                
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
    for col in ["precipitacion_mm"]:
        if col not in result.columns:
            result[col] = None

    out = DATA_PROCESSED / "clima_mensual.parquet"
    result.to_parquet(out, index=False)
    logger.info(f"Clima mensual unificado: {len(result)} registros -> {out}")
    return result

def prepare_raw_clima(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Realiza el parseo técnico para los datos de Clima IDEAM."""
    df = df_raw.copy()
    
    # 1. Parseo Numérico
    if "valorobservado" in df.columns:
        df = parse_numeric_columns(df, ["valorobservado"])
        
    # 2. Parseo de Fechas
    if "fechaobservacion" in df.columns:
        df = parse_datetime_columns(df, ["fechaobservacion"])

    # 3. Normalización Semántica (Sensores y Nombres)
    if "descripcionsensor" in df.columns:
        # Se asegura de no tener espacios raros que rompan el cruce
        df["descripcionsensor"] = df["descripcionsensor"].astype(str).str.strip()
        
    if "unidadmedida" in df.columns:
        df["unidadmedida"] = df["unidadmedida"].astype(str).str.strip()

    return df

def clean_and_validate_clima(df_raw: pd.DataFrame, engine=None):
    """Ejecuta el pipeline completo de limpieza y validación para Clima."""
    console.rule("[bold blue]Iniciando Pipeline de Calidad: Clima IDEAM")
    
    df_prepared = prepare_raw_clima(df_raw)
    df_valid = pd.DataFrame()
    df_invalid = pd.DataFrame()
    
    error_counts_by_rule = {}
    error_counts_by_category = {"parse_error": 0, "schema_error": 0, "business_rule_error": 0, "unknown_error": 0}

    try:
        df_valid = clima_schema.validate(df_prepared, lazy=True)
        console.print("[bold green]¡Validación superada al 100%! No hay registros inválidos.")
    except pa.errors.SchemaErrors as err:
        failure_cases = err.failure_cases.copy()
        failure_cases = failure_cases.drop_duplicates(subset=["index", "check"])
        
        grouped_errors = failure_cases.groupby("index")["check"].apply(lambda x: " | ".join(x.astype(str)))
        df_invalid_indices = grouped_errors.index
        
        df_invalid = df_prepared.loc[df_invalid_indices].copy()
        df_invalid["error_summary"] = grouped_errors
        
        failure_cases["category"] = failure_cases["check"].apply(categorize_error)
        error_counts_by_rule = failure_cases["check"].value_counts().to_dict()
        
        cat_counts = failure_cases["category"].value_counts().to_dict()
        for cat, cnt in cat_counts.items():
            error_counts_by_category[cat] += cnt

        valid_indices = df_prepared.index.difference(df_invalid_indices)
        df_valid = df_prepared.loc[valid_indices].copy()

    total = len(df_raw)
    valid_count = len(df_valid)
    invalid_count = len(df_invalid)
    
    if engine is not None:
        log_data_quality(engine, "clima_ideam", total, valid_count, invalid_count, error_counts_by_category)
        
    table = Table(title="Resultados de Calidad de Datos (Clima)")
    table.add_column("Métrica", style="cyan")
    table.add_column("Cantidad", justify="right", style="magenta")
    
    table.add_row("Total Registros Raw", str(total))
    table.add_row("Registros Válidos", str(valid_count))
    table.add_row("Registros Inválidos", str(invalid_count))
    console.print(table)
    
    invalid_dir = DATA_PROCESSED / "invalid"
    invalid_dir.mkdir(parents=True, exist_ok=True)
    
    if not df_invalid.empty:
        invalid_path = invalid_dir / "clima_invalid.csv"
        df_invalid.to_csv(invalid_path, index=False)
        console.print(f"\n[bold yellow][OK] {invalid_count} registros inválidos guardados en: {invalid_path}")
        
    return df_valid, df_invalid
