import pandas as pd
import pandera.pandas as pa
import logging
from pathlib import Path
from rich.console import Console
from rich.table import Table

from clean.quality_checks import (
    normalizar_texto, 
    parse_numeric_columns,
    parse_datetime_columns,
    categorize_error,
    log_data_quality
)
from clean.validation_contracts_sipsa import sipsa_schema
from config.settings import DATA_PROCESSED

logger = logging.getLogger(__name__)
console = Console()

def prepare_raw_sipsa(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()
    
    if "precio_promedio_cop_kg" in df.columns:
        df = parse_numeric_columns(df, ["precio_promedio_cop_kg"])
        
    if "fecha_registro" in df.columns:
        df = parse_datetime_columns(df, ["fecha_registro"])

    if "producto" in df.columns:
        df["producto"] = df["producto"].apply(normalizar_texto)
        
    if "central" in df.columns:
        df["central"] = df["central"].apply(normalizar_texto)

    if "ciudad" in df.columns:
        df["ciudad"] = df["ciudad"].apply(normalizar_texto)

    return df

def clean_and_validate_sipsa(df_raw: pd.DataFrame, engine=None):
    console.rule("[bold blue]Iniciando Pipeline de Calidad: SIPSA (Precios)")
    
    df_prepared = prepare_raw_sipsa(df_raw)
    df_valid = pd.DataFrame()
    df_invalid = pd.DataFrame()
    
    error_counts_by_rule = {}
    error_counts_by_category = {"parse_error": 0, "schema_error": 0, "business_rule_error": 0, "unknown_error": 0}

    try:
        df_valid = sipsa_schema.validate(df_prepared, lazy=True)
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
        log_data_quality(engine, "sipsa_mayoristas", total, valid_count, invalid_count, error_counts_by_category)

    table = Table(title="Resultados de Calidad de Datos (SIPSA)")
    table.add_column("Métrica", style="cyan")
    table.add_column("Cantidad", justify="right", style="magenta")
    
    table.add_row("Total Registros Raw", str(total))
    table.add_row("Registros Válidos", str(valid_count))
    table.add_row("Registros Inválidos", str(invalid_count))
    console.print(table)
    
    invalid_dir = DATA_PROCESSED / "invalid"
    invalid_dir.mkdir(parents=True, exist_ok=True)
    
    if not df_invalid.empty:
        invalid_path = invalid_dir / "sipsa_invalid.csv"
        df_invalid.to_csv(invalid_path, index=False)
        console.print(f"\n[bold yellow][OK] {invalid_count} registros inválidos guardados en: {invalid_path}")
        
    return df_valid, df_invalid
