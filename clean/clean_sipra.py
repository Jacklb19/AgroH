import pandas as pd
import pandera.pandas as pa
import logging
from pathlib import Path
from rich.console import Console
from rich.table import Table

from clean.quality_checks import (
    normalizar_texto, 
    normalizar_codigo_divipola,
    categorize_error,
    log_data_quality
)
from clean.validation_contracts_sipra import sipra_schema
from config.settings import DATA_PROCESSED

logger = logging.getLogger(__name__)
console = Console()

def map_aptitud(val: str) -> str:
    """Normaliza la clase de aptitud al dominio de BD."""
    if not isinstance(val, str) or pd.isna(val): 
        return None
    v = val.lower()
    if "alta" in v: return "alta"
    if "media" in v or "moderada" in v: return "moderada"
    if "baja" in v or "marginal" in v: return "marginal"
    if "no apta" in v or "exclusion" in v or "exclusión" in v: return "no_apta"
    return "no_apta"

def prepare_raw_sipra(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()
    
    if "id_municipio" in df.columns:
        df["id_municipio"] = df["id_municipio"].apply(normalizar_codigo_divipola)
        
    if "aptitud" in df.columns:
        df["clase_aptitud"] = df["aptitud"].apply(map_aptitud)

    if "cultivo_origen" in df.columns:
        df["cultivo_origen"] = df["cultivo_origen"].apply(normalizar_texto)

    return df

def clean_and_validate_sipra(df_raw: pd.DataFrame, engine=None):
    console.rule("[bold blue]Iniciando Pipeline de Calidad: SIPRA (Aptitud Suelo)")
    
    df_prepared = prepare_raw_sipra(df_raw)
    df_valid = pd.DataFrame()
    df_invalid = pd.DataFrame()
    
    error_counts_by_rule = {}
    error_counts_by_category = {"parse_error": 0, "schema_error": 0, "business_rule_error": 0, "unknown_error": 0}

    try:
        df_valid = sipra_schema.validate(df_prepared, lazy=True)
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
        log_data_quality(engine, "sipra_aptitud", total, valid_count, invalid_count, error_counts_by_category)

    table = Table(title="Resultados de Calidad de Datos (SIPRA)")
    table.add_column("Métrica", style="cyan")
    table.add_column("Cantidad", justify="right", style="magenta")
    
    table.add_row("Total Registros Raw", str(total))
    table.add_row("Registros Válidos", str(valid_count))
    table.add_row("Registros Inválidos", str(invalid_count))
    console.print(table)
    
    invalid_dir = DATA_PROCESSED / "invalid"
    invalid_dir.mkdir(parents=True, exist_ok=True)
    
    if not df_invalid.empty:
        invalid_path = invalid_dir / "sipra_invalid.csv"
        df_invalid.to_csv(invalid_path, index=False)
        console.print(f"\n[bold yellow][OK] {invalid_count} registros inválidos guardados en: {invalid_path}")
        
    return df_valid, df_invalid
