import pandas as pd
import pandera.pandas as pa
import logging
from pathlib import Path
from rich.console import Console
from rich.table import Table

from clean.quality_checks import (
    normalizar_texto, 
    normalizar_codigo_divipola, 
    parse_numeric_columns,
    categorize_error,
    log_data_quality
)
from clean.validation_contracts_divipola import divipola_schema
from config.settings import DATA_PROCESSED, DATA_RAW

logger = logging.getLogger(__name__)
console = Console()

def prepare_raw_divipola(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Realiza el parseo técnico y la normalización base para DIVIPOLA."""
    df = df_raw.copy()
    
    # 1. Normalización de Códigos
    if "cod_dpto" in df.columns:
        df["cod_dpto"] = df["cod_dpto"].apply(lambda x: str(x).zfill(2) if pd.notna(x) else None)
        
    if "cod_mpio" in df.columns:
        df["cod_mpio"] = df["cod_mpio"].apply(normalizar_codigo_divipola)
    
    # 2. Parseo Técnico (Latitud / Longitud)
    num_cols = ["latitud", "longitud"]
    df = parse_numeric_columns(df, num_cols)

    # 3. Normalización Semántica (Nombres)
    if "nom_mpio" in df.columns:
        df["nom_mpio"] = df["nom_mpio"].apply(normalizar_texto)
    if "dpto" in df.columns:
        df["dpto"] = df["dpto"].apply(normalizar_texto)

    return df

def clean_and_validate_divipola(df_raw: pd.DataFrame, engine=None):
    """Ejecuta el pipeline completo de limpieza y validación para DIVIPOLA."""
    console.rule("[bold blue]Iniciando Pipeline de Calidad: DIVIPOLA")
    
    df_prepared = prepare_raw_divipola(df_raw)
    df_valid = pd.DataFrame()
    df_invalid = pd.DataFrame()
    
    error_counts_by_rule = {}
    error_counts_by_category = {"parse_error": 0, "schema_error": 0, "business_rule_error": 0, "unknown_error": 0}

    try:
        df_valid = divipola_schema.validate(df_prepared, lazy=True)
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
        log_data_quality(engine, "divipola", total, valid_count, invalid_count, error_counts_by_category)
        
    table = Table(title="Resultados de Calidad de Datos (DIVIPOLA)")
    table.add_column("Métrica", style="cyan")
    table.add_column("Cantidad", justify="right", style="magenta")
    table.add_column("Porcentaje", justify="right", style="green")
    
    table.add_row("Total Registros Raw", str(total), "100.0%")
    table.add_row("Registros Válidos", str(valid_count), f"{(valid_count/total)*100:.1f}%" if total else "0%")
    table.add_row("Registros Inválidos", str(invalid_count), f"{(invalid_count/total)*100:.1f}%" if total else "0%")
    console.print(table)
    
    if invalid_count > 0:
        cat_table = Table(title="Ocurrencias por Categoría Semántica")
        cat_table.add_column("Categoría de Error", style="yellow")
        cat_table.add_column("Frecuencia", justify="right", style="red")
        
        for cat, cnt in error_counts_by_category.items():
            if cnt > 0:
                cat_table.add_row(cat, str(cnt))
        console.print(cat_table)
    
    if error_counts_by_rule:
        err_table = Table(title="Detalle Específico de Reglas Infringidas")
        err_table.add_column("Regla (Check)", style="red")
        err_table.add_column("Frecuencia", justify="right")
        for check, count in error_counts_by_rule.items():
            err_table.add_row(str(check), str(count))
        console.print(err_table)

    invalid_dir = DATA_PROCESSED / "invalid"
    invalid_dir.mkdir(parents=True, exist_ok=True)
    
    if not df_invalid.empty:
        invalid_path = invalid_dir / "divipola_invalid.csv"
        df_invalid.to_csv(invalid_path, index=False)
        console.print(f"\n[bold yellow][OK] {invalid_count} registros inválidos guardados en: {invalid_path}")
        
    return df_valid, df_invalid

if __name__ == "__main__":
    import os
    os.environ["DISABLE_PANDERA_IMPORT_WARNING"] = "True"
    
    raw_path = DATA_RAW / "divipola" / "divipola_raw.csv"
    
    if raw_path.exists():
        console.print(f"Leyendo archivo crudo desde {raw_path}...")
        df_sample = pd.read_csv(raw_path, dtype=str)
    else:
        console.print("[bold yellow]Archivo raw no encontrado. Generando datos dummy para test...")
        df_sample = pd.DataFrame({
            "cod_dpto": ["11", "5", "abc", "15", None],
            "dpto": ["Bogota", "Antioquia", "A", "Boyaca", "Valle"],
            "cod_mpio": ["11001", "05001", "abcde", "15001", "1"],
            "nom_mpio": ["Bogota D.C.", "Medellin", "M", "Tunja", "Cali"],
            "tipo_municipio": ["Municipio", "Municipio", "Municipio", "Municipio", "Municipio"],
            "latitud": ["4.6", "6.2", "90.0", "5.5", "invalid"], # 90 is outside Colombia
            "longitud": ["-74.0", "-75.5", "-74.0", "-73.3", "-76.5"]
        })

    df_val, df_inv = clean_and_validate_divipola(df_sample)
