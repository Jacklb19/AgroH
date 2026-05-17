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
from clean.validation_contracts import produccion_schema
from config.settings import DATA_PROCESSED, DATA_RAW

logger = logging.getLogger(__name__)
console = Console()

def prepare_raw_produccion(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza el parseo técnico y la normalización base antes de validar semánticamente.
    """
    df = df_raw.copy()
    
    # Mapeo de columnas
    rename_map = {
        "c_digo_dane_municipio": "id_municipio_raw",
        "a_o": "anio_raw",
        "rea_sembrada": "area_sembrada_ha",
        "rea_cosechada": "area_cosechada_ha",
        "producci_n": "produccion_total_ton",
        "rendimiento": "rendimiento_t_ha",
        "grupo_cultivo": "grupo_de_cultivo",
        "ciclo_del_cultivo": "ciclo_de_cultivo"
    }
    df = df.rename(columns=rename_map)

    if "id_municipio_raw" in df.columns:
        df["id_municipio"] = df["id_municipio_raw"].apply(normalizar_codigo_divipola)
    
    num_cols = ["anio_raw", "area_sembrada_ha", "area_cosechada_ha", "produccion_total_ton", "rendimiento_t_ha"]
    df = parse_numeric_columns(df, num_cols)
    
    if "anio_raw" in df.columns:
        df["anio"] = df["anio_raw"].fillna(-1).astype(int)

    if "cultivo" in df.columns:
        df["cultivo_normalizado"] = df["cultivo"].apply(normalizar_texto)

    return df

def clean_and_validate_produccion(df_raw: pd.DataFrame, engine=None):
    """
    Ejecuta el pipeline completo de limpieza y validación para Producción Agrícola.
    """
    console.rule("[bold blue]Iniciando Pipeline de Calidad: Producción Agrícola")
    
    df_prepared = prepare_raw_produccion(df_raw)
    df_valid = pd.DataFrame()
    df_invalid = pd.DataFrame()
    
    # Contadores para el reporte
    error_counts_by_rule = {}
    error_counts_by_category = {"parse_error": 0, "schema_error": 0, "business_rule_error": 0, "unknown_error": 0}

    try:
        df_valid = produccion_schema.validate(df_prepared, lazy=True)
        console.print("[bold green]¡Validación superada al 100%! No hay registros inválidos.")
    except pa.errors.SchemaErrors as err:
        failure_cases = err.failure_cases.copy()
        
        # Pandera duplicate cross-checks for each column involved, so we drop duplicates per row/check
        failure_cases = failure_cases.drop_duplicates(subset=["index", "check"])
        
        # 1. Agrupar por fila para construir el error_summary
        grouped_errors = failure_cases.groupby("index")["check"].apply(lambda x: " | ".join(x.astype(str)))
        df_invalid_indices = grouped_errors.index
        
        # 2. Extraer df_invalid y añadir el summary
        df_invalid = df_prepared.loc[df_invalid_indices].copy()
        df_invalid["error_summary"] = grouped_errors
        
        # 3. Categorizar errores para el reporte
        failure_cases["category"] = failure_cases["check"].apply(categorize_error)
        
        # Conteo por regla exacta
        error_counts_by_rule = failure_cases["check"].value_counts().to_dict()
        
        # Conteo por categoría
        cat_counts = failure_cases["category"].value_counts().to_dict()
        for cat, cnt in cat_counts.items():
            error_counts_by_category[cat] += cnt

        # 4. Extraer df_valid
        valid_indices = df_prepared.index.difference(df_invalid_indices)
        df_valid = df_prepared.loc[valid_indices].copy()

    total = len(df_raw)
    valid_count = len(df_valid)
    invalid_count = len(df_invalid)
    
    if engine is not None:
        log_data_quality(engine, "produccion_agricola", total, valid_count, invalid_count, error_counts_by_category)
        
    # ── Reporte General ──
    table = Table(title="Resultados de Calidad de Datos (Filas)")
    table.add_column("Métrica", style="cyan")
    table.add_column("Cantidad", justify="right", style="magenta")
    table.add_column("Porcentaje", justify="right", style="green")
    
    table.add_row("Total Registros Raw", str(total), "100.0%")
    table.add_row("Registros Válidos", str(valid_count), f"{(valid_count/total)*100:.1f}%" if total else "0%")
    table.add_row("Registros Inválidos", str(invalid_count), f"{(invalid_count/total)*100:.1f}%" if total else "0%")
    console.print(table)
    
    # ── Reporte por Categoría ──
    if invalid_count > 0:
        cat_table = Table(title="Ocurrencias por Categoría Semántica")
        cat_table.add_column("Categoría de Error", style="yellow")
        cat_table.add_column("Frecuencia", justify="right", style="red")
        
        for cat, cnt in error_counts_by_category.items():
            if cnt > 0:
                cat_table.add_row(cat, str(cnt))
        console.print(cat_table)
    
    # ── Reporte por Regla Exacta ──
    if error_counts_by_rule:
        err_table = Table(title="Detalle Específico de Reglas Infringidas")
        err_table.add_column("Regla (Check)", style="red")
        err_table.add_column("Frecuencia", justify="right")
        for check, count in error_counts_by_rule.items():
            err_table.add_row(str(check), str(count))
        console.print(err_table)

    # ── Exportación ──
    invalid_dir = DATA_PROCESSED / "invalid"
    invalid_dir.mkdir(parents=True, exist_ok=True)
    
    if not df_invalid.empty:
        invalid_path = invalid_dir / "produccion_agricola_invalid.csv"
        df_invalid.to_csv(invalid_path, index=False)
        console.print(f"\n[bold yellow][OK] {invalid_count} registros inválidos guardados en: {invalid_path}")
        
    return df_valid, df_invalid

if __name__ == "__main__":
    import os
    # Suprimir el warning de importación de pandera
    os.environ["DISABLE_PANDERA_IMPORT_WARNING"] = "True"
    
    raw_path = DATA_RAW / "produccion" / "produccion_agricola_raw.csv"
    
    if raw_path.exists():
        console.print(f"Leyendo archivo crudo desde {raw_path}...")
        df_sample = pd.read_csv(raw_path, dtype=str)
    else:
        console.print("[bold yellow]Archivo raw no encontrado. Generando datos dummy para test...")
        df_sample = pd.DataFrame({
            "c_digo_dane_municipio": ["11001", "5001", "abc", "15001", None],
            "a_o": ["2020", "2021", "2000", "2023", "2022"],
            "rea_sembrada": ["100", "50", "200", "0", "-10"],
            "rea_cosechada": ["90", "60", "200", "10", "10"],
            "producci_n": ["150", "100", "300", "0", "0"],
            "rendimiento": ["1.5", "1.6", "1.5", "0", "0"],
            "cultivo": ["Arroz", "Papa", "Maiz", "M", "Yuca"]
        })

    df_val, df_inv = clean_and_validate_produccion(df_sample)
