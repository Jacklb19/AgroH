import pandas as pd
import unicodedata
import logging

logger = logging.getLogger(__name__)

def categorize_error(check_name: str) -> str:
    """Clasifica los errores generados por Pandera según su origen."""
    if pd.isna(check_name):
        return "unknown_error"
    if "business_rule" in str(check_name):
        return "business_rule_error"
    if str(check_name) == "not_nullable" or "dtype" in str(check_name):
        return "parse_error"
    return "schema_error"

def normalizar_texto(s: str) -> str:
    """
    Convierte a minúsculas, remueve tildes, espacios extra
    y caracteres especiales innecesarios.
    """
    if not isinstance(s, str) or pd.isna(s):
        return ""
    s = s.lower().strip()
    s = s.split("(")[0].strip() # Quita contenido entre paréntesis como '(Santander)'
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s

def normalizar_codigo_divipola(s: str) -> str:
    """
    Asegura que el código DIVIPOLA tenga 5 caracteres (relleno de ceros a la izquierda).
    Si es nulo o inválido, devuelve None.
    """
    if pd.isna(s):
        return None
    s_str = str(s).strip()
    # Limpiar flotantes si vienen como '1234.0'
    if s_str.endswith(".0"):
        s_str = s_str[:-2]
    if not s_str or not s_str.isdigit():
        return None
    return s_str.zfill(5)

def parse_numeric_columns(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    Aplica pd.to_numeric con coerce a una lista de columnas para forzar tipos.
    Los errores de conversión se vuelven NaN explícitos.
    """
    df_out = df.copy()
    for col in columns:
        if col in df_out.columns:
            df_out[col] = pd.to_numeric(df_out[col], errors='coerce')
    return df_out

def parse_datetime_columns(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    Aplica pd.to_datetime con coerce a una lista de columnas para forzar fechas.
    Los errores se vuelven NaT explícitos.
    """
    df_out = df.copy()
    for col in columns:
        if col in df_out.columns:
            df_out[col] = pd.to_datetime(df_out[col], errors='coerce')
    return df_out

def log_data_quality(engine, fuente: str, total_raw: int, total_validos: int, total_invalidos: int, error_counts_dict: dict):
    """
    Registra el resultado de la validación de calidad en PostgreSQL usando SQLAlchemy puro
    para inyectar un JSONB con los contadores de errores.
    """
    import json
    from sqlalchemy import text
    
    if engine is None:
        logger.warning(f"No hay conexión a BD para registrar data_quality_log de {fuente}")
        return
        
    error_summary_json = json.dumps(error_counts_dict)
    
    query = text("""
        INSERT INTO data_quality_log (fuente, total_raw, total_validos, total_invalidos, error_summary_json)
        VALUES (:fuente, :raw, :validos, :invalidos, :err_json)
    """)
    
    try:
        with engine.begin() as conn:
            conn.execute(query, {
                "fuente": fuente,
                "raw": total_raw,
                "validos": total_validos,
                "invalidos": total_invalidos,
                "err_json": error_summary_json
            })
        logger.info(f"Log de calidad registrado para {fuente} (Raw: {total_raw}, Inválidos: {total_invalidos})")
    except Exception as e:
        logger.error(f"Fallo al insertar log de calidad para {fuente}: {e}")

