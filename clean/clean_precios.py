import logging
import pandas as pd
from clean.clean_municipios import agregar_id_municipio

logger = logging.getLogger(__name__)

def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Busca una columna entre candidatos (case-insensitive)."""
    lower_map = {col.lower(): col for col in df.columns}
    for candidate in candidates:
        if candidate.lower() in lower_map:
            return lower_map[candidate.lower()]
    return None

def normalizar_precios_sipsa(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Homologa microdatos SIPSA a la forma mensual esperada por el loader.
    # FIX v1: Mapeo robusto de columnas y logging estandarizado.
    """
    if df_raw is None or df_raw.empty:
        return pd.DataFrame()

    df = df_raw.copy()
    
    # Mapeo de columnas con candidatos flexibles
    cols_map = {
        "fecha_registro": _find_column(df, ["fecha", "fecha_registro", "fecha del precio"]),
        "producto":       _find_column(df, ["producto", "articulo", "nombre_producto"]),
        "nombre_central": _find_column(df, ["central", "central_abastos", "plaza"]),
        "ciudad":         _find_column(df, ["ciudad", "municipio", "ciudad_central"]),
        "precio_min":     _find_column(df, ["precio_min", "precio_min_cop_kg", "precio minimo"]),
        "precio_max":     _find_column(df, ["precio_max", "precio_max_cop_kg", "precio maximo"]),
        "precio_prom":    _find_column(df, ["precio_promedio", "precio_promedio_cop_kg", "precio"]),
        "volumen":        _find_column(df, ["volumen", "volumen_ton", "cantidad"]),
    }

    # Verificar columnas obligatorias
    required = ["fecha_registro", "producto", "nombre_central", "ciudad", "precio_prom"]
    missing = [k for k in required if cols_map[k] is None]
    if missing:
        logger.warning("SIPSA: Faltan columnas críticas para normalizar: %s", missing)
        return pd.DataFrame()

    # Renombrar y seleccionar
    rename_dict = {v: k for k, v in cols_map.items() if v is not None}
    df = df.rename(columns=rename_dict)
    
    # Normalización de tipos
    df["fecha_registro"] = pd.to_datetime(df["fecha_registro"], errors="coerce")
    df = df.dropna(subset=["fecha_registro", "producto", "nombre_central", "ciudad"])
    
    for col in ["producto", "nombre_central", "ciudad"]:
        df[col] = df[col].astype(str).str.replace(r"\s+", " ", regex=True).str.strip().str.upper()

    df["anio"] = df["fecha_registro"].dt.year
    df["mes"] = df["fecha_registro"].dt.month

    # Asegurar columnas numéricas
    num_cols = ["precio_min", "precio_max", "precio_prom", "volumen"]
    for col in num_cols:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # Agregar ID Municipio (DIVIPOLA)
    df["municipio_match"] = df["ciudad"]
    df = agregar_id_municipio(df, "municipio_match")

    # Agregación mensual
    aggregated = (
        df.groupby(["anio", "mes", "producto", "nombre_central", "ciudad", "id_municipio"], dropna=False)
        .agg({
            "precio_min":  "mean",
            "precio_max":  "mean",
            "precio_prom": "mean",
            "volumen":     "sum",
        })
        .reset_index()
    )
    
    # Renombrar a nombres del schema final
    aggregated = aggregated.rename(columns={
        "precio_min":  "precio_min_cop_kg",
        "precio_max":  "precio_max_cop_kg",
        "precio_prom": "precio_promedio_cop_kg",
        "volumen":     "volumen_abastecimiento_ton"
    })

    logger.info("SIPSA: %s registros mensuales normalizados", len(aggregated))
    return aggregated

def construir_dim_centrales(df_precios: pd.DataFrame) -> pd.DataFrame:
    """Extrae la dimensión única de centrales de abastos."""
    if df_precios.empty:
        return pd.DataFrame()
    return (
        df_precios[["nombre_central", "ciudad", "id_municipio"]]
        .dropna(subset=["nombre_central", "ciudad"])
        .drop_duplicates()
    )
