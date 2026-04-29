import pandas as pd
import logging
from pathlib import Path
from config.settings import DATA_RAW, DATA_PROCESSED

logger = logging.getLogger(__name__)

def clean_insumos_ipia() -> pd.DataFrame:
    """
    Limpia el Índice de Precios de Insumos Agrícolas (IPIA).
    # FIX v1: Compatibilidad con nuevo formato Parquet y logging estandarizado.
    """
    path_parquet = DATA_RAW / "insumos" / "insumos_raw_consolidado.parquet"
    path_csv = DATA_RAW / "insumos_ipia_raw.csv"
    
    if path_parquet.exists():
        df = pd.read_parquet(path_parquet)
    elif path_csv.exists():
        df = pd.read_csv(path_csv)
    else:
        logger.warning("INSUMOS: No se encontró el archivo raw en %s", path_parquet)
        return pd.DataFrame()
        
    if df.empty:
        return df

    # 1. Convertir fecha
    df["fecha"] = pd.to_datetime(df["fecha"]).dt.normalize()
    
    # 2. Identificar columnas de índices (excluyendo fecha y metadatos)
    exclude = ["fecha", "fuente_origen", "es_sintetico", "region"]
    index_cols = [c for c in df.columns if c not in exclude]
    
    # 3. Transformar a formato largo (Tidy Data)
    df_long = df.melt(id_vars=[c for c in df.columns if c in exclude], 
                      value_vars=index_cols, 
                      var_name="nombre_insumo", value_name="precio_cop_unidad")
    
    # 4. Clasificar tipo_insumo
    def categorizar(nombre):
        nombre = str(nombre).lower()
        if any(x in nombre for x in ["fertilizante", "urea", "dap", "kcl", "sam", "_"]):
            return "Fertilizante"
        if any(x in nombre for x in ["plaguicida", "herbicida", "fungicida", "insecticida"]):
            return "Plaguicida"
        return "Otros"

    df_long["tipo_insumo"] = df_long["nombre_insumo"].apply(categorizar)
    df_long["nombre_insumo"] = df_long["nombre_insumo"].str.replace("_", " ").str.strip().str.title()
    df_long["unidad_medida"] = "Indice (Base 100)"
    
    out_path = DATA_PROCESSED / "insumos_clean.parquet"
    df_long.to_parquet(out_path, index=False)
    logger.info("INSUMOS: %s registros limpios -> %s", len(df_long), out_path)
    
    return df_long

def normalizar_insumos(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza los datos de insumos para la base de datos.
    # FIX v1: Manejo robusto de tiempo y región.
    """
    if df_raw is None or df_raw.empty:
        return pd.DataFrame()
    
    df = df_raw.copy()
    
    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    else:
        logger.warning("INSUMOS: No se encontró columna 'fecha' para normalizar")
        return pd.DataFrame()
        
    # Recuperar id_tiempo
    from load.db import get_engine
    engine = get_engine()
    if engine:
        try:
            df_tiempo = pd.read_sql("SELECT id_tiempo, anio, mes FROM dim_tiempo", engine)
            df["anio"] = df["fecha"].dt.year
            df["mes"] = df["fecha"].dt.month
            df = df.merge(df_tiempo, on=["anio", "mes"], how="inner")
            
            if "region" in df.columns:
                df_region = pd.read_sql("SELECT id_region, nombre_region FROM dim_region_natural", engine)
                df = df.merge(df_region, left_on="region", right_on="nombre_region", how="left")
            else:
                df["id_region"] = None
        except Exception as e:
            logger.error("INSUMOS: Error al cruzar con dimensiones en DB: %s", e)
            return pd.DataFrame()
    else:
        logger.warning("INSUMOS: Sin conexión a DB, omitiendo normalización completa")
        return pd.DataFrame()

    expected_cols = [
        "id_tiempo", "tipo_insumo", "nombre_insumo", "precio_cop_unidad",
        "unidad_medida", "id_region", "fuente_origen", "es_sintetico",
    ]
    for col in expected_cols:
        if col not in df.columns: df[col] = None
            
    df_out = df[expected_cols].dropna(subset=["id_tiempo", "nombre_insumo"])
    
    out_path = DATA_PROCESSED / "insumos_clean.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_parquet(out_path, index=False)
    
    return df_out

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    clean_insumos_ipia()
