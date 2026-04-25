import pandas as pd
import logging
from pathlib import Path
from config.settings import DATA_RAW, DATA_PROCESSED

logger = logging.getLogger(__name__)

def clean_insumos_ipia() -> pd.DataFrame:
    """Limpia el Índice de Precios de Insumos Agrícolas (IPIA)."""
    raw_path = DATA_RAW / "insumos_ipia_raw.csv"
    if not raw_path.exists():
        logger.warning(f"No existe el archivo raw: {raw_path}")
        return pd.DataFrame()
        
    df = pd.read_csv(raw_path)
    if df.empty:
        return df

    # 1. Convertir fecha
    df["fecha"] = pd.to_datetime(df["fecha"]).dt.normalize()
    
    # 2. Identificar columnas de índices (excluyendo fecha)
    index_cols = [c for c in df.columns if c != "fecha"]
    
    # 3. Transformar a formato largo (Tidy Data)
    # Queremos: fecha, nombre_insumo, precio_cop_unidad
    df_long = df.melt(id_vars=["fecha"], value_vars=index_cols, 
                      var_name="nombre_insumo", value_name="precio_cop_unidad")
    
    # 4. Clasificar tipo_insumo basado en el nombre de la columna original
    def categorizar(nombre):
        nombre = nombre.lower()
        if "fertilizante" in nombre or "urea" in nombre or "dap" in nombre or "kcl" in nombre or "sam" in nombre or "_" in nombre:
            # Los que empiezan con _ suelen ser fertilizantes compuestos como _15_15_15
            if nombre.startswith("_") or any(x in nombre for x in ["fertilizante", "urea", "dap", "kcl", "sam"]):
                return "Fertilizante"
        if "plaguicida" in nombre or "herbicida" in nombre or "fungicida" in nombre or "insecticida" in nombre:
            return "Plaguicida"
        return "Otros"

    df_long["tipo_insumo"] = df_long["nombre_insumo"].apply(categorizar)
    
    # Limpiar nombres para visualización
    df_long["nombre_insumo"] = df_long["nombre_insumo"].str.replace("_", " ").str.strip().str.title()
    
    # Asegurar unidad de medida (IPIA es un índice, base 100, pero lo tratamos como valor)
    df_long["unidad_medida"] = "Indice (Base 100)"
    
    # Guardar procesado
    out_path = DATA_PROCESSED / "insumos_clean.parquet"
    df_long.to_parquet(out_path, index=False)
    logger.info(f"Insumos IPIA limpios: {len(df_long)} registros -> {out_path}")
    
    return df_long

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    clean_insumos_ipia()
