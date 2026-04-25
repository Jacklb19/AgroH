import pandas as pd
import logging
from pathlib import Path
from config.settings import DATA_RAW, DATA_PROCESSED

logger = logging.getLogger(__name__)

def clean_boletines_enso() -> pd.DataFrame:
    """Limpia los datos extraídos de los boletines agroclimáticos (ENSO)."""
    raw_path = DATA_RAW / "enso_boletines_raw.csv"
    if not raw_path.exists():
        logger.warning(f"No existe el archivo raw: {raw_path}")
        return pd.DataFrame()
        
    df = pd.read_csv(raw_path)
    if df.empty:
        return df

    # 1. Asegurar tipos de datos
    df["anio"] = pd.to_numeric(df["anio"], errors="coerce")
    df["indice_spi"] = pd.to_numeric(df["indice_spi"], errors="coerce")
    
    # 2. Filtrar registros inválidos
    df = df.dropna(subset=["anio", "region"])
    df = df[df["anio"] > 0]
    
    # 3. Normalizar fase_enso
    # El extractor ya lo hace, pero aseguramos
    fases_validas = ["El Niño", "La Niña", "Neutro"]
    df.loc[~df["fase_enso"].isin(fases_validas), "fase_enso"] = "Neutro"
    
    # 4. Crear fecha de referencia (primer día del trimestre/mes)
    # T1 -> 01-01, T2 -> 04-01, T3 -> 07-01, T4 -> 10-01
    def get_month(t):
        if t == "T1": return 1
        if t == "T2": return 4
        if t == "T3": return 7
        if t == "T4": return 10
        return 1
        
    df["mes"] = df["trimestre"].apply(get_month)
    # Pandas requiere nombres específicos para el ensamblado de fechas
    df["fecha"] = pd.to_datetime(df.assign(day=1).rename(columns={"anio": "year", "mes": "month"})[["year", "month", "day"]])
    
    # Guardar procesado
    out_path = DATA_PROCESSED / "enso_clean.parquet"
    df.to_parquet(out_path, index=False)
    logger.info(f"Boletines ENSO limpios: {len(df)} registros -> {out_path}")
    
    return df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    clean_boletines_enso()
