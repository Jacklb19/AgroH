import pandas as pd
import requests
import logging
from pathlib import Path
from config.settings import DATA_RAW

logger = logging.getLogger(__name__)

# ID del dataset de Índice de Precios de Insumos Agrícolas (UPRA)
DATASET_ID = "gwbi-fnzs"
URL = f"https://www.datos.gov.co/resource/{DATASET_ID}.json"

def extract_insumos_ipia(limit: int = 5000) -> pd.DataFrame:
    """Extrae el Índice de Precios de Insumos Agrícolas (IPIA) desde datos.gov.co."""
    logger.info(f"Extrayendo IPIA desde {URL}...")
    try:
        # Socrata permite un límite alto, pero paginamos si es necesario
        params = {"$limit": limit}
        response = requests.get(URL, params=params, timeout=60)
        response.raise_for_status()
        
        data = response.json()
        if not data:
            logger.warning("No se obtuvieron datos del IPIA")
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        
        # Guardar crudo
        out_path = DATA_RAW / "insumos_ipia_raw.csv"
        df.to_csv(out_path, index=False)
        logger.info(f"IPIA: {len(df)} registros guardados en {out_path}")
        
        return df
    except Exception as e:
        logger.error(f"Error al extraer IPIA: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    extract_insumos_ipia()
