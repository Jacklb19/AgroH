"""
extract_insumos.py — Extracción de precios de insumos agrícolas (IPIA).
Actualizado para usar el nuevo recurso de la UPRA (gwbi-fnzs).
"""
import pandas as pd
import requests
import logging
from config.settings import SOURCES, DATA_RAW

logger = logging.getLogger(__name__)

_API_ENDPOINTS = [
    "https://www.datos.gov.co/resource/gwbi-fnzs.json",  # Nuevo IPIA (UPRA)
]
_LIMIT   = 50_000
_TIMEOUT = 120

def _fetch_single_endpoint(url: str) -> pd.DataFrame:
    try:
        r = requests.get(url, params={"$limit": _LIMIT}, timeout=_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if not data: return pd.DataFrame()
        df = pd.DataFrame(data)
        df["fuente_origen"] = url
        df["es_sintetico"]  = False
        return df
    except Exception as e:
        logger.error(f"Error descargando {url}: {e}")
        return pd.DataFrame()

def _fetch_api() -> pd.DataFrame:
    frames = []
    for url in _API_ENDPOINTS:
        df = _fetch_single_endpoint(url)
        if df.empty: continue
            
        if "urea_46" in df.columns and "fecha" in df.columns:
            logger.info("Insumos: detectado formato ancho de UPRA. Pivoteando...")
            meta_cols = ["fecha", "fuente_origen", "es_sintetico"]
            insumo_cols = [c for c in df.columns if c not in meta_cols and not c.startswith("total_") and c != "indice_total"]
            
            df_long = df.melt(id_vars=meta_cols, value_vars=insumo_cols, var_name="nombre_insumo", value_name="precio_cop_unidad")
            df_long["tipo_insumo"] = "fertilizante"
            df_long.loc[df_long["nombre_insumo"].isin(["glifosato", "paraquat", "propanil"]), "tipo_insumo"] = "agroquimico"
            df = df_long

        frames.append(df)

    if not frames: return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    if "fecha" in combined.columns:
        combined["fecha"] = pd.to_datetime(combined["fecha"], errors="coerce")
    return combined.drop_duplicates(subset=["fecha", "nombre_insumo", "tipo_insumo"], keep="first")

def extract_insumos() -> pd.DataFrame:
    logger.info("Iniciando extracción de insumos (IPIA)...")
    df = _fetch_api()
    if df.empty:
        logger.warning("No se obtuvieron datos de insumos reales.")
    return df