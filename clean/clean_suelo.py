import logging
from pathlib import Path
import pandas as pd
from config.settings import MANUAL_DATA_DIR

logger = logging.getLogger(__name__)


def resumir_aptitud_suelo_por_municipio(
    gdf_sipra: pd.DataFrame,
    df_divipola: pd.DataFrame,
) -> pd.DataFrame:
    """
    Formatea la salida de la API REST de SIPRA (UPRA) para el esquema fact_aptitud_suelo.
    """
    if gdf_sipra.empty:
        return pd.DataFrame()

    df = gdf_sipra.copy()
    
    # Asegurar que id_municipio es string de 5 caracteres
    if "id_municipio" in df.columns:
        df["id_municipio"] = df["id_municipio"].astype(str).str.zfill(5)
        
    # Renombrar para que load_fact_aptitud_suelo lo procese
    if "cultivo_origen" in df.columns:
        df = df.rename(columns={"cultivo_origen": "producto"})
        
    # Mapeo estricto de aptitudes al CHECK de la BD
    def map_aptitud(val):
        if pd.isna(val): return None
        val_lower = str(val).lower()
        if "alta" in val_lower: return "alta"
        if "media" in val_lower: return "moderada"
        if "baja" in val_lower: return "marginal"
        if "no apta" in val_lower or "exclusion" in val_lower or "exclusión" in val_lower: return "no_apta"
        return None
        
    if "aptitud" in df.columns:
        df["clase_aptitud"] = df["aptitud"].apply(map_aptitud)

    return df


def load_censo_agropecuario_local() -> pd.DataFrame:
    base = MANUAL_DATA_DIR / "cna"
    candidates = list(base.glob("*.csv")) + list(base.glob("*.xlsx")) + list(base.glob("*.xls")) + list(base.glob("*.parquet"))
    if not candidates:
        return pd.DataFrame()

    path = candidates[0]
    logger.info("Leyendo archivo CNA manual: %s", path.name)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(path)
    elif suffix == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_excel(path)

    rename_map = {}
    lower_map = {col.lower(): col for col in df.columns}
    optional = {
        "id_municipio": ["id_municipio", "cod_mpio", "divipola"],
        "anio_censo": ["anio_censo", "año_censo", "anio"],
        "upa_promedio_ha": ["upa_promedio_ha", "upa_promedio"],
        "pct_tenencia_propia": ["pct_tenencia_propia", "tenencia_propia_pct"],
        "pct_tenencia_arrendada": ["pct_tenencia_arrendada", "tenencia_arrendada_pct"],
        "pct_acceso_riego": ["pct_acceso_riego", "acceso_riego_pct"],
        "pct_asistencia_tecnica": ["pct_asistencia_tecnica", "asistencia_tecnica_pct"],
        "area_cultivos_permanentes_ha": ["area_cultivos_permanentes_ha"],
        "area_cultivos_transitorios_ha": ["area_cultivos_transitorios_ha"],
    }
    for target, candidates in optional.items():
        for candidate in candidates:
            if candidate.lower() in lower_map:
                rename_map[lower_map[candidate.lower()]] = target
                break

    df = df.rename(columns=rename_map)
    if "id_municipio" in df.columns:
        df["id_municipio"] = df["id_municipio"].astype(str).str.zfill(5)
    return df
