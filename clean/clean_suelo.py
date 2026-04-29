import logging
from pathlib import Path
import geopandas as gpd
import pandas as pd
from config.settings import MANUAL_DATA_DIR, DATA_RAW

logger = logging.getLogger(__name__)

_CATEGORY_COLS  = ["clase_aptitud", "aptitud", "categoria", "clase"]
_SOIL_COLS      = ["tipo_suelo", "suelo"]
_TEXTURE_COLS   = ["textura_suelo", "textura"]
_SLOPE_COLS     = ["pendiente_dominante", "pendiente"]
_DRAIN_COLS     = ["drenaje"]
_LIMIT_COLS     = ["limitante_principal", "limitante"]
_PRODUCT_COLS   = ["producto", "cultivo", "cultivo_origen", "_source_file"]

def _pick_col(obj, candidates: list) -> str | None:
    """Selecciona columna entre candidatos (case-insensitive)."""
    cols = obj.columns if hasattr(obj, "columns") else obj
    lower = {c.lower(): c for c in cols}
    for name in candidates:
        if name.lower() in lower:
            return lower[name.lower()]
    return None

def _read_municipios_polygons() -> gpd.GeoDataFrame:
    """
    Lee polígonos municipales desde cache automatizado o manual.
    # FIX v1: Búsqueda extendida en DATA_RAW.
    """
    bases = [DATA_RAW / "municipios", MANUAL_DATA_DIR / "municipios"]
    for base in bases:
        if not base.exists(): continue
        candidates = list(base.glob("*.geojson")) + list(base.glob("*.json")) + \
                     list(base.glob("*.gpkg")) + list(base.glob("*.shp"))
        if candidates:
            try:
                gdf = gpd.read_file(candidates[0])
                logger.info("SUELO: Usando capa municipal %s", candidates[0].name)
                return gdf
            except Exception as e:
                logger.warning("SUELO: Error leyendo capa %s: %s", candidates[0].name, e)
    return gpd.GeoDataFrame()

def resumir_aptitud_suelo_por_municipio(gdf_sipra: gpd.GeoDataFrame,
                                        df_divipola: pd.DataFrame) -> pd.DataFrame:
    """
    Extrae la clase dominante de aptitud agrícola por municipio.
    # FIX v1: Priorización de columnas de código DIVIPOLA.
    """
    if gdf_sipra is None or gdf_sipra.empty:
        return pd.DataFrame()

    cod_col = _pick_col(gdf_sipra, ["codmunicipio", "cod_municipio", "id_municipio", "divipola"])
    if cod_col:
        return _resumir_por_codigo(gdf_sipra, cod_col)
    return _resumir_por_overlay(gdf_sipra)

def _resumir_por_codigo(gdf: gpd.GeoDataFrame, cod_col: str) -> pd.DataFrame:
    """Ruta directa cuando el dataset ya tiene códigos DIVIPOLA."""
    df = pd.DataFrame(gdf.drop(columns=["geometry"], errors="ignore"))
    df = df.rename(columns={cod_col: "id_municipio"})
    df["id_municipio"] = df["id_municipio"].astype(str).str.zfill(5)

    cols_map = {
        "clase_aptitud":       _pick_col(df, _CATEGORY_COLS),
        "tipo_suelo":          _pick_col(df, _SOIL_COLS),
        "textura_suelo":       _pick_col(df, _TEXTURE_COLS),
        "pendiente_dominante": _pick_col(df, _SLOPE_COLS),
        "drenaje":             _pick_col(df, _DRAIN_COLS),
        "limitante_principal": _pick_col(df, _LIMIT_COLS),
        "producto":            _pick_col(df, _PRODUCT_COLS),
    }

    result = pd.DataFrame({"id_municipio": df["id_municipio"]})
    for out_col, src_col in cols_map.items():
        result[out_col] = df[src_col] if src_col else None

    if result["producto"].notna().any():
        result["producto"] = result["producto"].astype(str).str.replace(r"^aptitud_", "", regex=True).str.upper()

    result = result.drop_duplicates(subset=["id_municipio", "producto"] if "producto" in result.columns else ["id_municipio"])
    logger.info("SUELO: %s registros por código (SIPRA)", len(result))
    return result

def _resumir_por_overlay(gdf_sipra: gpd.GeoDataFrame) -> pd.DataFrame:
    """Join espacial (overlay) con polígonos municipales."""
    gdf_municipios = _read_municipios_polygons()
    if gdf_municipios.empty:
        logger.warning("SUELO: No hay polígonos municipales para overlay")
        return pd.DataFrame()

    muni_id_col = _pick_col(gdf_municipios, ["id_municipio", "cod_mpio", "divipola"])
    if muni_id_col is None:
        return pd.DataFrame()

    gdf_municipios = gdf_municipios.rename(columns={muni_id_col: "id_municipio"})
    gdf_municipios["id_municipio"] = gdf_municipios["id_municipio"].astype(str).str.zfill(5)

    if gdf_sipra.crs and gdf_municipios.crs and gdf_sipra.crs != gdf_municipios.crs:
        gdf_sipra = gdf_sipra.to_crs(gdf_municipios.crs)

    joined = gpd.overlay(gdf_sipra, gdf_municipios[["id_municipio", "geometry"]], how="intersection")
    if joined.empty:
        return pd.DataFrame()

    joined["_area"] = joined.geometry.area
    joined = joined.sort_values(["id_municipio", "_area"], ascending=[True, False])
    grouped = joined.groupby("id_municipio", as_index=False).first()

    cols_map = {
        "clase_aptitud":       _pick_col(grouped, _CATEGORY_COLS),
        "producto":            _pick_col(grouped, _PRODUCT_COLS),
    }

    result = pd.DataFrame({"id_municipio": grouped["id_municipio"]})
    for out_col, src_col in cols_map.items():
        result[out_col] = grouped[src_col] if src_col else None

    logger.info("SUELO: %s municipios mediante overlay espacial", len(result))
    return result

def load_censo_agropecuario_local() -> pd.DataFrame:
    """
    Carga datos del CNA con soporte para Parquet/Subdirs.
    # FIX v1: Estandarización de carga y mapeo de columnas.
    """
    path_parquet = DATA_RAW / "cna" / "cna_raw.parquet"
    path_csv = DATA_RAW / "cna_raw_automatizado.csv"
    
    if path_parquet.exists():
        df = pd.read_parquet(path_parquet)
    elif path_csv.exists():
        df = pd.read_csv(path_csv)
    else:
        base = MANUAL_DATA_DIR / "cna"
        candidates = list(base.glob("*.csv")) + list(base.glob("*.xlsx")) + \
                     list(base.glob("*.xls")) + list(base.glob("*.parquet"))
        if not candidates:
            return pd.DataFrame()
        path = candidates[0]
        suffix = path.suffix.lower()
        df = pd.read_parquet(path) if suffix == ".parquet" else \
             pd.read_csv(path) if suffix == ".csv" else pd.read_excel(path)

    rename_map = {}
    lower_cols = {col.lower(): col for col in df.columns}
    mapping = {
        "id_municipio":                  ["id_municipio", "cod_mpio", "divipola"],
        "area_cultivos_permanentes_ha":   ["area_cultivos_permanentes_ha"],
        "area_cultivos_transitorios_ha":  ["area_cultivos_transitorios_ha"],
    }
    for target, cands in mapping.items():
        for c in cands:
            if c.lower() in lower_cols:
                rename_map[lower_cols[c.lower()]] = target
                break

    df = df.rename(columns=rename_map)
    if "id_municipio" in df.columns:
        df["id_municipio"] = df["id_municipio"].astype(str).str.zfill(5)
        
    logger.info("CNA: %s registros cargados", len(df))
    return df
