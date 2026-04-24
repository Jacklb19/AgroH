import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd

from config.settings import MANUAL_DATA_DIR

logger = logging.getLogger(__name__)


def _read_municipios_polygons() -> gpd.GeoDataFrame:
    base = MANUAL_DATA_DIR / "municipios"
    candidates = list(base.glob("*.geojson")) + list(base.glob("*.json")) + list(base.glob("*.gpkg"))
    if not candidates:
        return gpd.GeoDataFrame()
    return gpd.read_file(candidates[0])


def resumir_aptitud_suelo_por_municipio(
    gdf_sipra: gpd.GeoDataFrame,
    df_divipola: pd.DataFrame,
) -> pd.DataFrame:
    """
    Cruza polígonos SIPRA con municipios cuando existen polígonos municipales locales.
    Si no están disponibles, devuelve vacío de forma segura.
    """
    if gdf_sipra.empty:
        return pd.DataFrame()

    gdf_municipios = _read_municipios_polygons()
    if gdf_municipios.empty:
        logger.warning(
            "No se encontraron polígonos municipales locales; no es posible construir fact_aptitud_suelo todavía"
        )
        return pd.DataFrame()

    muni_id_col = next(
        (col for col in gdf_municipios.columns if col.lower() in {"id_municipio", "cod_mpio", "divipola"}),
        None,
    )
    if muni_id_col is None:
        logger.warning("La capa municipal no contiene id_municipio/cod_mpio/divipola")
        return pd.DataFrame()

    gdf_municipios = gdf_municipios.rename(columns={muni_id_col: "id_municipio"})
    gdf_municipios["id_municipio"] = gdf_municipios["id_municipio"].astype(str).str.zfill(5)

    if gdf_sipra.crs and gdf_municipios.crs and gdf_sipra.crs != gdf_municipios.crs:
        gdf_sipra = gdf_sipra.to_crs(gdf_municipios.crs)

    joined = gpd.overlay(gdf_sipra, gdf_municipios[["id_municipio", "geometry"]], how="intersection")
    if joined.empty:
        return pd.DataFrame()

    joined["intersection_area"] = joined.geometry.area
    category_candidates = ["clase_aptitud", "aptitud", "categoria", "clase"]
    soil_candidates = ["tipo_suelo", "suelo"]
    texture_candidates = ["textura_suelo", "textura"]
    slope_candidates = ["pendiente_dominante", "pendiente"]
    drain_candidates = ["drenaje"]
    limit_candidates = ["limitante_principal", "limitante"]
    product_candidates = ["producto", "cultivo"]

    def pick(colnames: list[str]) -> str | None:
        lower = {col.lower(): col for col in joined.columns}
        for name in colnames:
            if name.lower() in lower:
                return lower[name.lower()]
        return None

    cols = {
        "clase_aptitud": pick(category_candidates),
        "tipo_suelo": pick(soil_candidates),
        "textura_suelo": pick(texture_candidates),
        "pendiente_dominante": pick(slope_candidates),
        "drenaje": pick(drain_candidates),
        "limitante_principal": pick(limit_candidates),
        "producto": pick(product_candidates),
    }

    sort_cols = ["id_municipio", "intersection_area"]
    joined = joined.sort_values(sort_cols, ascending=[True, False])
    grouped = joined.groupby("id_municipio", as_index=False).first()

    result = pd.DataFrame({"id_municipio": grouped["id_municipio"]})
    for output_col, source_col in cols.items():
        result[output_col] = grouped[source_col] if source_col else None
    return result


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
