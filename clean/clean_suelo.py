"""
clean_suelo.py — Limpieza y resumen de aptitud de suelo por municipio.

Correcciones aplicadas:
  - Corrección 2.5.a (2026-04-29): Advertencia sobre herencia de coeficientes fijos.
    Verifica area_cultivo_fuente del CNA y emite logger.error si 'no_disponible'.
    Agrega campo area_dato_confiable.
  - Corrección 2.5.b (2026-04-29): Reemplaza .first() por moda en _resumir_por_overlay.
    Agrega n_zonas y pct_apta al output.
  - Corrección 2.5.c (2026-04-29): Distribución de aptitudes en _resumir_por_codigo.
    Genera pct_apta_municipio, pct_mod_apta_municipio, pct_no_apta_municipio,
    aptitud_predominante.
"""
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


def _moda_aptitud(series: pd.Series) -> str:
    """Retorna la moda (valor más frecuente) de una serie de aptitudes."""
    if series.empty:
        return "Sin Información"
    return series.mode().iloc[0]


def resumir_aptitud_suelo_por_municipio(gdf_sipra: gpd.GeoDataFrame,
                                        df_divipola: pd.DataFrame) -> pd.DataFrame:
    """
    Extrae la clase dominante de aptitud agrícola por municipio.
    """
    if gdf_sipra is None or gdf_sipra.empty:
        return pd.DataFrame()

    cod_col = _pick_col(gdf_sipra, ["codmunicipio", "cod_municipio", "id_municipio", "divipola"])
    if cod_col:
        return _resumir_por_codigo(gdf_sipra, cod_col)
    return _resumir_por_overlay(gdf_sipra)


def _resumir_por_codigo(gdf: gpd.GeoDataFrame, cod_col: str) -> pd.DataFrame:
    """
    Ruta directa cuando el dataset ya tiene códigos DIVIPOLA.

    Corrección 2.5.c: Genera distribución de aptitudes por municipio+producto
    con columnas pct_apta_municipio, pct_mod_apta_municipio, pct_no_apta_municipio,
    aptitud_predominante.
    """
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

    aptitud_col = "clase_aptitud"
    if aptitud_col in result.columns and result[aptitud_col].notna().any():
        # Corrección 2.5.c: Calcular distribución de aptitudes
        group_cols = ["id_municipio"]
        if "producto" in result.columns and result["producto"].notna().any():
            group_cols.append("producto")

        def _calc_distribution(group):
            total = len(group)
            if total == 0:
                return pd.Series({
                    "pct_apta_municipio": 0.0,
                    "pct_mod_apta_municipio": 0.0,
                    "pct_no_apta_municipio": 0.0,
                    "aptitud_predominante": "Sin Información",
                    "n_zonas": 0,
                })
            aptitudes = group[aptitud_col].astype(str).str.casefold()
            return pd.Series({
                "pct_apta_municipio": round(
                    aptitudes.str.contains("alta|apta", na=False).sum() / total * 100, 1
                ),
                "pct_mod_apta_municipio": round(
                    aptitudes.str.contains("moderada|media", na=False).sum() / total * 100, 1
                ),
                "pct_no_apta_municipio": round(
                    aptitudes.str.contains("no apta|no_apta|marginal|exclusion", na=False).sum() / total * 100, 1
                ),
                "aptitud_predominante": _moda_aptitud(group[aptitud_col]),
                "n_zonas": total,
            })

        distribution = result.groupby(group_cols, dropna=False).apply(
            _calc_distribution, include_groups=False
        ).reset_index()

        # Merge distribución de vuelta al resultado deduplicado
        result_dedup = result.drop_duplicates(subset=group_cols)
        result_dedup = result_dedup.merge(distribution, on=group_cols, how="left")
        result = result_dedup
    else:
        result = result.drop_duplicates(
            subset=["id_municipio", "producto"] if "producto" in result.columns else ["id_municipio"]
        )

    logger.info("SUELO: %s registros por código (SIPRA)", len(result))
    return result


def _resumir_por_overlay(gdf_sipra: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Join espacial (overlay) con polígonos municipales.

    Corrección 2.5.b: Reemplaza .first() por moda. Agrega n_zonas y pct_apta.
    """
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

    # Corrección 2.5.b: Usar moda en vez de .first()
    aptitud_col = _pick_col(joined, _CATEGORY_COLS)
    producto_col = _pick_col(joined, _PRODUCT_COLS)

    if aptitud_col:
        resumen = joined.groupby("id_municipio", as_index=False).agg(
            aptitud_predominante=(aptitud_col, _moda_aptitud),
            n_zonas=(aptitud_col, "count"),
            pct_apta=(aptitud_col, lambda x: round((x.astype(str).str.casefold().str.contains("apta|alta", na=False)).mean() * 100, 1)),
        )
    else:
        resumen = joined.groupby("id_municipio", as_index=False).first()

    # Agregar producto si existe
    if producto_col and producto_col in joined.columns:
        producto_moda = joined.groupby("id_municipio")[producto_col].agg(_moda_aptitud).reset_index()
        producto_moda = producto_moda.rename(columns={producto_col: "producto"})
        resumen = resumen.merge(producto_moda, on="id_municipio", how="left")

    # Renombrar para consistencia
    result = pd.DataFrame({"id_municipio": resumen["id_municipio"]})
    if "aptitud_predominante" in resumen.columns:
        result["clase_aptitud"] = resumen["aptitud_predominante"]
    if "producto" in resumen.columns:
        result["producto"] = resumen["producto"]
    if "n_zonas" in resumen.columns:
        result["n_zonas"] = resumen["n_zonas"]
    if "pct_apta" in resumen.columns:
        result["pct_apta"] = resumen["pct_apta"]

    logger.info("SUELO: %s municipios mediante overlay espacial", len(result))
    return result


def load_censo_agropecuario_local() -> pd.DataFrame:
    """
    Carga datos del CNA con soporte para Parquet/Subdirs.

    Corrección 2.5.a: Verifica area_cultivo_fuente y emite logger.error
    si los datos de área cultivable no son confiables.
    Agrega campo area_dato_confiable.
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

    # Corrección 2.5.a: Verificar area_cultivo_fuente
    if "area_cultivo_fuente" in df.columns:
        no_disponible_count = (df["area_cultivo_fuente"] == "no_disponible").sum()
        if no_disponible_count > 0:
            logger.error(
                "CNA: %s/%s municipios tienen area_cultivo_fuente='no_disponible'. "
                "Las estimaciones de área cultivable NO son confiables. "
                "El análisis de aptitud de suelo tendrá baja precisión.",
                no_disponible_count, len(df)
            )
        df["area_dato_confiable"] = df["area_cultivo_fuente"] == "real_cna"
    else:
        # Sin campo de fuente — asumir no confiable y advertir
        logger.warning(
            "CNA: campo 'area_cultivo_fuente' no encontrado. "
            "No se puede verificar la confiabilidad de las áreas de cultivo."
        )
        df["area_dato_confiable"] = False

    logger.info("CNA: %s registros cargados", len(df))
    return df
