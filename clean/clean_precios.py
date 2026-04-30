"""
clean_precios.py — Limpieza y normalización de precios mayoristas SIPSA.

Correcciones aplicadas:
  - Corrección 2.4.a (2026-04-29): Reemplazado fillna(0.0) por fillna(np.nan)
    en columnas de precio para evitar promedios artificialmente bajos.
  - Corrección 2.4.b (2026-04-29): _find_column acepta lista de candidatos
    ordenados por preferencia. Lanza ValueError si no encuentra columna de precio.
  - Corrección 2.4.c (2026-04-29): Deduplicación correcta en construir_dim_centrales
    con nulos en id_municipio.
"""
import logging

import numpy as np
import pandas as pd

from clean.clean_municipios import agregar_id_municipio

logger = logging.getLogger(__name__)

# Mapeo de homologación de nombres SIPSA -> EVA
MAPEO_SIPSA_EVA = {
    "BANANO*": "BANANO",
    "MORA DE CASTILLA": "MORA",
    "PLATANO HARTON VERDE": "PLATANO",
    "PLATANO HARTON MADURO": "PLATANO",
    "ARVEJA VERDE EN VAINA": "ARVEJA",
    "CEBOLLA CABEZONA BLANCA": "CEBOLLA DE BULBO",
    "CEBOLLA CABEZONA ROJA": "CEBOLLA DE BULBO",
    "CEBOLLA JUNCA": "CEBOLLA DE RAMA",
    "CHOCOLO MAZORCA": "MAIZ",
    "LECHUGA BATAVIA": "LECHUGA",
    "AGUACATE*": "AGUACATE",
    "AGUACATE *": "AGUACATE",
    "LIMON TAHITI": "LIMON",
    "LIMON COMUN": "LIMON",
    "MANGO TOMMY": "MANGO",
    "MANZANA ROYAL GALA": "MANZANA",
    "FRIJOL VERDE*": "FRIJOL",
    "TOMATE*": "TOMATE",
    "GUAYABA*": "GUAYABA",
    "PINA *": "PINA",
    "PINA*": "PINA",
    "PAPA CRIOLLA": "PAPA",
    "PAPA NEGRA*": "PAPA",
    "NARANJA*": "NARANJA",
    "YUCA*": "YUCA",
    "MANDARINA*": "MANDARINA",
    "ARRACACHA*": "ARRACACHA",
    "PAPAYA MARADOL": "PAPAYA",
    "PLATANO GUINEO": "PLATANO",
}


def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """
    Busca una columna entre candidatos (case-insensitive), ordenados por preferencia.

    Corrección 2.4.b: Acepta lista de candidatos por preferencia.
    """
    lower_map = {col.lower(): col for col in df.columns}
    for candidate in candidates:
        if candidate.lower() in lower_map:
            return lower_map[candidate.lower()]
    return None


def normalizar_precios_sipsa(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Homologa microdatos SIPSA a la forma mensual esperada por el loader.

    Corrección 2.4.a: fillna(np.nan) en precios.
    Corrección 2.4.b: _find_column lanza ValueError si falta columna de precio.
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
        # Corrección 2.4.b: Lista ampliada de candidatos para precio promedio
        "precio_prom":    _find_column(df, [
            "precio_promedio_cop_kg", "precio_promedio", "precio_cop_kg", "precio"
        ]),
        "volumen":        _find_column(df, ["volumen", "volumen_ton", "cantidad"]),
    }

    # Verificar columnas obligatorias
    required = ["fecha_registro", "producto", "nombre_central", "ciudad", "precio_prom"]
    missing = [k for k in required if cols_map[k] is None]
    if missing:
        # Corrección 2.4.b: Mensaje detallado con columnas disponibles
        if "precio_prom" in missing:
            raise ValueError(
                f"No se encontró columna de precio. "
                f"Columnas disponibles: {df.columns.tolist()}"
            )
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

    # Homologación de cultivos SIPSA -> EVA
    df["producto"] = df["producto"].replace(MAPEO_SIPSA_EVA)

    df["anio"] = df["fecha_registro"].dt.year
    df["mes"] = df["fecha_registro"].dt.month

    # Corrección 2.4.a: Usar np.nan en vez de 0.0 para columnas de precio faltantes
    num_cols = ["precio_min", "precio_max", "precio_prom", "volumen"]
    for col in num_cols:
        if col not in df.columns:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(np.nan)

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
            "volumen":     lambda x: x.sum(min_count=1),  # sum con min_count=1 retorna NaN si todo es NaN
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
    """
    Extrae la dimensión única de centrales de abastos.

    Corrección 2.4.c: Deduplicación correcta con nulos en id_municipio.
    Ordena por id_municipio (nulos al final) y deduplica por nombre_central+ciudad,
    manteniendo la fila con id_municipio resuelto si existe.
    """
    if df_precios.empty:
        return pd.DataFrame()

    df_dim = (
        df_precios[["nombre_central", "ciudad", "id_municipio"]]
        .dropna(subset=["nombre_central", "ciudad"])
        .copy()
    )

    # Corrección 2.4.c: Ordenar para que filas con id_municipio resuelto estén primero
    df_dim = df_dim.sort_values("id_municipio", na_position="last")
    df_dim = df_dim.drop_duplicates(
        subset=["nombre_central", "ciudad"],
        keep="first"
    )

    return df_dim
