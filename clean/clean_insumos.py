"""
clean_insumos.py — Limpieza del Índice de Precios de Insumos Agrícolas (IPIA).

Correcciones aplicadas:
  - Corrección 2.2.a (2026-04-29): Eliminado melt sobre datos ya en formato long.
    Detecta automáticamente si el DataFrame está en formato long o wide.
  - Corrección 2.2.b (2026-04-29): Modo degradado para normalizar_insumos.
    Fallback a cache local si la DB no está disponible. No retorna DF vacío sin loguear.
  - Corrección 2.2.c (2026-04-29): Categorizador tipo_insumo mejorado con
    nomenclatura real de endpoints Socrata. Usa casefold(). "Sin Clasificar" en vez de "Otros".
"""
import logging
from pathlib import Path

import pandas as pd

from config.settings import DATA_RAW, DATA_PROCESSED

logger = logging.getLogger(__name__)


def _categorizar_insumo(nombre: str) -> str:
    """
    Clasifica un insumo por tipo según su nombre.

    Corrección 2.2.c: Cobertura ampliada para nomenclatura real de Socrata.
    Usa casefold() en todas las comparaciones. 'Sin Clasificar' cuando no hay match.
    """
    n = str(nombre).casefold()

    # Fertilizantes
    if any(x in n for x in [
        "fertilizante", "urea", "dap", "kcl", "sam", "npk",
        "fertilizantes compuestos", "abono", "cal dolomita",
        "cloruro de potasio", "fosfato"
    ]):
        return "Fertilizante"

    # Agroquímicos
    if any(x in n for x in [
        "herbicida", "fungicida", "insecticida", "plaguicida",
        "agroquimico", "agroquímico", "glifosato", "clorotalonil",
        "clorpirifos"
    ]):
        return "Agroquímico"

    # Mano de obra
    if any(x in n for x in ["mano de obra", "jornal"]):
        return "Mano de Obra"

    # Semillas
    if any(x in n for x in ["semilla", "material vegetal"]):
        return "Semillas"

    # Energía
    if any(x in n for x in ["combustible", "gasolina", "diesel", "acpm"]):
        return "Energía"

    # Arrendamiento
    if any(x in n for x in ["arriendo", "alquiler"]):
        return "Arrendamiento"

    logger.debug("INSUMOS: Insumo sin clasificar: '%s'", nombre)
    return "Sin Clasificar"


def clean_insumos_ipia() -> pd.DataFrame:
    """
    Limpia el Índice de Precios de Insumos Agrícolas (IPIA).

    Corrección 2.2.a: Detecta si los datos están en formato long o wide
    antes de aplicar melt. Si ya están en formato long, los usa directamente.
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
    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"]).dt.normalize()

    # --- Corrección 2.2.a: Detectar formato (long vs wide) ---
    if "nombre_insumo" in df.columns and "precio_cop_unidad" in df.columns:
        # Datos ya están en formato long — usar directamente
        logger.info("INSUMOS: Formato LONG detectado (columnas nombre_insumo, precio_cop_unidad presentes)")
        df_long = df.copy()
    else:
        # Datos en formato wide — aplicar melt
        logger.info("INSUMOS: Formato WIDE detectado — aplicando melt")
        exclude = ["fecha", "fuente_origen", "es_sintetico", "region",
                    "tipo_insumo", "unidad_medida"]
        index_cols = [c for c in df.columns if c not in exclude]

        id_vars = [c for c in df.columns if c in exclude]
        df_long = df.melt(
            id_vars=id_vars,
            value_vars=index_cols,
            var_name="nombre_insumo",
            value_name="precio_cop_unidad"
        )

    # 2. Clasificar tipo_insumo (Corrección 2.2.c)
    df_long["tipo_insumo"] = df_long["nombre_insumo"].apply(_categorizar_insumo)

    # 3. Limpieza de nombres
    df_long["nombre_insumo"] = (
        df_long["nombre_insumo"]
        .astype(str)
        .str.replace("_", " ")
        .str.strip()
        .str.title()
    )

    # 4. Unidad de medida por defecto si no existe
    if "unidad_medida" not in df_long.columns:
        df_long["unidad_medida"] = "Indice (Base 100)"

    out_path = DATA_PROCESSED / "insumos_clean.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_long.to_parquet(out_path, index=False)
    logger.info("INSUMOS: %s registros limpios -> %s", len(df_long), out_path)

    return df_long


def normalizar_insumos(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza los datos de insumos para la base de datos.

    Corrección 2.2.b: Modo degradado si la DB no está disponible.
    Intenta cargar insumos_normalizados_cache.parquet como fuente alternativa.
    NUNCA retorna pd.DataFrame() vacío sin loguear el motivo.
    """
    if df_raw is None or df_raw.empty:
        logger.warning("INSUMOS: DataFrame de entrada vacío para normalizar.")
        return pd.DataFrame()

    df = df_raw.copy()

    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    else:
        logger.warning("INSUMOS: No se encontró columna 'fecha' para normalizar")
        return df  # Retornar sin normalizar en vez de vacío

    # Recuperar id_tiempo
    from load.db import get_engine
    cache_path = DATA_PROCESSED / "insumos_normalizados_cache.parquet"

    engine = None
    try:
        engine = get_engine()
    except Exception as e:
        logger.warning("INSUMOS: No se pudo obtener conexión a DB: %s", e)

    if engine is not None:
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
            # Corrección 2.2.b: Intentar cache local
            if cache_path.exists():
                logger.warning("INSUMOS: Usando cache local de normalización: %s", cache_path)
                return pd.read_parquet(cache_path)
            logger.error(
                "INSUMOS: Sin conexión a DB y sin cache local. "
                "Retornando datos SIN normalizar para no perder información."
            )
            return df
    else:
        # Corrección 2.2.b: DB no disponible — intentar cache local
        if cache_path.exists():
            logger.warning(
                "INSUMOS: Sin conexión a DB, usando cache local: %s", cache_path
            )
            return pd.read_parquet(cache_path)

        logger.error(
            "INSUMOS: Sin conexión a DB y sin cache local de normalización. "
            "Retornando datos SIN normalizar para no perder información."
        )
        return df

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

    # Corrección 2.2.b: Guardar cache de normalización exitosa
    try:
        df_out.to_parquet(cache_path, index=False)
        logger.info("INSUMOS: Cache de normalización guardado en %s", cache_path)
    except Exception as e:
        logger.warning("INSUMOS: Error guardando cache de normalización: %s", e)

    return df_out

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    clean_insumos_ipia()
