"""
clean_insumos.py — Limpieza y normalización de precios de insumos agrícolas (A07)

Normaliza columnas variables según distintos formatos del IPIA/DANE y produce
un DataFrame listo para cargar en fact_precios_insumos.
"""
import logging
import unicodedata

import pandas as pd

logger = logging.getLogger(__name__)

# Mapa de tipos de insumo conocidos para normalizar etiquetas
_TIPO_INSUMO_MAP = {
    "fertilizante": "fertilizante",
    "abono": "fertilizante",
    "urea": "fertilizante",
    "dap": "fertilizante",
    "fosfato": "fertilizante",
    "herbicida": "agroquimico",
    "fungicida": "agroquimico",
    "insecticida": "agroquimico",
    "agroqu": "agroquimico",
    "jornal": "mano_de_obra",
    "mano de obra": "mano_de_obra",
    "semilla": "semilla",
    "combustible": "combustible",
    "gasolina": "combustible",
    "diesel": "combustible",
    "ipia": "indice",
    "indice": "indice",
}

# Posibles nombres de columna por campo
_ALIAS = {
    "fecha":            ["fecha", "fecha_registro", "periodo", "mes_año", "fecha_dato"],
    "tipo_insumo":      ["tipo_insumo", "tipo", "categoria", "grupo"],
    "nombre_insumo":    ["nombre_insumo", "insumo", "producto", "descripcion", "nombre"],
    "precio_cop_unidad":["precio_cop_unidad", "precio", "valor", "precio_cop", "precio_cop_kg", "precio_promedio"],
    "unidad_medida":    ["unidad_medida", "unidad", "presentacion"],
    "region":           ["region", "region_natural", "departamento", "zona"],
}


def _normalizar_texto(texto: str) -> str:
    if not isinstance(texto, str):
        return ""
    texto = unicodedata.normalize("NFD", texto.strip().lower())
    return "".join(c for c in texto if unicodedata.category(c) != "Mn")


def _detectar_columna(df: pd.DataFrame, candidatos: list[str]) -> str | None:
    lower_map = {col.lower(): col for col in df.columns}
    for c in candidatos:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None


def _clasificar_tipo(valor: str) -> str:
    norm = _normalizar_texto(str(valor))
    for kw, tipo in _TIPO_INSUMO_MAP.items():
        if kw in norm:
            return tipo
    return "otro"


def normalizar_insumos(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza el DataFrame crudo de insumos A07 al esquema de fact_precios_insumos:
        fecha, tipo_insumo, nombre_insumo, precio_cop_unidad, unidad_medida, region
    """
    if df_raw.empty:
        return pd.DataFrame()

    df = df_raw.copy()

    # ── Mapear columnas ─────────────────────────────────────────────────────
    renombres: dict[str, str] = {}
    for campo, candidatos in _ALIAS.items():
        col_original = _detectar_columna(df, candidatos)
        if col_original and col_original != campo:
            renombres[col_original] = campo
    df = df.rename(columns=renombres)

    # ── Asegurar columnas mínimas ────────────────────────────────────────────
    for campo in _ALIAS:
        if campo not in df.columns:
            df[campo] = None

    # ── Parsear fecha ────────────────────────────────────────────────────────
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce", dayfirst=True)
    df = df.dropna(subset=["fecha"])
    df["anio"] = df["fecha"].dt.year
    df["mes"]  = df["fecha"].dt.month

    # ── Normalizar tipo de insumo ────────────────────────────────────────────
    df["tipo_insumo"] = df["tipo_insumo"].fillna(df["nombre_insumo"]).apply(_clasificar_tipo)

    # ── Limpiar nombre del insumo ────────────────────────────────────────────
    df["nombre_insumo"] = df["nombre_insumo"].astype(str).str.strip().str.upper()

    # ── Precio numérico ──────────────────────────────────────────────────────
    df["precio_cop_unidad"] = pd.to_numeric(df["precio_cop_unidad"], errors="coerce")
    df = df.dropna(subset=["precio_cop_unidad"])

    # ── Región: normalizar a nombre de región natural IDEAM ─────────────────
    df["region"] = df["region"].astype(str).str.strip()

    cols_salida = [
        "anio", "mes", "tipo_insumo", "nombre_insumo",
        "precio_cop_unidad", "unidad_medida", "region",
    ]
    for c in cols_salida:
        if c not in df.columns:
            df[c] = None

    result = df[cols_salida].copy()
    logger.info("Insumos normalizados: %s registros", len(result))
    return result
