import pandas as pd
import unicodedata
import logging
from config.settings import DATA_RAW, BASE_DIR

logger = logging.getLogger(__name__)

def _normalizar_texto(s: str) -> str:
    """Minúsculas, sin tildes, sin paréntesis de departamento."""
    if not isinstance(s, str):
        return ""
    s = s.lower().strip()
    s = s.split("(")[0].strip()                    # quita "(Santander)" etc.
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s

def build_synonym_map() -> dict:
    """
    Lee config/synonyms_municipios.csv y devuelve {nombre_normalizado: divipola}.
    El CSV tiene columnas: sinonimo, divipola
    """
    path = BASE_DIR / "config" / "synonyms_municipios.csv"
    if not path.exists():
        logger.warning("synonyms_municipios.csv no encontrado, usando mapa vacío")
        return {}
    df = pd.read_csv(path, dtype=str)
    return {_normalizar_texto(row["sinonimo"]): row["divipola"] for _, row in df.iterrows()}

def load_divipola_map() -> dict:
    """
    Carga el CSV de DIVIPOLA descargado y devuelve {nombre_normalizado: codigo_divipola}.
    """
    path = DATA_RAW / "divipola.csv"
    df = pd.read_csv(path, dtype=str)
    result = {}
    for _, row in df.iterrows():
        nombre = _normalizar_texto(row.get("nom_mpio", row.get("municipio", "")))
        codigo = str(row.get("cod_mpio", row.get("divipola", ""))).zfill(5)
        if nombre:
            result[nombre] = codigo
    return result

def resolver_municipio(nombre: str, divipola_map: dict, synonym_map: dict) -> str | None:
    """
    Devuelve el código DIVIPOLA (5 dígitos) para un nombre de municipio.
    Prioridad: 1) mapa directo, 2) sinónimos, 3) None.
    """
    norm = _normalizar_texto(nombre)
    if norm in divipola_map:
        return divipola_map[norm]
    if norm in synonym_map:
        return synonym_map[norm]
    logger.debug(f"Municipio no resuelto: '{nombre}' → '{norm}'")
    return None

def agregar_id_municipio(df: pd.DataFrame, col_nombre: str) -> pd.DataFrame:
    """
    Agrega columna id_municipio (DIVIPOLA) a cualquier DataFrame
    que tenga una columna con nombres de municipio.
    """
    divipola_map = load_divipola_map()
    synonym_map  = build_synonym_map()
    df = df.copy()
    df["id_municipio"] = df[col_nombre].apply(
        lambda x: resolver_municipio(x, divipola_map, synonym_map)
    )
    nulos = df["id_municipio"].isna().sum()
    total = len(df)
    pct   = nulos / total * 100
    logger.info(f"Normalización municipios: {total - nulos}/{total} resueltos ({pct:.1f}% sin resolver)")
    if pct > 5:
        logger.warning(f"Más del 5% de municipios sin resolver — revisar synonyms_municipios.csv")
    return df
