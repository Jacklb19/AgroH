"""
clean_municipios.py — Resolución de municipios y asignación espacial de estaciones.

Correcciones aplicadas:
  - Corrección 2.3.a (2026-04-29): Cache a nivel de módulo para load_divipola_map
    y build_synonym_map usando functools.lru_cache.
  - Corrección 2.3.b (2026-04-29): Bloqueo de pipeline ante exceso de nulos.
    DataQualityError si % no resueltos > fail_on_unresolved_pct.
    Filtra dropna de id_municipio antes de retornar si está por debajo del umbral.
"""
import functools
import logging
import unicodedata

import numpy as np
import pandas as pd

from config.settings import DATA_RAW, BASE_DIR

logger = logging.getLogger(__name__)

# Umbral por defecto para bloquear pipeline por municipios no resueltos
DEFAULT_FAIL_ON_UNRESOLVED_PCT = 0.10  # 10%


class DataQualityError(Exception):
    """Excepción lanzada cuando la calidad de datos no alcanza el umbral mínimo."""
    pass


DEPARTAMENTO_REGION = {
    "05": "Andina",
    "08": "Caribe",
    "11": "Andina",
    "13": "Caribe",
    "15": "Andina",
    "17": "Andina",
    "18": "Amazonía",
    "19": "Pacífico",
    "20": "Caribe",
    "23": "Caribe",
    "25": "Andina",
    "27": "Pacífico",
    "41": "Andina",
    "44": "Caribe",
    "47": "Caribe",
    "50": "Orinoquía",
    "52": "Pacífico",
    "54": "Andina",
    "63": "Andina",
    "66": "Andina",
    "68": "Andina",
    "70": "Caribe",
    "73": "Andina",
    "76": "Pacífico",
    "81": "Orinoquía",
    "85": "Orinoquía",
    "86": "Amazonía",
    "88": "Caribe",
    "91": "Amazonía",
    "94": "Amazonía",
    "95": "Amazonía",
    "97": "Amazonía",
    "99": "Orinoquía",
}

def _normalizar_texto(s: str) -> str:
    """Minúsculas, sin tildes, sin paréntesis de departamento."""
    if not isinstance(s, str):
        return ""
    s = s.lower().strip()
    s = s.split("(")[0].strip()                    # quita "(Santander)" etc.
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s


def _to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(",", ".", regex=False), errors="coerce")


# --- Corrección 2.3.a: Cache a nivel de módulo ---
@functools.lru_cache(maxsize=None)
def build_synonym_map() -> tuple:
    """
    Lee config/synonyms_municipios.csv y devuelve {nombre_normalizado: divipola}.
    El CSV tiene columnas: sinonimo, divipola

    Corrección 2.3.a: Cacheado con lru_cache.
    Retorna tuple de items para que sea hashable; se convierte a dict al usar.
    """
    path = BASE_DIR / "config" / "synonyms_municipios.csv"
    if not path.exists():
        logger.warning("synonyms_municipios.csv no encontrado, usando mapa vacío")
        return ()
    df = pd.read_csv(path, dtype=str)
    result = {_normalizar_texto(row["sinonimo"]): row["divipola"] for _, row in df.iterrows()}
    return tuple(result.items())


@functools.lru_cache(maxsize=None)
def load_divipola_map() -> tuple:
    """
    Carga el catálogo DIVIPOLA y devuelve {nombre_normalizado: codigo_divipola}.

    Corrección 2.3.a: Cacheado con lru_cache.
    Retorna tuple de items para que sea hashable; se convierte a dict al usar.
    """
    path_parquet = DATA_RAW / "divipola" / "divipola_raw.parquet"
    path_csv = DATA_RAW / "divipola.csv"

    if path_parquet.exists():
        df = pd.read_parquet(path_parquet)
    elif path_csv.exists():
        df = pd.read_csv(path_csv, dtype=str)
    else:
        logger.error("DIVIPOLA: No se encontró el archivo de referencia en %s", path_parquet)
        return ()
    result = {}
    for _, row in df.iterrows():
        nombre = _normalizar_texto(row.get("nom_mpio", row.get("municipio", "")))
        codigo = str(row.get("cod_mpio", row.get("divipola", ""))).zfill(5)
        if nombre:
            result[nombre] = codigo
    return tuple(result.items())


def _get_divipola_dict() -> dict:
    """Helper que convierte el resultado cacheado de load_divipola_map a dict."""
    return dict(load_divipola_map())


def _get_synonym_dict() -> dict:
    """Helper que convierte el resultado cacheado de build_synonym_map a dict."""
    return dict(build_synonym_map())


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
    logger.debug(f"Municipio no resuelto: '{nombre}' -> '{norm}'")
    return None

def agregar_id_municipio(df: pd.DataFrame, col_nombre: str,
                         fail_on_unresolved_pct: float = DEFAULT_FAIL_ON_UNRESOLVED_PCT) -> pd.DataFrame:
    """
    Agrega columna id_municipio (DIVIPOLA) a cualquier DataFrame
    que tenga una columna con nombres de municipio.

    Corrección 2.3.a: Usa mapas cacheados a nivel de módulo.
    Corrección 2.3.b: Lanza DataQualityError si el % de no resueltos
    supera fail_on_unresolved_pct. Filtra registros con id_municipio=None
    si está por debajo del umbral.
    """
    divipola_map = _get_divipola_dict()
    synonym_map  = _get_synonym_dict()
    df = df.copy()
    df["id_municipio"] = df[col_nombre].apply(
        lambda x: resolver_municipio(x, divipola_map, synonym_map)
    )
    nulos = df["id_municipio"].isna().sum()
    total = len(df)
    pct   = nulos / total * 100 if total > 0 else 0
    pct_decimal = nulos / total if total > 0 else 0

    logger.info(f"Normalización municipios: {total - nulos}/{total} resueltos ({pct:.1f}% sin resolver)")

    if pct_decimal > fail_on_unresolved_pct:
        # Obtener sample de municipios no resueltos para el mensaje
        sample_no_resueltos = (
            df[df["id_municipio"].isna()][col_nombre]
            .value_counts()
            .head(10)
            .to_dict()
        )
        raise DataQualityError(
            f"Más del {fail_on_unresolved_pct*100:.0f}% de municipios sin resolver "
            f"({pct:.1f}% real). Sample no resueltos: {sample_no_resueltos}"
        )

    if nulos > 0 and pct > 5:
        logger.warning(
            f"Más del 5% de municipios sin resolver ({pct:.1f}%) — "
            f"revisar synonyms_municipios.csv. "
            f"Filtrando {nulos} registros con id_municipio=None."
        )

    # Filtrar registros con id_municipio=None para evitar violaciones FK
    df = df.dropna(subset=["id_municipio"])

    return df


def build_region_map_from_divipola(df_divipola: pd.DataFrame) -> pd.DataFrame:
    """Construye un mapeo municipio -> región natural usando el código de departamento."""
    df = df_divipola.copy()
    df["id_municipio"] = df["cod_mpio"].astype(str).str.zfill(5)
    df["id_departamento"] = df["cod_dpto"].astype(str).str.zfill(2)
    df["nombre_region"] = df["id_departamento"].map(DEPARTAMENTO_REGION)
    return df[["id_municipio", "nombre_region"]].drop_duplicates()


def _haversine_km(lat1: float, lon1: float, lat2: np.ndarray, lon2: np.ndarray) -> np.ndarray:
    lat1_rad = np.radians(lat1)
    lon1_rad = np.radians(lon1)
    lat2_rad = np.radians(lat2)
    lon2_rad = np.radians(lon2)

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = (
        np.sin(dlat / 2.0) ** 2
        + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2.0) ** 2
    )
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return 6371.0 * c


def _get_radius_for_region(region_name: str | None) -> float:
    """
    Retorna el radio de búsqueda en km según la región natural del municipio.
    Corrección 3.2.b: Usa diccionario de radios por región desde settings.
    """
    from config.settings import SPATIAL_JOIN_RADIUS_KM, SPATIAL_JOIN_FALLBACK_NO_LIMIT

    if isinstance(SPATIAL_JOIN_RADIUS_KM, dict):
        if region_name:
            key = region_name.lower().replace("í", "i").replace("ó", "o").replace("á", "a")
            return SPATIAL_JOIN_RADIUS_KM.get(key, SPATIAL_JOIN_RADIUS_KM.get("default", 100))
        return SPATIAL_JOIN_RADIUS_KM.get("default", 100)
    # Compatibilidad: si sigue siendo un int
    return float(SPATIAL_JOIN_RADIUS_KM)


def asignar_estaciones_a_municipios(
    df_estaciones: pd.DataFrame,
    df_divipola: pd.DataFrame,
    fallback_col: str = "municipio",
    max_radius_km: float = None,
) -> pd.DataFrame:
    """
    Asigna cada estación al municipio más cercano usando coordenadas.
    Si faltan coordenadas o no hay match confiable, usa el nombre del municipio como respaldo.

    Corrección 3.2.b: Usa radio por región natural si SPATIAL_JOIN_RADIUS_KM es dict.
    """
    df = df_estaciones.copy()
    df["latitud"] = _to_float(df["latitud"])
    df["longitud"] = _to_float(df["longitud"])

    municipios = df_divipola.copy()
    municipios["id_municipio"] = municipios["cod_mpio"].astype(str).str.zfill(5)
    municipios["latitud_centroide"] = _to_float(municipios["latitud"])
    municipios["longitud_centroide"] = _to_float(municipios["longitud"])
    # Añadimos altitud de divipola si existe, o 0 si no
    municipios["altitud_muni"] = _to_float(municipios.get("altitud", pd.Series(0, index=municipios.index)))

    # Agregar región para radio variable
    if "cod_dpto" in municipios.columns:
        municipios["id_departamento"] = municipios["cod_dpto"].astype(str).str.zfill(2)
        municipios["nombre_region"] = municipios["id_departamento"].map(DEPARTAMENTO_REGION)
    else:
        municipios["nombre_region"] = None

    municipios = municipios.dropna(subset=["latitud_centroide", "longitud_centroide"])

    muni_lat = municipios["latitud_centroide"].to_numpy()
    muni_lon = municipios["longitud_centroide"].to_numpy()
    muni_alt = municipios["altitud_muni"].to_numpy()
    muni_ids = municipios["id_municipio"].to_numpy()
    muni_regions = municipios["nombre_region"].to_numpy()

    assigned_ids: list[str | None] = []
    distances: list[float | None] = []
    methods: list[str] = []

    divipola_map = _get_divipola_dict()
    synonym_map = _get_synonym_dict()

    for row in df.itertuples(index=False):
        lat = getattr(row, "latitud", None)
        lon = getattr(row, "longitud", None)
        fallback_name = getattr(row, fallback_col, None) if hasattr(row, fallback_col) else None

        alt_estacion = _to_float(pd.Series([getattr(row, "altitud", 0)])).iloc[0]

        if pd.notna(lat) and pd.notna(lon) and len(muni_ids) > 0:
            dist_2d = _haversine_km(float(lat), float(lon), muni_lat, muni_lon)

            # Penalización altitudinal: sumar 10km por cada 100m de diferencia (ajuste empírico)
            diferencia_alt = np.abs(muni_alt - alt_estacion)
            penalizacion = (diferencia_alt / 100.0) * 10.0

            # Distancia penalizada
            dist = dist_2d + penalizacion

            idx = int(np.argmin(dist))
            nearest_distance = float(dist_2d[idx]) # Guardamos la distancia 2D real, no la penalizada

            # Obtener radio para la región del municipio más cercano
            region_nearest = muni_regions[idx] if idx < len(muni_regions) else None
            effective_radius = max_radius_km if max_radius_km else _get_radius_for_region(region_nearest)

            assigned_ids.append(str(muni_ids[idx]))
            distances.append(nearest_distance)
            if nearest_distance <= effective_radius and diferencia_alt[idx] <= 400:
                methods.append("spatial_within_radius_and_altitude")
            else:
                methods.append("spatial_nearest_outside_radius_or_altitude")
            continue

        fallback_id = resolver_municipio(fallback_name, divipola_map, synonym_map)
        assigned_ids.append(fallback_id)
        distances.append(None)
        methods.append("text_fallback" if fallback_id else "unresolved")

    df["id_municipio"] = assigned_ids
    df["distancia_municipio_km"] = distances
    df["metodo_asignacion_municipio"] = methods

    fuera_radio = (df["metodo_asignacion_municipio"] == "spatial_nearest_outside_radius_or_altitude").sum()
    sin_resolver = df["id_municipio"].isna().sum()
    logger.info(
        "Asignación espacial estaciones->municipio: %s estaciones, %s fuera del radio, %s sin resolver",
        len(df),
        int(fuera_radio),
        int(sin_resolver),
    )
    return df
