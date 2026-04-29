"""
extract_cna.py — Extracción del Censo Nacional Agropecuario 2014 (DANE).

Correcciones aplicadas:
  - Corrección 1.1 (2026-04-29): Eliminados coeficientes fijos nacionales (60/40).
    Ahora lee las columnas reales de desglose por tipo de cultivo del Cuadro 1 del CNA.
    Si no se encuentran, deja np.nan con area_cultivo_fuente='no_disponible'.
"""
import logging
import re

import numpy as np
import pandas as pd
import requests
import urllib3
import io

from config.settings import DATA_RAW

# Desactivar advertencias de SSL para servidores del DANE si es necesario
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


def _detectar_columnas_cultivo(df: pd.DataFrame) -> dict:
    """
    Busca columnas de cultivos permanentes y transitorios por nombre aproximado
    en los headers del Excel del CNA.

    Returns:
        dict con claves 'permanentes' y 'transitorios', cada una con el índice
        de columna correspondiente o None si no se encuentra.
    """
    result = {"permanentes": None, "transitorios": None}

    # Buscar en todas las filas del header (primeras 15 filas)
    for row_idx in range(min(15, len(df))):
        for col_idx in range(len(df.columns)):
            cell = str(df.iloc[row_idx, col_idx]).lower()
            if re.search(r"permanente", cell) and re.search(r"(cultivo|agr[ií]cola|[áa]rea)", cell):
                if result["permanentes"] is None:
                    result["permanentes"] = col_idx
                    logger.info("CNA: columna permanentes detectada en fila %s, col %s: '%s'",
                                row_idx, col_idx, df.iloc[row_idx, col_idx])
            if re.search(r"transitorio", cell) and re.search(r"(cultivo|agr[ií]cola|[áa]rea)", cell):
                if result["transitorios"] is None:
                    result["transitorios"] = col_idx
                    logger.info("CNA: columna transitorios detectada en fila %s, col %s: '%s'",
                                row_idx, col_idx, df.iloc[row_idx, col_idx])

    # También buscar en los encabezados de columna si son strings
    for col_idx, col_name in enumerate(df.columns):
        col_str = str(col_name).lower()
        if "permanente" in col_str and result["permanentes"] is None:
            result["permanentes"] = col_idx
        if "transitorio" in col_str and result["transitorios"] is None:
            result["transitorios"] = col_idx

    return result


def extract_cna() -> pd.DataFrame:
    """
    Descarga anexos municipales del Censo Nacional Agropecuario 2014 desde DANE.

    Corrección 1.1: Lee columnas reales de desglose por tipo de cultivo.
    NO usa coeficientes proxy. Si las columnas no están disponibles,
    deja np.nan y marca area_cultivo_fuente='no_disponible'.

    El campo area_cultivo_fuente siempre está presente:
      - 'real_cna': datos leídos directamente del Excel del CNA.
      - 'no_disponible': columnas no encontradas, valores son np.nan.
    """
    logger.info("CNA: iniciando extracción automatizada (DANE)...")

    url_uso_suelo = "https://www.dane.gov.co/files/CensoAgropecuario/entrega-definitiva/Boletin-1-Uso-del-suelo/1-Anexos-municipales.xls"

    out_dir = DATA_RAW / "cna"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "cna_raw.parquet"

    try:
        # Intentar descarga con reintentos para SSL
        resp = None
        for intento in range(2):
            try:
                resp = requests.get(url_uso_suelo, timeout=120, verify=True)
                resp.raise_for_status()
                break
            except requests.exceptions.SSLError:
                logger.warning("CNA: error SSL, reintentando con verify=False")
                resp = requests.get(url_uso_suelo, timeout=120, verify=False)
                resp.raise_for_status()
                break
            except requests.exceptions.Timeout:
                logger.warning("CNA: timeout en intento %s", intento + 1)

        if not resp:
            raise ConnectionError("No se pudo descargar el archivo de CNA")

        # Cargar Excel (Cuadro 1 - Uso del Suelo suele estar en sheet_name=2)
        df_uso = pd.read_excel(io.BytesIO(resp.content), sheet_name=2, header=None, skiprows=10)

        # Mapeo correcto según estructura DANE:
        # 3: Cod Municipio, 4: Municipio, 5: Area Agricola, 6: Siguiente...
        df_uso_base = df_uso[[3, 4, 5, 6]].copy()
        df_uso_base.columns = ["id_municipio", "nombre_municipio", "area_agropecuaria_ha", "area_no_agropecuaria_ha"]

        # Limpieza y validación de DIVIPOLA
        df_uso_base = df_uso_base.dropna(subset=["id_municipio"])
        df_uso_base["id_municipio"] = pd.to_numeric(df_uso_base["id_municipio"], errors="coerce").astype("Int64").astype(str).str.zfill(5)
        df_uso_base = df_uso_base[df_uso_base["id_municipio"] != "<NA>"]

        # Validación de formato DIVIPOLA
        if not df_uso_base.empty:
            sample = df_uso_base["id_municipio"].head(5).tolist()
            assert all(str(v).isdigit() and len(str(v)) in (4, 5) for v in sample), \
                f"CNA: id_municipio no parece un código DIVIPOLA válido: {sample}"

        df_uso_base["anio_censo"] = 2014

        # --- Corrección 1.1: Leer columnas reales de cultivos ---
        # Intentar detectar columnas de cultivos permanentes y transitorios
        # en el Excel original (no recortado)
        cols_cultivo = _detectar_columnas_cultivo(df_uso)

        if cols_cultivo["permanentes"] is not None and cols_cultivo["transitorios"] is not None:
            # Extraer las columnas reales del CNA
            col_perm = cols_cultivo["permanentes"]
            col_trans = cols_cultivo["transitorios"]

            df_uso_base["area_cultivos_permanentes_ha"] = pd.to_numeric(
                df_uso.iloc[:, col_perm], errors="coerce"
            ).reindex(df_uso_base.index)
            df_uso_base["area_cultivos_transitorios_ha"] = pd.to_numeric(
                df_uso.iloc[:, col_trans], errors="coerce"
            ).reindex(df_uso_base.index)
            df_uso_base["area_cultivo_fuente"] = "real_cna"

            logger.info(
                "CNA: area_cultivos_permanentes_ha y area_cultivos_transitorios_ha "
                "leídas directamente del Excel del CNA (columnas %s y %s).",
                col_perm, col_trans
            )
        else:
            # No se encontraron las columnas de desglose
            df_uso_base["area_cultivos_permanentes_ha"] = np.nan
            df_uso_base["area_cultivos_transitorios_ha"] = np.nan
            df_uso_base["area_cultivo_fuente"] = "no_disponible"

            logger.warning(
                "CNA: No se encontraron las columnas de desglose por tipo de cultivo "
                "(permanentes/transitorios) en el Excel del CNA. "
                "Los campos area_cultivos_permanentes_ha y area_cultivos_transitorios_ha "
                "se dejan como NaN. NO se usan coeficientes proxy."
            )

        df_uso_base.to_parquet(out_file, index=False)
        logger.info("CNA 2014: %s municipios consolidados -> %s", len(df_uso_base), out_file)

        return df_uso_base

    except requests.exceptions.Timeout:
        logger.error("CNA: error de red (timeout) al descargar desde DANE")
        return pd.DataFrame()
    except Exception as e:
        logger.error("CNA: error inesperado procesando Excel: %s", e)
        return pd.DataFrame()
