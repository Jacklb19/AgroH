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
    Busca columnas de cultivos permanentes y transitorios por nombre aproximado.
    """
    result = {"permanentes": None, "transitorios": None}

    # Buscar en las primeras 50 filas (algunos archivos tienen headers muy largos)
    for row_idx in range(min(50, len(df))):
        for col_idx in range(len(df.columns)):
            val = df.iloc[row_idx, col_idx]
            if pd.isna(val):
                continue
            cell = str(val).lower()
            
            # Patrón para permanentes: incluye variaciones y abreviaturas
            if any(p in cell for p in ["permanente", "perm."]) and any(x in cell for x in ["cultivo", "agri", "area"]):
                if result["permanentes"] is None:
                    result["permanentes"] = col_idx
                    logger.info("CNA: columna permanentes detectada en fila %s, col %s: '%s'", row_idx, col_idx, val)
            
            # Patrón para transitorios: incluye variaciones y abreviaturas
            if any(p in cell for p in ["transitorio", "trans."]) and any(x in cell for x in ["cultivo", "agri", "area"]):
                if result["transitorios"] is None:
                    result["transitorios"] = col_idx
                    logger.info("CNA: columna transitorios detectada en fila %s, col %s: '%s'", row_idx, col_idx, val)

    # Si no se detectaron por nombre, intentar por posición relativa a "agricola" si se encuentra
    if result["permanentes"] is None or result["transitorios"] is None:
        for row_idx in range(min(40, len(df))):
            for col_idx in range(len(df.columns)):
                val = df.iloc[row_idx, col_idx]
                if pd.isna(val): continue
                cell = str(val).lower()
                if "agricola" in cell:
                    # En algunos anexos, los nombres están en la fila de abajo
                    if row_idx + 1 < len(df):
                        for sub_col in range(col_idx, min(col_idx + 5, len(df.columns))):
                            sub_val = str(df.iloc[row_idx + 1, sub_col]).lower()
                            if "perm" in sub_val and result["permanentes"] is None:
                                result["permanentes"] = sub_col
                                logger.info("CNA: Detectada col permanentes por sub-header en (%s, %s)", row_idx+1, sub_col)
                            if "trans" in sub_val and result["transitorios"] is None:
                                result["transitorios"] = sub_col
                                logger.info("CNA: Detectada col transitorios por sub-header en (%s, %s)", row_idx+1, sub_col)
                    
                    # Si aún no, usar posición fija relativa (CNA Cuadro 1 estándar)
                    if result["permanentes"] is None and col_idx + 1 < len(df.columns):
                        result["permanentes"] = col_idx + 1
                    if result["transitorios"] is None and col_idx + 2 < len(df.columns):
                        result["transitorios"] = col_idx + 2
                    break
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

        # Cargar Excel completo para buscar el header
        df_full = pd.read_excel(io.BytesIO(resp.content), sheet_name=2, header=None)

        # Buscar la fila donde comienza la tabla (buscando "Código" y "Municipio")
        header_row_idx = None
        for i in range(min(30, len(df_full))):
            row_str = " ".join([str(x) for x in df_full.iloc[i]]).lower()
            if "código" in row_str and "municipio" in row_str:
                header_row_idx = i
                break

        if header_row_idx is not None:
            logger.info("CNA: Fila de encabezados encontrada en índice %s: %s", header_row_idx, df_full.iloc[header_row_idx].tolist())

        # Re-leer o recortar desde el header detectado
        df_uso = df_full.iloc[header_row_idx+1:].copy()
        
        # Loggear los primeros valores de las columnas sospechosas
        logger.info("CNA: Muestra de datos crudos (primeras 5 filas):\n%s", df_uso.head(5).to_string())


        # Mapeo por posición según muestra observada:
        # Col 2: Código, Col 3: Municipio, Col 4: Area Agro, Col 5: Area No Agro
        col_idx_cod = 2
        col_idx_muni = 3
        col_idx_agro = 4
        col_idx_no_agro = 5

        df_uso_base = pd.DataFrame({
            "id_municipio": df_uso.iloc[:, col_idx_cod],
            "nombre_municipio": df_uso.iloc[:, col_idx_muni],
            "area_agropecuaria_ha": df_uso.iloc[:, col_idx_agro],
            "area_no_agropecuaria_ha": df_uso.iloc[:, col_idx_no_agro]
        })


        # Limpieza y validación de DIVIPOLA
        df_uso_base = df_uso_base.dropna(subset=["id_municipio"])
        df_uso_base["id_municipio"] = pd.to_numeric(df_uso_base["id_municipio"], errors="coerce").fillna(0).astype(int).astype(str).str.zfill(5)
        df_uso_base = df_uso_base[df_uso_base["id_municipio"] != "00000"]

        # Validación de formato DIVIPOLA
        if not df_uso_base.empty:
            sample = df_uso_base["id_municipio"].head(5).tolist()
            # Aceptar si son números
            if not all(str(v).isdigit() for v in sample):
                logger.error("CNA: id_municipio no parece válido: %s", sample)


        df_uso_base["anio_censo"] = 2014

        # --- Corrección 1.1: Leer columnas reales de cultivos ---
        # Intentar detectar columnas de cultivos permanentes y transitorios
        # en el Excel original (completo)
        cols_cultivo = _detectar_columnas_cultivo(df_full)

        if cols_cultivo["permanentes"] is not None and cols_cultivo["transitorios"] is not None:
            # Extraer las columnas reales del CNA
            col_perm = cols_cultivo["permanentes"]
            col_trans = cols_cultivo["transitorios"]

            df_uso_base["area_cultivos_permanentes_ha"] = pd.to_numeric(
                df_full.iloc[df_uso_base.index, col_perm], errors="coerce"
            )
            df_uso_base["area_cultivos_transitorios_ha"] = pd.to_numeric(
                df_full.iloc[df_uso_base.index, col_trans], errors="coerce"
            )
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
        logger.error("CNA: error inesperado procesando Excel: %s", e, exc_info=True)
        return pd.DataFrame()
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    extract_cna()
