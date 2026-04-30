"""
extract_sipsa.py — Extracción de precios mayoristas SIPSA del DANE.

Correcciones aplicadas:
  - Corrección 1.4 (2026-04-29): Cache y fallback para scraping frágil.
    Guarda sipsa_last_success.parquet en cache. Fallback a último éxito si
    el scraping falla. Regex ampliado con re.IGNORECASE. Agregado campo
    fecha_archivo_sipsa.
"""
import logging
import re
import time
import unicodedata
from datetime import date, datetime

import pandas as pd
import requests
import urllib3

from config.settings import DATA_RAW

# Desactivar advertencias de SSL si es necesario
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

# Patrones ampliados para búsqueda de links (Corrección 1.4)
_SIPSA_LINK_RE = re.compile(
    r'anex[-_]?SIPSADiario|SIPSADiario|SIPSA[-_]Diario|sipsa[-_]diario',
    re.IGNORECASE
)


def _parse_spanish_date(date_str: str) -> str:
    """Convierte 'Viernes 24 de abril de 2026' a '2026-04-24'"""
    meses = {
        "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
        "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
        "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12"
    }
    match = re.search(r'(\d{1,2})\s+de\s+([a-zA-Z]+)\s+de\s+(\d{4})', date_str, re.IGNORECASE)
    if match:
        dia = match.group(1).zfill(2)
        mes = meses.get(match.group(2).lower(), "01")
        anio = match.group(3)
        return f"{anio}-{mes}-{dia}"
    return date_str

def _find_fecha_row(df_raw):
    """Busca la fila que contiene la fecha del boletín."""
    for i in range(min(15, len(df_raw))):
        cell = str(df_raw.iloc[i, 0])
        if re.search(r'\d{1,2}\s+de\s+[a-zA-Z]+\s+de\s+\d{4}', cell, re.IGNORECASE):
            return i
    return 1  # fallback al índice original

def _find_ciudades_row(df_raw, after_row):
    """Busca la primera fila con múltiples celdas no-nulas después de la fecha."""
    for i in range(after_row + 1, min(after_row + 10, len(df_raw))):
        non_null = sum(1 for j in range(1, len(df_raw.columns))
                      if str(df_raw.iloc[i, j]).strip() not in ('nan', '', 'None'))
        if non_null >= 3:
            return i
    return after_row + 1  # fallback


def _get_cache_path():
    """Retorna la ruta del archivo de cache."""
    cache_dir = DATA_RAW / "sipsa"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / "sipsa_last_success.parquet"


def _load_cache() -> pd.DataFrame:
    """Carga el archivo de último éxito si existe."""
    cache_path = _get_cache_path()
    if cache_path.exists():
        df = pd.read_parquet(cache_path)
        if not df.empty:
            fecha_cache = "desconocida"
            if "fecha_archivo_sipsa" in df.columns:
                fecha_cache = str(df["fecha_archivo_sipsa"].iloc[0])
            logger.warning(
                "SIPSA: Usando datos cacheados del último éxito (fecha archivo: %s). "
                "El scraping actual falló.", fecha_cache
            )
            return df
    return pd.DataFrame()


def _save_cache(df: pd.DataFrame):
    """Guarda el resultado exitoso en el archivo de cache."""
    cache_path = _get_cache_path()
    try:
        df.to_parquet(cache_path, index=False)
        logger.info("SIPSA: Cache actualizado -> %s", cache_path)
    except Exception as e:
        logger.warning("SIPSA: Error guardando cache: %s", e)


def extract_sipsa() -> pd.DataFrame:
    """
    Automatización: Descarga el último anexo de precios diarios mayoristas del SIPSA (DANE)
    y lo transforma de una tabla cruzada a formato tabular plano.

    Corrección 1.4: Cache y fallback para scraping frágil.
    - Verifica sipsa_last_success.parquet antes de retornar vacío.
    - Regex ampliado con re.IGNORECASE.
    - Guarda resultado exitoso en cache.
    - Agrega campo fecha_archivo_sipsa.
    """
    logger.info("SIPSA: iniciando búsqueda de boletín en DANE...")
    url_base = 'https://www.dane.gov.co/index.php/estadisticas-por-tema/agropecuario/sistema-de-informacion-de-precios-sipsa/componente-precios-mayoristas'

    out_dir = DATA_RAW / "sipsa"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "sipsa_raw_consolidado.csv"

    r = None
    for intento in range(3):
        try:
            r = requests.get(url_base, timeout=60, verify=True)
            r.raise_for_status()
            break
        except requests.exceptions.SSLError:
            if intento == 0:
                logger.warning("SIPSA: SSL error, reintentando con verify=False")
                try:
                    r = requests.get(url_base, timeout=60, verify=False)
                    r.raise_for_status()
                    break
                except Exception as e2:
                    logger.error("SIPSA: fallo con verify=False: %s", e2)
                    return _load_cache()
        except requests.exceptions.Timeout:
            logger.warning("SIPSA: timeout intento %s/3", intento + 1)
            time.sleep(5)
        except Exception as e:
            logger.error("SIPSA: error inesperado: %s", e)
            return _load_cache()

    if not r:
        return _load_cache()

    try:
        links = re.findall(r'href=[\'"]?([^\'" >]+\.xlsx?)', r.text)
        # Usar regex ampliado con IGNORECASE (Corrección 1.4)
        daily_links = [
            l for l in set(links)
            if _SIPSA_LINK_RE.search(l)
            and l.endswith(('.xlsx', '.xls'))
        ]

        if not daily_links:
            logger.warning("SIPSA: no se encontraron links de SIPSA Diario.")
            return _load_cache()

        daily_links.sort(reverse=True)
        url_file = daily_links[0]
        if not url_file.startswith('http'):
            url_file = "https://www.dane.gov.co" + url_file

        logger.info("SIPSA: descargando %s", url_file)
        df_raw = pd.read_excel(url_file, header=None)

        idx_fecha = _find_fecha_row(df_raw)
        fecha_texto = str(df_raw.iloc[idx_fecha, 0])
        fecha_iso = _parse_spanish_date(fecha_texto)

        # Determinar fecha del archivo para el campo fecha_archivo_sipsa
        try:
            fecha_archivo = pd.to_datetime(fecha_iso).date()
        except Exception:
            fecha_archivo = date.today()

        idx_ciudades = _find_ciudades_row(df_raw, idx_fecha)

        ciudades = {}
        # Mapeo: {col_promedio: {'ciudad': name, 'col_min': idx, 'col_max': idx}}
        
        # En el boletín diario del DANE, las ciudades suelen ser celdas combinadas de 3 columnas
        # Col A: Producto | Col B: Min, Col C: Max, Col D: Prom (para Ciudad 1)
        for col in range(1, len(df_raw.columns), 3):
            ciudad = str(df_raw.iloc[idx_ciudades, col]).strip()
            if ciudad not in ('nan', '', 'None'):
                # Asumimos el patrón: Min, Max, Promedio en columnas consecutivas
                ciudades[col + 2] = {
                    'nombre': ciudad,
                    'col_min': col,
                    'col_max': col + 1
                }

        records = []
        for idx in range(idx_ciudades + 2, len(df_raw)):
            producto = str(df_raw.iloc[idx, 0]).strip()
            if pd.isna(df_raw.iloc[idx, 1]) or producto in ('nan', '', 'None') or 'Fuente:' in producto:
                continue

            for col_prom, info in ciudades.items():
                if col_prom >= len(df_raw.columns): continue
                
                precio_prom = df_raw.iloc[idx, col_prom]
                precio_min = df_raw.iloc[idx, info['col_min']]
                precio_max = df_raw.iloc[idx, info['col_max']]
                
                if pd.notna(precio_prom) and str(precio_prom).strip().lower() not in ('n.d.', 'nan', ''):
                    prod_limpio = unicodedata.normalize("NFKD", producto).encode("ASCII", "ignore").decode("utf-8")
                    ciu_limpia = unicodedata.normalize("NFKD", info['nombre']).encode("ASCII", "ignore").decode("utf-8")
                    central_limpia = " ".join(ciu_limpia.replace("\r", " ").replace("\n", " ").split())
                    ciudad_base = central_limpia.split(',')[0].strip()

                    records.append({
                        'fecha_registro': fecha_iso,
                        'producto': prod_limpio,
                        'central': central_limpia,
                        'ciudad': ciudad_base,
                        'precio_min_cop_kg': precio_min,
                        'precio_max_cop_kg': precio_max,
                        'precio_promedio_cop_kg': precio_prom,
                        'fecha_archivo_sipsa': fecha_archivo,
                    })

        df_flat = pd.DataFrame(records)

        if not df_flat.empty:
            df_flat.to_csv(out_file, index=False)
            logger.info("SIPSA: %s registros extraídos -> %s", len(df_flat), out_file)
            # Guardar cache de último éxito (Corrección 1.4)
            _save_cache(df_flat)
        else:
            logger.warning("SIPSA: DataFrame extraído vacío, intentando cache.")
            return _load_cache()

        return df_flat

    except Exception as e:
        logger.error("SIPSA: error procesando boletín: %s", e)
        return _load_cache()
