import logging
import re
import time
import unicodedata
import requests
import urllib3
import pandas as pd
from pathlib import Path
from config.settings import DATA_RAW

# Desactivar advertencias de SSL si es necesario
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

SIPSA_LINK_PATTERNS = [
    'anex-SIPSADiario', 'anexo-sipsa-diario', 
    'SIPSADiario', 'sipsa_diario', 'sipsa-diario'
]

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

def extract_sipsa() -> pd.DataFrame:
    """
    Automatización: Descarga el último anexo de precios diarios mayoristas del SIPSA (DANE)
    y lo transforma de una tabla cruzada a formato tabular plano.
    # FIX v1: Búsqueda dinámica de filas, reintentos robustos, selectores flexibles y output path.
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
                    return pd.DataFrame()
        except requests.exceptions.Timeout:
            logger.warning("SIPSA: timeout intento %s/3", intento + 1)
            time.sleep(5)
        except Exception as e:
            logger.error("SIPSA: error inesperado: %s", e)
            return pd.DataFrame()

    if not r:
        return pd.DataFrame()

    try:
        links = re.findall(r'href=[\'"]?([^\'" >]+\.xlsx?)', r.text)
        daily_links = [
            l for l in set(links) 
            if any(p.lower() in l.lower() for p in SIPSA_LINK_PATTERNS)
            and l.endswith(('.xlsx', '.xls'))
        ]
        
        if not daily_links:
            logger.warning("SIPSA: no se encontraron links de SIPSA Diario.")
            return pd.DataFrame()
            
        daily_links.sort(reverse=True)
        url_file = daily_links[0]
        if not url_file.startswith('http'):
            url_file = "https://www.dane.gov.co" + url_file
        
        logger.info("SIPSA: descargando %s", url_file)
        df_raw = pd.read_excel(url_file, header=None)
        
        idx_fecha = _find_fecha_row(df_raw)
        fecha_texto = str(df_raw.iloc[idx_fecha, 0])
        fecha_iso = _parse_spanish_date(fecha_texto)
        
        idx_ciudades = _find_ciudades_row(df_raw, idx_fecha)
        
        ciudades = {}
        for col in range(1, len(df_raw.columns)):
            ciudad = str(df_raw.iloc[idx_ciudades, col]).strip()
            if ciudad not in ('nan', '', 'None'):
                ciudades[col] = ciudad
                
        records = []
        # Los datos empiezan después de la fila de ciudades
        for idx in range(idx_ciudades + 1, len(df_raw)):
            producto = str(df_raw.iloc[idx, 0]).strip()
            if pd.isna(df_raw.iloc[idx, 1]) or producto in ('nan', '', 'None') or 'Fuente:' in producto:
                continue
            
            for col, ciudad in ciudades.items():
                if col >= len(df_raw.columns): continue
                precio = df_raw.iloc[idx, col]
                if pd.notna(precio) and str(precio).strip().lower() not in ('n.d.', 'nan', ''):
                    # Limpiar caracteres
                    prod_limpio = unicodedata.normalize("NFKD", producto).encode("ASCII", "ignore").decode("utf-8")
                    ciu_limpia = unicodedata.normalize("NFKD", ciudad).encode("ASCII", "ignore").decode("utf-8")
                    central_limpia = " ".join(ciu_limpia.replace("\r", " ").replace("\n", " ").split())
                    ciudad_base = central_limpia.split(',')[0].strip()
                    
                    records.append({
                        'fecha_registro': fecha_iso,
                        'producto': prod_limpio,
                        'central': central_limpia,
                        'ciudad': ciudad_base,
                        'precio_promedio_cop_kg': precio
                    })
                    
        df_flat = pd.DataFrame(records)
        
        if not df_flat.empty:
            df_flat.to_csv(out_file, index=False)
            logger.info("SIPSA: %s registros extraídos -> %s", len(df_flat), out_file)
            
        return df_flat
        
    except Exception as e:
        logger.error("SIPSA: error procesando boletín: %s", e)
        return pd.DataFrame()

