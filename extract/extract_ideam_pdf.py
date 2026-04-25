import pdfplumber
import pandas as pd
import requests
import re
import logging
from pathlib import Path
from config.settings import DATA_RAW, ENSO_BOLETIN_URLS

logger = logging.getLogger(__name__)

LISTING_URL = "https://www.ideam.gov.co/sala-de-prensa/boletines/Bolet%C3%ADn-agroclim%C3%A1tico-nacional"

def discover_bulletin_urls(max_pages: int = 5) -> list[str]:
    """Descubre automáticamente las URLs de los boletines desde el sitio de IDEAM."""
    urls = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    logger.info("Descubriendo boletines en el sitio de IDEAM...")
    
    for page in range(max_pages):
        try:
            page_url = f"{LISTING_URL}?page={page}"
            r = requests.get(page_url, headers=headers, timeout=30, verify=False)
            r.raise_for_status()
            
            # Buscar enlaces de descarga: /file-download/download/public/XXXXX o con dominio
            pattern_file = r'(/file-download/download/public/\d+|https?://www.ideam.gov.co/file-download/download/public/\d+)'
            pattern_sites = r'(/sites/default/files/prensa/boletines/[^"\'>\s]+\.pdf|https?://www.ideam.gov.co/sites/default/files/prensa/boletines/[^"\'>\s]+\.pdf)'
            
            matches = re.findall(pattern_file, r.text) + re.findall(pattern_sites, r.text)
            
            # Convertir a absolutas si son relativas
            new_urls = []
            for m in set(matches):
                if m.startswith("/"):
                    new_urls.append(f"https://www.ideam.gov.co{m}")
                else:
                    new_urls.append(m)
            
            urls.extend(new_urls)
            logger.info(f"  Página {page+1}: {len(new_urls)} URLs encontradas")
            
            if not new_urls: break # No hay más resultados
            
        except Exception as e:
            logger.error(f"Error descubriendo URLs en página {page}: {e}")
            break
            
    return list(set(urls))

REGIONES = ["Andina", "Caribe", "Pacífico", "Orinoquía", "Amazonía"]

def _parse_fase_enso(texto: str) -> str:
    texto = texto.lower()
    if "niño" in texto:  return "El Niño"
    if "niña" in texto:  return "La Niña"
    return "Neutro"

def _extract_spi(texto: str) -> float | None:
    """Busca un valor de SPI (decimal entre -3 y 3) en el texto."""
    # Buscar patrones como "SPI: -1.2" o "índice de -0.5"
    match = re.search(r"[-+]?[0-2]\.\d+", texto)
    if match:
        return float(match.group())
    return None

def extract_boletin_pdf(pdf_path: Path, trimestre: str, anio: int) -> pd.DataFrame:
    """Extrae tabla de alertas ENSO de un PDF de boletín agroclimático IDEAM."""
    registros = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                # 1. Intentar con tablas estructuradas
                tables = page.extract_tables()
                found_in_page = False
                for table in tables:
                    for row in table:
                        if not row: continue
                        row_text = " ".join([str(c) for c in row if c])
                        for region in REGIONES:
                            if region.lower() in row_text.lower():
                                spi = _extract_spi(row_text)
                                registros.append({
                                    "trimestre": trimestre,
                                    "anio": anio,
                                    "region": region,
                                    "fase_enso": _parse_fase_enso(row_text),
                                    "indice_spi": spi,
                                    "texto_raw": row_text[:300],
                                })
                                found_in_page = True
                
                # 2. Fallback a texto libre si no se detectaron regiones en tablas
                if not found_in_page:
                    text = page.extract_text()
                    if text:
                        for line in text.split("\n"):
                            for region in REGIONES:
                                if region.lower() in line.lower():
                                    spi = _extract_spi(line)
                                    registros.append({
                                        "trimestre": trimestre,
                                        "anio": anio,
                                        "region": region,
                                        "fase_enso": _parse_fase_enso(line),
                                        "indice_spi": spi,
                                        "texto_raw": line[:300],
                                    })
    except Exception as e:
        logger.error(f"Error procesando {pdf_path.name}: {e}")
        
    return pd.DataFrame(registros).drop_duplicates(subset=["region", "anio", "trimestre"], keep="first") if registros else pd.DataFrame()


def _infer_periodo(filename: str) -> tuple[int, str]:
    # Intentar sacar año y mes/trimestre del nombre del archivo
    lower_name = filename.lower()
    
    # Buscar año
    anio_match = re.search(r"(20\d{2})", lower_name)
    anio = int(anio_match.group(1)) if anio_match else 0
    
    # Buscar trimestre o mes
    if "enero" in lower_name or "febrero" in lower_name or "marzo" in lower_name or "t1" in lower_name:
        trimestre = "T1"
    elif "abril" in lower_name or "mayo" in lower_name or "junio" in lower_name or "t2" in lower_name:
        trimestre = "T2"
    elif "julio" in lower_name or "agosto" in lower_name or "septiembre" in lower_name or "t3" in lower_name:
        trimestre = "T3"
    elif "octubre" in lower_name or "noviembre" in lower_name or "diciembre" in lower_name or "t4" in lower_name:
        trimestre = "T4"
    else:
        trimestre = "T0"
        
    return anio, trimestre

def extract_all_boletines() -> pd.DataFrame:
    """Descarga y procesa todos los PDFs configurados."""
    pdf_dir = DATA_RAW / "boletines_pdf"
    pdf_dir.mkdir(exist_ok=True)
    all_dfs = []

    # Descubrimiento automático + URLs manuales de .env
    urls = discover_bulletin_urls()
    if ENSO_BOLETIN_URLS:
        urls = list(set(urls + ENSO_BOLETIN_URLS))
        
    headers = {"User-Agent": "Mozilla/5.0"}
    
    for url in urls:
        try:
            # Hacemos GET con stream=True para leer los headers antes de descargar todo si ya existe
            r = requests.get(url, headers=headers, timeout=60, verify=False, stream=True)
            r.raise_for_status()
            
            cd = r.headers.get("Content-Disposition", "")
            if "filename=" in cd:
                filename = cd.split("filename=")[-1].strip('"\'')
            else:
                filename = url.rstrip("/").split("/")[-1]
                
            if not filename.endswith(".pdf"): filename += ".pdf"
            local = pdf_dir / filename
            
            if not local.exists():
                logger.info(f"Descargando {filename}...")
                content = r.content
                # Verificar que sea un PDF real
                if b"%PDF" not in content[:100]:
                    logger.warning(f"  {filename} no parece ser un PDF válido, ignorando...")
                    continue
                local.write_bytes(content)
            else:
                # Ya existe, cerramos la conexión
                r.close()
            
            anio, trimestre = _infer_periodo(filename)
            df = extract_boletin_pdf(local, trimestre, anio)
            if not df.empty:
                all_dfs.append(df)
        except Exception as e:
            logger.error(f"Error con URL {url}: {e}")

    # También procesar archivos locales que ya existan
    for local in sorted(pdf_dir.glob("*.pdf")):
        if any(local.name in url for url in urls): continue
        anio, trimestre = _infer_periodo(local.name)
        df = extract_boletin_pdf(local, trimestre, anio)
        if not df.empty:
            all_dfs.append(df)

    result = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
    
    # Estandarizar columnas para que coincidan con fact_alerta_enso
    if not result.empty:
        cols_finales = ["trimestre", "anio", "region", "fase_enso", "indice_spi", "texto_raw"]
        for c in ["anomalia_precipitacion_pct", "probabilidad_deficit_hidrico", "probabilidad_exceso_hidrico"]:
            result[c] = None
            cols_finales.append(c)
        result = result[cols_finales]

    out = DATA_RAW / "enso_boletines_raw.csv"
    result.to_csv(out, index=False)
    logger.info(f"A10 boletines: {len(result)} registros -> {out}")
    return result

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    extract_all_boletines()

