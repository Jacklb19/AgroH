import pdfplumber
import pandas as pd
import requests
import re
import logging
from pathlib import Path
from config.settings import DATA_RAW, ENSO_BOLETIN_URLS

logger = logging.getLogger(__name__)

BOLETIN_URLS = ENSO_BOLETIN_URLS

REGIONES = ["Andina", "Caribe", "Pacífico", "Orinoquía", "Amazonía"]

# Palabras clave para identificar boletines ENSO en la página del IDEAM
_ENSO_KEYWORDS = {"enso", "agroclim", "climatico", "climatica", "boletin_agro"}

_IDEAM_BASE      = "https://www.ideam.gov.co"
_IDEAM_BOLETINES = "https://www.ideam.gov.co/nuestra-entidad/meteorologia/boletines"
# Máximo de páginas a escanear (cada página tiene ~10 boletines)
_MAX_PAGES = 5


def _scrape_enso_urls() -> list[str]:
    """
    Escanea las primeras _MAX_PAGES páginas de boletines del IDEAM y
    devuelve las URLs absolutas de los PDFs que contengan palabras clave ENSO.
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.warning("beautifulsoup4 no instalado — scraping desactivado. Corré: pip install beautifulsoup4")
        return []

    found: list[str] = []
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (AgroIA/1.0)"})

    for page in range(_MAX_PAGES):
        url = f"{_IDEAM_BOLETINES}?page={page}"
        try:
            resp = session.get(url, timeout=20)
            resp.raise_for_status()
        except Exception as exc:
            logger.warning("IDEAM scraping: no se pudo acceder a página %s: %s", page, exc)
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        links = soup.find_all("a", href=re.compile(r"/file-download/download/public/\d+"))

        page_hits = 0
        for a in links:
            filename = (a.get("title") or a.get_text(strip=True)).lower()
            if any(kw in filename for kw in _ENSO_KEYWORDS):
                full_url = _IDEAM_BASE + a["href"]
                if full_url not in found:
                    found.append(full_url)
                    page_hits += 1

        logger.debug("IDEAM scraping página %s: %s boletines ENSO encontrados", page, page_hits)

        # Si ninguna página reciente tiene boletines ENSO dejamos de paginar
        if page > 1 and page_hits == 0:
            break

    logger.info("IDEAM scraping: %s URLs de boletines ENSO descubiertas", len(found))
    return found


def _parse_fase_enso(texto: str) -> str:
    texto = texto.lower()
    if "niño" in texto:  return "El Niño"
    if "niña" in texto:  return "La Niña"
    return "Neutro"


def extract_boletin_pdf(pdf_path: Path, trimestre: str, anio: int) -> pd.DataFrame:
    """Extrae tabla de alertas ENSO de un PDF de boletín agroclimático IDEAM."""
    registros = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    if not row:
                        continue
                    row_text = " ".join([str(c) for c in row if c])
                    for region in REGIONES:
                        if region.lower() in row_text.lower():
                            spi_match = re.search(r"[-+]?\d+\.\d+", row_text)
                            spi = float(spi_match.group()) if spi_match else None
                            registros.append({
                                "trimestre":   trimestre,
                                "anio":        anio,
                                "region":      region,
                                "fase_enso":   _parse_fase_enso(row_text),
                                "indice_spi":  spi,
                                "anomalia_precipitacion_pct": None,
                                "probabilidad_deficit_hidrico": None,
                                "probabilidad_exceso_hidrico": None,
                                "texto_raw":   row_text[:300],
                            })
    return pd.DataFrame(registros)


_MES_A_TRIMESTRE = {
    # abreviaciones 3 letras
    "ene": "T1", "jan": "T1", "feb": "T1", "mar": "T1",
    "abr": "T2", "apr": "T2", "may": "T2", "jun": "T2",
    "jul": "T3", "ago": "T3", "aug": "T3", "sep": "T3",
    "oct": "T4", "nov": "T4", "dic": "T4", "dec": "T4",
    # nombres completos en español
    "enero": "T1", "febrero": "T1", "marzo": "T1",
    "abril": "T2", "mayo": "T2", "junio": "T2",
    "julio": "T3", "agosto": "T3", "septiembre": "T3",
    "octubre": "T4", "noviembre": "T4", "diciembre": "T4",
}

# Ordenados de más largo a más corto para que "febrero" gane sobre "feb"
_MES_PATTERN = "|".join(sorted(_MES_A_TRIMESTRE.keys(), key=len, reverse=True))


def _infer_periodo(filename: str) -> tuple[int, str]:
    lower_name = filename.lower()

    # Patrón explícito trimestre: 2025_T3 o T3_2025
    for pattern in [
        r"(?P<anio>\d{4})[_-]?[tT](?P<trimestre>[1-4])",
        r"(?P<anio>\d{4}).*trimestre[_ -]?(?P<trimestre>[1-4])",
        r"trimestre[_ -]?(?P<trimestre>[1-4]).*(?P<anio>\d{4})",
    ]:
        m = re.search(pattern, lower_name, re.IGNORECASE)
        if m:
            return int(m.group("anio")), f"T{m.group('trimestre')}"

    # Patrón con nombre de mes + año: "marzo_2026", "abr_24_2026", "feb2026"
    # Busca mes seguido (opcionalmente por día) por año de 4 dígitos
    m = re.search(
        rf"({_MES_PATTERN})[_\-]?\d{{0,2}}[_\-]?(\d{{4}})",
        lower_name,
    )
    if m:
        mes, anio_str = m.group(1), m.group(2)
        trimestre = _MES_A_TRIMESTRE.get(mes)
        if trimestre:
            return int(anio_str), trimestre

    # Patrón año + mes: "2026_mar"
    m = re.search(
        rf"(\d{{4}})[_\-]?({_MES_PATTERN})",
        lower_name,
    )
    if m:
        anio_str, mes = m.group(1), m.group(2)
        trimestre = _MES_A_TRIMESTRE.get(mes)
        if trimestre:
            return int(anio_str), trimestre

    logger.warning("No se pudo inferir periodo de '%s' — asignando T0/0", filename)
    return 0, "T0"


def _download_pdf(url: str, pdf_dir: Path) -> Path | None:
    """
    Descarga un PDF desde una URL del IDEAM.
    Detecta el nombre real del archivo desde el header Content-Disposition o la URL.
    Retorna la ruta local o None si falla.
    """
    try:
        resp = requests.get(url, timeout=60, stream=True)
        resp.raise_for_status()

        # Intentar obtener nombre real desde Content-Disposition
        cd = resp.headers.get("Content-Disposition", "")
        name_match = re.search(r'filename[^;=\n]*=(["\']?)([^"\'\n;]+)\1', cd)
        if name_match:
            filename = name_match.group(2).strip()
        else:
            # Usar el ID numérico de la URL como nombre de archivo
            filename = url.rstrip("/").split("/")[-1]
            if not filename.endswith(".pdf"):
                filename += ".pdf"

        local = pdf_dir / filename
        if local.exists():
            return local

        local.write_bytes(resp.content)
        logger.info("Descargado: %s -> %s", url, filename)
        return local

    except Exception as exc:
        logger.warning("No se pudo descargar %s: %s", url, exc)
        return None


def extract_all_boletines() -> pd.DataFrame:
    """
    Descarga y procesa todos los PDFs de boletines ENSO del IDEAM.

    Prioridad:
      1. Scraping automático de ideam.gov.co/nuestra-entidad/meteorologia/boletines
      2. URLs fijas desde variable de entorno ENSO_BOLETIN_URLS (respaldo)
      3. PDFs ya presentes en data/raw/boletines_pdf/ (procesamiento local)
    """
    pdf_dir = DATA_RAW / "boletines_pdf"
    pdf_dir.mkdir(exist_ok=True)
    all_dfs = []
    processed_files: set[str] = set()

    # ── 1. Scraping automático ───────────────────────────────────────────────
    scraped_urls = _scrape_enso_urls()

    # ── 2. URLs fijas como respaldo ──────────────────────────────────────────
    fixed_urls = list(BOLETIN_URLS)

    all_urls = scraped_urls + [u for u in fixed_urls if u not in scraped_urls]

    for url in all_urls:
        local = _download_pdf(url, pdf_dir)
        if local is None:
            continue
        processed_files.add(local.name)
        anio, trimestre = _infer_periodo(local.name)
        if anio == 0:
            logger.info("Omitiendo %s: nombre sin informacion de periodo", local.name)
            continue
        df = extract_boletin_pdf(local, trimestre, anio)
        if not df.empty:
            all_dfs.append(df)

    # ── 3. PDFs locales no descargados por URL ───────────────────────────────
    for local in sorted(pdf_dir.glob("*.pdf")):
        if local.name in processed_files:
            continue
        anio, trimestre = _infer_periodo(local.name)
        if anio == 0:
            logger.info("Omitiendo %s: nombre sin informacion de periodo", local.name)
            continue
        df = extract_boletin_pdf(local, trimestre, anio)
        if not df.empty:
            all_dfs.append(df)

    result = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
    out = DATA_RAW / "enso_boletines_raw.csv"
    result.to_csv(out, index=False)
    logger.info("A10 boletines: %s registros -> %s", len(result), out)
    return result
