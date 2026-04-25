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
                            # Extraer SPI si aparece un número como -1.2 o 0.8
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
    "ene": "T1", "jan": "T1", "feb": "T1", "mar": "T1",
    "abr": "T2", "apr": "T2", "may": "T2", "jun": "T2",
    "jul": "T3", "ago": "T3", "aug": "T3", "sep": "T3",
    "oct": "T4", "nov": "T4", "dic": "T4", "dec": "T4",
}


def _infer_periodo(filename: str) -> tuple[int, str]:
    patterns = [
        r"(?P<anio>\d{4})[_-]?[tT](?P<trimestre>[1-4])",
        r"(?P<anio>\d{4}).*trimestre[_ -]?(?P<trimestre>[1-4])",
        r"trimestre[_ -]?(?P<trimestre>[1-4]).*(?P<anio>\d{4})",
    ]
    lower_name = filename.lower()
    for pattern in patterns:
        match = re.search(pattern, lower_name, re.IGNORECASE)
        if match:
            return int(match.group("anio")), f"T{match.group('trimestre')}"

    # Formato IDEAM: "boletin_jul2025.pdf" o "dic2025" o "2025_jul"
    month_match = re.search(
        r"([a-z]{3})[_\-]?(\d{4})|(\d{4})[_\-]?([a-z]{3})",
        lower_name,
    )
    if month_match:
        if month_match.group(1) and month_match.group(2):
            mes, anio_str = month_match.group(1), month_match.group(2)
        else:
            anio_str, mes = month_match.group(3), month_match.group(4)
        trimestre = _MES_A_TRIMESTRE.get(mes[:3])
        if trimestre:
            return int(anio_str), trimestre

    logger.warning("No se pudo inferir periodo de '%s' — asignando T0/0", filename)
    return 0, "T0"

def extract_all_boletines() -> pd.DataFrame:
    """Descarga y procesa todos los PDFs configurados."""
    pdf_dir = DATA_RAW / "boletines_pdf"
    pdf_dir.mkdir(exist_ok=True)
    all_dfs = []

    urls = list(BOLETIN_URLS)
    url_filenames = set()
    for url in urls:
        filename = url.rstrip("/").split("/")[-1]
        url_filenames.add(filename)
        local = pdf_dir / filename
        if not local.exists():
            logger.info("Descargando %s...", filename)
            try:
                r = requests.get(url, timeout=60)
                r.raise_for_status()
                local.write_bytes(r.content)
            except Exception as exc:
                logger.warning("No se pudo descargar %s: %s", url, exc)
                continue
        anio, trimestre = _infer_periodo(filename)
        if anio == 0:
            logger.info("Omitiendo %s: nombre sin informacion de periodo", filename)
            continue
        df = extract_boletin_pdf(local, trimestre, anio)
        all_dfs.append(df)

    for local in sorted(pdf_dir.glob("*.pdf")):
        if local.name in url_filenames:
            continue
        anio, trimestre = _infer_periodo(local.name)
        if anio == 0:
            logger.info("Omitiendo %s: nombre sin informacion de periodo", local.name)
            continue
        df = extract_boletin_pdf(local, trimestre, anio)
        all_dfs.append(df)

    result = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
    out = DATA_RAW / "enso_boletines_raw.csv"
    result.to_csv(out, index=False)
    logger.info("A10 boletines: %s registros -> %s", len(result), out)
    return result
