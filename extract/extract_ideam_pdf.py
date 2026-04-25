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


def _infer_periodo(filename: str) -> tuple[int, str]:
    patterns = [
        r"(?P<anio>\d{4})[_-]?T(?P<trimestre>[1-4])",
        r"(?P<anio>\d{4}).*trimestre[_ -]?(?P<trimestre>[1-4])",
        r"trimestre[_ -]?(?P<trimestre>[1-4]).*(?P<anio>\d{4})",
    ]
    lower_name = filename.lower()
    for pattern in patterns:
        match = re.search(pattern, lower_name)
        if match:
            anio = int(match.group("anio"))
            trimestre = f"T{match.group('trimestre')}"
            return anio, trimestre
    return 0, "T0"

def extract_all_boletines() -> pd.DataFrame:
    """Descarga y procesa todos los PDFs configurados."""
    pdf_dir = DATA_RAW / "boletines_pdf"
    pdf_dir.mkdir(exist_ok=True)
    all_dfs = []

    urls = list(BOLETIN_URLS)
    for url in urls:
        filename = url.rstrip("/").split("/")[-1]
        local = pdf_dir / filename
        if not local.exists():
            logger.info(f"Descargando {filename}...")
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            local.write_bytes(r.content)
        anio, trimestre = _infer_periodo(filename)
        df = extract_boletin_pdf(local, trimestre, anio)
        all_dfs.append(df)

    for local in sorted(pdf_dir.glob("*.pdf")):
        if urls and local.name in {url.rstrip("/").split("/")[-1] for url in urls}:
            continue
        anio, trimestre = _infer_periodo(local.name)
        df = extract_boletin_pdf(local, trimestre, anio)
        all_dfs.append(df)

    result = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
    out = DATA_RAW / "enso_boletines_raw.csv"
    result.to_csv(out, index=False)
    logger.info(f"A10 boletines: {len(result)} registros -> {out}")
    return result
