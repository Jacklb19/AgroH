import pdfplumber
import pandas as pd
import requests
import re
import logging
from pathlib import Path
from config.settings import DATA_RAW

logger = logging.getLogger(__name__)

# URLs de boletines agroclimáticos IDEAM (agregar URLs reales al descargar)
BOLETIN_URLS = [
    # Ejemplo: "https://www.ideam.gov.co/.../boletin_agroclimatico_2024_T1.pdf",
]

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
                                "texto_raw":   row_text[:300],
                            })
    return pd.DataFrame(registros)

def extract_all_boletines() -> pd.DataFrame:
    """Descarga y procesa todos los PDFs configurados."""
    pdf_dir = DATA_RAW / "boletines_pdf"
    pdf_dir.mkdir(exist_ok=True)
    all_dfs = []
    for url in BOLETIN_URLS:
        filename = url.rstrip("/").split("/")[-1]
        local = pdf_dir / filename
        if not local.exists():
            logger.info(f"Descargando {filename}...")
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            local.write_bytes(r.content)
        # Extraer año y trimestre del nombre del archivo
        match = re.search(r"(\d{4})_T(\d)", filename)
        if match:
            anio, trimestre = int(match.group(1)), f"T{match.group(2)}"
        else:
            anio, trimestre = 0, "T0"
        df = extract_boletin_pdf(local, trimestre, anio)
        all_dfs.append(df)
    result = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
    out = DATA_RAW / "enso_boletines_raw.csv"
    result.to_csv(out, index=False)
    logger.info(f"A10 boletines: {len(result)} registros → {out}")
    return result
