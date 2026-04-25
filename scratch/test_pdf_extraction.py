import pdfplumber
import pandas as pd
import requests
import re
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_pdf")

# Una de las URLs encontradas
TEST_URL = "https://www.ideam.gov.co/sites/default/files/prensa/boletines/2025-01-30/boletin_agroclimatico_nacional_enero_2025.pdf"
TEMP_PDF = Path("test_boletin.pdf")

REGIONES = ["Andina", "Caribe", "Pacífico", "Orinoquía", "Amazonía"]

def _parse_fase_enso(texto: str) -> str:
    texto = texto.lower()
    if "niño" in texto:  return "El Niño"
    if "niña" in texto:  return "La Niña"
    return "Neutro"

def test_extraction(pdf_path: Path):
    logger.info(f"Probando extracción en {pdf_path}...")
    registros = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            # Intentar extraer tablas
            tables = page.extract_tables()
            if tables:
                logger.info(f"Página {i+1}: detectadas {len(tables)} tablas")
                for t_idx, table in enumerate(tables):
                    for row in table:
                        if not row: continue
                        row_text = " ".join([str(c) for c in row if c])
                        # logger.info(f"Fila: {row_text[:100]}") # Debug ruidoso
                        for region in REGIONES:
                            if region.lower() in row_text.lower():
                                logger.info(f"MATCH REGION: {region} en página {i+1}")
                                spi_match = re.search(r"[-+]?\d+\.\d+", row_text)
                                spi = float(spi_match.group()) if spi_match else None
                                registros.append({
                                    "pagina": i+1,
                                    "region": region,
                                    "fase_enso": _parse_fase_enso(row_text),
                                    "indice_spi": spi,
                                    "texto": row_text[:200]
                                })
            else:
                # Si no hay tablas, probar extract_text
                text = page.extract_text()
                if text:
                    for line in text.split("\n"):
                        for region in REGIONES:
                            if region.lower() in line.lower():
                                logger.info(f"MATCH REGION (TEXTO): {region} en página {i+1}")
                                spi_match = re.search(r"[-+]?\d+\.\d+", line)
                                spi = float(spi_match.group()) if spi_match else None
                                registros.append({
                                    "pagina": i+1,
                                    "region": region,
                                    "fase_enso": _parse_fase_enso(line),
                                    "indice_spi": spi,
                                    "texto": line[:200]
                                })

    df = pd.DataFrame(registros)
    print("\n--- RESULTADOS ---")
    print(df)
    return df

if __name__ == "__main__":
    if not TEMP_PDF.exists():
        logger.info(f"Descargando {TEST_URL}...")
        r = requests.get(TEST_URL, verify=False) # A veces el IDEAM tiene temas de SSL
        TEMP_PDF.write_bytes(r.content)
    
    test_extraction(TEMP_PDF)
