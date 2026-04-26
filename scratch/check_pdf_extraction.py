"""Verifica qué datos se extraen y en qué páginas de los boletines PDF."""
import pdfplumber
import re
import os
from pathlib import Path

PDF_DIR = Path("data/raw/boletines_pdf")
REGIONES = ["Andina", "Caribe", "Pacífico", "Orinoquía", "Amazonía"]

# 1. Revisar el CSV actual
print("=" * 60)
print("1. DATOS ACTUALES EN enso_boletines_raw.csv")
print("=" * 60)
raw_csv = Path("data/raw/enso_boletines_raw.csv")
if raw_csv.exists():
    import pandas as pd
    df = pd.read_csv(raw_csv)
    print(f"Total registros: {len(df)}")
    print(f"Columnas: {list(df.columns)}")
    if not df.empty:
        print(f"\nPor año:\n{df.groupby('anio').size()}")
        print(f"\nPor región:\n{df.groupby('region').size()}")
        print(f"\nMuestra:\n{df.head(10).to_string()}")
else:
    print("No existe el archivo CSV")

# 2. Analizar un PDF pequeño para ver en qué páginas aparece info ENSO
print("\n" + "=" * 60)
print("2. ANÁLISIS DE PÁGINAS CON INFO ENSO (PDF pequeño)")
print("=" * 60)

# Buscar un PDF pequeño (< 10MB)
small_pdfs = sorted(
    [(f, f.stat().st_size) for f in PDF_DIR.glob("*.pdf") if f.stat().st_size < 10_000_000],
    key=lambda x: x[1]
)

if small_pdfs:
    test_pdf = small_pdfs[0][0]
    size_mb = small_pdfs[0][1] / 1_000_000
    print(f"Probando: {test_pdf.name} ({size_mb:.1f} MB)")
    
    with pdfplumber.open(test_pdf) as pdf:
        print(f"Total páginas: {len(pdf.pages)}")
        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            found_regions = [r for r in REGIONES if r.lower() in text.lower()]
            has_enso = any(k in text.lower() for k in ["niño", "niña", "neutro", "enso", "spi"])
            if found_regions or has_enso:
                print(f"  Pág {i+1}: Regiones={found_regions}, ENSO={has_enso}, chars={len(text)}")

# 3. Tamaños de todos los PDFs
print("\n" + "=" * 60)
print("3. TAMAÑOS DE PDFs DESCARGADOS")
print("=" * 60)
for f in sorted(PDF_DIR.glob("*.pdf")):
    size_mb = f.stat().st_size / 1_000_000
    marker = " ⚠️ PESADO" if size_mb > 15 else ""
    print(f"  {f.name}: {size_mb:.1f} MB{marker}")
