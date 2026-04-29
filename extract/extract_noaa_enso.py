"""
extract_noaa_enso.py — Extracción del índice ONI de NOAA para análisis ENSO.

Correcciones aplicadas:
  - Corrección 1.3 (2026-04-29): El campo `es_sintetico` (bool) está SIEMPRE presente.
    Agregado campo `fuente_datos` ('noaa_oni_real' o 'sintetico_fallback').
    La ruta sintética emite logger.error. Docstring actualizado.
"""
import io
import logging
from datetime import datetime

import numpy as np
import pandas as pd
import requests

from config.settings import DATA_RAW, YEAR_END

logger = logging.getLogger(__name__)

# URL principal (www funciona; origin está caído)
URL_NOAA_PRIMARY = "https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/ensostuff/detrend.nino34.ascii.txt"
# Fallback: PSL NOAA (Niño 3.4 anomalías)
URL_NOAA_FALLBACK = "https://psl.noaa.gov/data/correlation/nina34.anom.data"

def extract_noaa_enso() -> pd.DataFrame:
    """
    Extrae el índice ONI directamente de la NOAA, reemplazando PDFs.

    El campo `es_sintetico` (bool) está SIEMPRE presente en el DataFrame de
    retorno y DEBE verificarse antes de usar los datos en modelos o alertas:
      - False: datos reales de NOAA ONI.
      - True: datos sintéticos generados como fallback.

    El campo `fuente_datos` (str) indica la procedencia:
      - 'noaa_oni_real': datos descargados correctamente de NOAA.
      - 'sintetico_fallback': datos generados sintéticamente.

    Corrección 1.3: es_sintetico siempre presente, fuente_datos agregado,
    logger.error en ruta sintética.
    """
    logger.info("NOAA ENSO: iniciando extracción (ONI Index)...")
    out_dir = DATA_RAW / "noaa"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "enso_noaa_raw.parquet"

    try:
        # Intentar URLs con timeout y fallback
        text = None
        for url in [URL_NOAA_PRIMARY, URL_NOAA_FALLBACK]:
            try:
                logger.info("NOAA ENSO: intentando %s", url)
                resp = requests.get(url, timeout=60)
                resp.raise_for_status()
                text = resp.text
                logger.info("NOAA ENSO: conexión exitosa")
                break
            except requests.RequestException as req_err:
                logger.warning("NOAA ENSO: fallo en %s: %s", url, req_err)
                continue

        if text is None:
            raise ConnectionError("No se pudo conectar a ninguna URL de NOAA")

        # El archivo de NOAA es texto ascii con cabecera YR MON TOTAL CLIM ANOM
        df = pd.read_csv(io.StringIO(text), sep=r'\s+', engine='python')

        # Filtrar años relevantes (2015+ hasta el año operativo actual del pipeline)
        df = df[(df["YR"] >= 2015) & (df["YR"] <= YEAR_END)]

        # Determinar fase ENSO: ANOM > 0.5 (Niño), < -0.5 (Niña), resto (Neutro)
        def get_fase(anom):
            if anom >= 0.5: return "El Niño"
            if anom <= -0.5: return "La Niña"
            return "Neutro"

        df["fase_enso"] = df["ANOM"].apply(get_fase)
        df = df.rename(columns={"YR": "anio", "MON": "mes", "ANOM": "indice_oni"})
        df["fuente_origen"] = "NOAA ONI"
        df["es_sintetico"] = False
        df["fuente_datos"] = "noaa_oni_real"

        # Crear fecha directamente desde columnas
        df["fecha"] = pd.to_datetime({"year": df["anio"], "month": df["mes"], "day": 1})

        # Guardar
        df.to_parquet(out_path, index=False)
        logger.info("NOAA ENSO: %s meses extraídos -> %s", len(df), out_path)
        return df

    except Exception as e:
        logger.error(
            "NOAA ENSO: DATOS SINTÉTICOS GENERADOS — La conexión con NOAA falló: %s. "
            "Los datos ONI son sintéticos y los análisis de riesgo climático "
            "NO deben usarse hasta que se restaure la conexión con NOAA.", e
        )

        # Generar datos sintéticos de fallback (sin fechas futuras)
        now = datetime.now()
        end_month = f"{now.year}-{now.month:02d}"
        dates = pd.date_range("2015-01", end_month, freq="MS")

        df_synth = pd.DataFrame({"fecha": dates})
        df_synth["anio"] = df_synth["fecha"].dt.year
        df_synth["mes"] = df_synth["fecha"].dt.month

        # Ruido con autocorrelación suave (más realista que seno puro)
        rng = np.random.default_rng(42)
        raw_noise = rng.normal(0, 0.8, len(df_synth))
        # Usar media móvil de 3 meses para autocorrelación mínima
        kernel = np.ones(3) / 3
        enso_signal = np.convolve(raw_noise, kernel, mode='same')
        df_synth["indice_oni"] = np.round(enso_signal, 2)

        def get_fase(anom):
            if anom >= 0.5: return "El Niño"
            if anom <= -0.5: return "La Niña"
            return "Neutro"

        df_synth["fase_enso"] = df_synth["indice_oni"].apply(get_fase)
        df_synth["fuente_origen"] = "NOAA ONI (fallback sintetico)"
        df_synth["es_sintetico"] = True
        df_synth["fuente_datos"] = "sintetico_fallback"

        out_path_synth = out_dir / "enso_noaa_raw_synth.parquet"
        df_synth.to_parquet(out_path_synth, index=False)
        logger.info("NOAA ENSO SINTÉTICO: %s meses generados -> %s", len(df_synth), out_path_synth)
        return df_synth

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    extract_noaa_enso()
