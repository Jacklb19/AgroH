import pandas as pd
import requests
import logging
import io
import numpy as np
from datetime import datetime
from pathlib import Path
from config.settings import DATA_RAW, YEAR_END

logger = logging.getLogger(__name__)

# URL principal (www funciona; origin está caído)
URL_NOAA_PRIMARY = "https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/ensostuff/detrend.nino34.ascii.txt"
# Fallback: PSL NOAA (Niño 3.4 anomalías)
URL_NOAA_FALLBACK = "https://psl.noaa.gov/data/correlation/nina34.anom.data"

def extract_noaa_enso() -> pd.DataFrame:
    """
    Extrae el índice ONI directamente de la NOAA, reemplazando PDFs.
    # FIX v1: Renombrado semántico (oni vs spi), fechas reales, ruido autocorrelacionado y timeouts.
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
        
        # Crear fecha directamente desde columnas
        df["fecha"] = pd.to_datetime({"year": df["anio"], "month": df["mes"], "day": 1})
        
        # Guardar
        df.to_parquet(out_path, index=False)
        logger.info("NOAA ENSO: %s meses extraídos -> %s", len(df), out_path)
        return df
        
    except Exception as e:
        logger.error("NOAA ENSO: fallo en extracción, generando sintéticos: %s", e)
        
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
        
        out_path_synth = out_dir / "enso_noaa_raw_synth.parquet"
        df_synth.to_parquet(out_path_synth, index=False)
        logger.info("NOAA ENSO SINTÉTICO: %s meses generados -> %s", len(df_synth), out_path_synth)
        return df_synth

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    extract_noaa_enso()
