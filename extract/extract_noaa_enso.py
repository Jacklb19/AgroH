import pandas as pd
import requests
import logging
from pathlib import Path
from config.settings import DATA_RAW, YEAR_END

logger = logging.getLogger(__name__)

URL_NOAA = "https://origin.cpc.ncep.noaa.gov/products/analysis_monitoring/ensostuff/detrend.nino34.ascii.txt"

def extract_noaa_enso() -> pd.DataFrame:
    """Extrae el índice ONI directamente de la NOAA, reemplazando PDFs."""
    logger.info("Extrayendo datos de ENSO desde NOAA (ONI Index)...")
    try:
        # El archivo de NOAA es texto ascii con cabecera YR MON TOTAL CLIM ANOM
        df = pd.read_csv(URL_NOAA, sep=r'\s+')
        
        # Filtrar años relevantes (2015+ hasta el año operativo actual del pipeline)
        df = df[(df["YR"] >= 2015) & (df["YR"] <= YEAR_END)]
        
        # Determinar fase ENSO: ANOM > 0.5 (Niño), < -0.5 (Niña), resto (Neutro)
        def get_fase(anom):
            if anom >= 0.5: return "El Niño"
            if anom <= -0.5: return "La Niña"
            return "Neutro"
            
        df["fase_enso"] = df["ANOM"].apply(get_fase)
        df = df.rename(columns={"YR": "anio", "MON": "mes", "ANOM": "indice_spi"})
        df["fuente_origen"] = "NOAA ONI"
        df["es_sintetico"] = False
        
        # Crear fecha
        df["fecha"] = pd.to_datetime(df.assign(day=1).rename(columns={"anio": "year", "mes": "month"})[["year", "month", "day"]])
        
        # Guardar
        out_path = DATA_RAW / "enso_noaa_raw.csv"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False)
        logger.info(f"NOAA ENSO: {len(df)} meses extraídos -> {out_path}")
        return df
    except Exception as e:
        logger.error(f"Error al extraer ENSO de NOAA (generando sintéticos como fallback): {e}")
        # Generar datos sintéticos de fallback
        import numpy as np
        dates = pd.date_range("2015-01", f"{YEAR_END}-12", freq="MS")
        df_synth = pd.DataFrame({"fecha": dates})
        df_synth["anio"] = df_synth["fecha"].dt.year
        df_synth["mes"] = df_synth["fecha"].dt.month
        
        # Simular ciclo ENSO (aprox 3-7 años)
        t = np.arange(len(df_synth))
        enso_cycle = np.sin(2 * np.pi * t / 48) + np.random.normal(0, 0.3, len(t))
        df_synth["indice_spi"] = np.round(enso_cycle, 2)
        
        def get_fase(anom):
            if anom >= 0.5: return "El Niño"
            if anom <= -0.5: return "La Niña"
            return "Neutro"
            
        df_synth["fase_enso"] = df_synth["indice_spi"].apply(get_fase)
        df_synth["fuente_origen"] = "NOAA ONI (fallback sintetico)"
        df_synth["es_sintetico"] = True
        
        out_path = DATA_RAW / "enso_noaa_raw_synth.csv"
        df_synth.to_csv(out_path, index=False)
        logger.info(f"NOAA ENSO SINTÉTICO: {len(df_synth)} meses generados -> {out_path}")
        return df_synth

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    extract_noaa_enso()
