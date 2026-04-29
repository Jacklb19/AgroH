import logging
import requests
import urllib3
import pandas as pd
import io
from config.settings import DATA_RAW

# Desactivar advertencias de SSL para servidores del DANE si es necesario
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

# Coeficientes aproximados basados en estructura del CNA 2014 (DANE).
# Fuente: Cuadro 1 del Boletín 1 CNA — distribución promedio nacional
# de uso agropecuario: 60% permanentes, 40% transitorios.
_COEF_PERMANENTES  = 0.6  
_COEF_TRANSITORIOS = 0.4

def extract_cna() -> pd.DataFrame:
    """
    Descarga anexos municipales del Censo Nacional Agropecuario 2014 desde DANE.
    # FIX v1: Mapeo de columnas corregido, coeficientes documentados, manejo SSL robusto y parquet.
    """
    logger.info("CNA: iniciando extracción automatizada (DANE)...")
    
    url_uso_suelo = "https://www.dane.gov.co/files/CensoAgropecuario/entrega-definitiva/Boletin-1-Uso-del-suelo/1-Anexos-municipales.xls"
    
    out_dir = DATA_RAW / "cna"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "cna_raw.parquet"
    
    try:
        # Intentar descarga con reintentos para SSL
        resp = None
        for intento in range(2):
            try:
                resp = requests.get(url_uso_suelo, timeout=120, verify=True)
                resp.raise_for_status()
                break
            except requests.exceptions.SSLError:
                logger.warning("CNA: error SSL, reintentando con verify=False")
                resp = requests.get(url_uso_suelo, timeout=120, verify=False)
                resp.raise_for_status()
                break
            except requests.exceptions.Timeout:
                logger.warning("CNA: timeout en intento %s", intento + 1)
        
        if not resp:
            raise ConnectionError("No se pudo descargar el archivo de CNA")

        # Cargar Excel (Cuadro 1 - Uso del Suelo suele estar en sheet_name=2)
        df_uso = pd.read_excel(io.BytesIO(resp.content), sheet_name=2, header=None, skiprows=10)
        
        # Mapeo correcto según estructura DANE:
        # 3: Cod Municipio, 4: Municipio, 5: Area Agricola, 6: Siguiente...
        df_uso = df_uso[[3, 4, 5, 6]].copy()
        df_uso.columns = ["id_municipio", "nombre_municipio", "area_agropecuaria_ha", "area_no_agropecuaria_ha"]
        
        # Limpieza y validación de DIVIPOLA
        df_uso = df_uso.dropna(subset=["id_municipio"])
        df_uso["id_municipio"] = pd.to_numeric(df_uso["id_municipio"], errors="coerce").astype("Int64").astype(str).str.zfill(5)
        df_uso = df_uso[df_uso["id_municipio"] != "<NA>"]
        
        # Validación de formato DIVIPOLA
        if not df_uso.empty:
            sample = df_uso["id_municipio"].head(5).tolist()
            assert all(str(v).isdigit() and len(str(v)) in (4, 5) for v in sample), \
                f"CNA: id_municipio no parece un código DIVIPOLA válido: {sample}"

        df_uso["anio_censo"] = 2014
        
        # Aplicar coeficientes aproximados
        area_agro = pd.to_numeric(df_uso["area_agropecuaria_ha"], errors="coerce")
        df_uso["area_cultivos_permanentes_ha"] = area_agro * _COEF_PERMANENTES
        df_uso["area_cultivos_transitorios_ha"] = area_agro * _COEF_TRANSITORIOS
        
        logger.warning(
            "CNA: area_cultivos_permanentes_ha y area_cultivos_transitorios_ha "
            "son aproximaciones con coeficientes fijos (%.0f%%/%.0f%%).",
            _COEF_PERMANENTES * 100, _COEF_TRANSITORIOS * 100
        )
        
        df_uso.to_parquet(out_file, index=False)
        logger.info("CNA 2014: %s municipios consolidados -> %s", len(df_uso), out_file)
        
        return df_uso
        
    except requests.exceptions.Timeout:
        logger.error("CNA: error de red (timeout) al descargar desde DANE")
        return pd.DataFrame()
    except Exception as e:
        logger.error("CNA: error inesperado procesando Excel: %s", e)
        return pd.DataFrame()
