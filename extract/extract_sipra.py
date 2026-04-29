import logging
import requests
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import urllib3

# Desactivar advertencias de SSL para servicios de la UPRA
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from config.settings import DATA_RAW

logger = logging.getLogger(__name__)

# Mapeo de algunos cultivos comunes a sus servicios en UPRA
UPRA_SERVICES = {
    "ARROZ": "aptitud_arroz_secano",
    "CAFE": "Aptitud_Cafe_Jul2022",
    "CACAO": "aptitud_cacao_diciembre_2019",
    "PAPA": "aptitud_papa_sem_1_Dic2019",
    "MAIZ": "Aptitud_Maiz_Tradicional",
    "PLATANO": "aptitud_platano",
    "AGUACATE": "aptitud_aguacate_hass_Dic2019",
    "YUCA": "aptitud_yuca",
    "CEBOLLA": "aptitud_cebolla_bulbo_sem_1_Dic2019",
    "ALGODON": "aptitud_algodon_sem_1_Jun2020",
    "BANANO": "aptitud_banano",
    "MANGO": "aptitud_mango_diciembre_2019",
    "PINA": "aptitud_pina",
    "CAUCHO": "aptitud_caucho_diciembre_2019",
    "PALMA DE ACEITE": "aptitud_palma_2018"
}

def _fetch_layer(url: str, cultivo: str) -> pd.DataFrame:
    """Descarga todos los registros de una capa ArcGIS con paginación."""
    all_records = []
    offset = 0
    page_size = 1000

    while True:
        params = {
            "where": "1=1",
            "outFields": "cod_dane_mpio,aptitud",
            "f": "json",
            "returnGeometry": "false",
            "resultOffset": offset,
            "resultRecordCount": page_size,
        }
        try:
            r = requests.get(url, params=params, verify=False, timeout=120)
            r.raise_for_status()
            data = r.json()
        except requests.exceptions.Timeout:
            logger.warning("SIPRA: timeout en %s (offset %s)", cultivo, offset)
            break
        except Exception as e:
            logger.error("SIPRA: error en %s (offset %s): %s", cultivo, offset, e)
            break

        if "error" in data:
            logger.error("SIPRA: ArcGIS error en %s: %s", cultivo, data["error"])
            break

        features = data.get("features", [])
        if not features:
            break

        all_records.extend(f["attributes"] for f in features)

        # Si no superó el límite de transferencia, ya terminamos
        if not data.get("exceededTransferLimit", False):
            break

        offset += page_size

    if not all_records:
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    df = df.rename(columns={"cod_dane_mpio": "id_municipio"})
    df["cultivo_origen"] = cultivo
    if "aptitud" in df.columns:
        df["aptitud"] = df["aptitud"].astype(str).str.replace(
            r'Exclusi.n', 'Exclusion', regex=True
        )
    return df

def extract_sipra() -> pd.DataFrame:
    """
    Descarga la aptitud de suelo por municipio directamente desde la API REST de ArcGIS de la UPRA.
    # FIX v1: Paginación ArcGIS (evita truncado), ThreadPoolExecutor y alertas de obsolescencia.
    """
    logger.info("SIPRA: iniciando extracción desde UPRA ArcGIS...")
    
    _anio_actual = datetime.now().year
    for cultivo, layer in UPRA_SERVICES.items():
        years_in_name = re.findall(r'20\d{2}', layer)
        if years_in_name:
            layer_year = max(int(y) for y in years_in_name)
            if _anio_actual - layer_year > 3:
                logger.warning(
                    "SIPRA: capa '%s' para %s data de %s (%s años). "
                    "Verificar si UPRA publicó una versión más reciente.",
                    layer, cultivo, layer_year, _anio_actual - layer_year
                )

    base_url = "https://geoservicios.upra.gov.co/arcgis/rest/services/aptitud_uso_suelo/{layer}/MapServer/0/query"
    
    def fetch_cultivo(item):
        cultivo, layer = item
        url = base_url.format(layer=layer)
        return _fetch_layer(url, cultivo)

    dfs = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(fetch_cultivo, item): item 
            for item in UPRA_SERVICES.items()
        }
        for future in as_completed(futures):
            cultivo, _ = futures[future]
            try:
                df = future.result()
                if not df.empty:
                    dfs.append(df)
                    logger.info("SIPRA: %s -> %s registros", cultivo, len(df))
            except Exception as e:
                logger.error("SIPRA: fallo inesperado en %s: %s", cultivo, e)
            
    if dfs:
        result = pd.concat(dfs, ignore_index=True)
        out_dir = DATA_RAW / "sipra"
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / "sipra_aptitud_raw.parquet"
        result.to_parquet(out, index=False)
        logger.info("SIPRA: %s registros consolidados -> %s", len(result), out)
        return result
    else:
        logger.warning("SIPRA: no se pudo extraer ningún dato.")
        return pd.DataFrame()
