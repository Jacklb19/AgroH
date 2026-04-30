"""
extract_sipra.py — Extracción de aptitud de suelo desde UPRA ArcGIS REST.

Correcciones aplicadas:
  - Corrección 1.2 (2026-04-29): Detección activa de capas desactualizadas.
    Las URLs de capas se leen desde config/capas_sipra.json.
    Se consulta el catálogo dinámico de UPRA para buscar versiones más recientes.
"""
import json
import logging
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests
import urllib3

# Desactivar advertencias de SSL para servicios de la UPRA
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from config.settings import DATA_RAW, BASE_DIR

logger = logging.getLogger(__name__)

# URL base del catálogo ArcGIS REST de UPRA
UPRA_CATALOG_URL = "https://geoservicios.upra.gov.co/arcgis/rest/services/aptitud_uso_suelo"


def _load_capas_config() -> list[dict]:
    """Carga la configuración de capas desde el archivo JSON externo."""
    config_path = BASE_DIR / "config" / "capas_sipra.json"
    if not config_path.exists():
        logger.error("SIPRA: No se encontró config/capas_sipra.json")
        return []
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _fetch_catalog_layers() -> list[str]:
    """
    Consulta el catálogo dinámico de capas disponibles en UPRA ArcGIS REST.
    Retorna lista de nombres de capas (servicios) disponibles.
    """
    try:
        resp = requests.get(
            UPRA_CATALOG_URL,
            params={"f": "json"},
            verify=False,
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        services = data.get("services", [])
        # Extraer nombres de capas del catálogo
        layer_names = []
        for svc in services:
            name = svc.get("name", "")
            # El nombre viene como "aptitud_uso_suelo/nombre_capa"
            if "/" in name:
                name = name.split("/")[-1]
            layer_names.append(name)
        logger.info("SIPRA: %s capas encontradas en catálogo UPRA", len(layer_names))
        return layer_names
    except (requests.RequestException, ValueError) as e:
        logger.warning(
            "SIPRA: No se pudo consultar catálogo dinámico de UPRA (%s). "
            "Usando capas configuradas.", e
        )
        return []


def _find_newer_layer(current_layer: str, current_year: int | None,
                      catalog_layers: list[str], producto: str) -> str | None:
    """
    Busca en el catálogo una versión más reciente de la capa para el producto dado.
    Compara el año en el nombre de la capa.
    """
    if not catalog_layers or current_year is None:
        return None

    producto_lower = producto.lower().replace(" ", "_")
    best_layer = None
    best_year = current_year

    for layer_name in catalog_layers:
        layer_lower = layer_name.lower()
        # Verificar que la capa corresponde al mismo producto
        if producto_lower not in layer_lower and producto_lower.replace("_", "") not in layer_lower:
            continue

        # Extraer años del nombre de la capa
        years_in_name = re.findall(r'20\d{2}', layer_name)
        if years_in_name:
            layer_year = max(int(y) for y in years_in_name)
            if layer_year > best_year:
                best_year = layer_year
                best_layer = layer_name

    return best_layer


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
    """
    out_file = DATA_RAW / "sipra" / "sipra_aptitud_raw.parquet"
    if out_file.exists():
        logger.info("SIPRA: Archivo consolidado ya existe. Saltando descarga dinámica.")
        return pd.read_parquet(out_file)

    logger.info("SIPRA: iniciando extracción desde UPRA ArcGIS...")

    # Cargar configuración desde archivo externo
    capas_config = _load_capas_config()
    if not capas_config:
        logger.error("SIPRA: Sin configuración de capas disponible.")
        return pd.DataFrame()

    # Consultar catálogo dinámico para detectar versiones más recientes
    _anio_actual = datetime.now().year
    catalog_layers = _fetch_catalog_layers()

    # Resolver capas (actualizar si hay versión más reciente)
    resolved_layers = {}  # {PRODUCTO_UPPER: layer_name}
    for capa in capas_config:
        producto = capa["producto"].upper()
        current_layer = capa["layer"]
        current_year = capa.get("anio_version")

        # Verificar si hay versión más reciente en el catálogo
        if catalog_layers and current_year:
            newer = _find_newer_layer(
                current_layer, current_year, catalog_layers, capa["producto"]
            )
            if newer:
                logger.info(
                    "SIPRA: Capa actualizada: %s → %s",
                    current_layer, newer
                )
                resolved_layers[producto] = newer
                continue

        # Advertencia de obsolescencia
        if current_year and _anio_actual - current_year > 3:
            logger.warning(
                "SIPRA: capa '%s' para %s data de %s (%s años). "
                "Verificar si UPRA publicó una versión más reciente.",
                current_layer, producto, current_year, _anio_actual - current_year
            )

        resolved_layers[producto] = current_layer

    base_url = "https://geoservicios.upra.gov.co/arcgis/rest/services/aptitud_uso_suelo/{layer}/MapServer/0/query"

    def fetch_cultivo(item):
        cultivo, layer = item
        url = base_url.format(layer=layer)
        return _fetch_layer(url, cultivo)

    dfs = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(fetch_cultivo, item): item
            for item in resolved_layers.items()
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
