import logging
from pathlib import Path

import pandas as pd

from config.settings import MANUAL_DATA_DIR

logger = logging.getLogger(__name__)


def _find_candidate_files() -> list[Path]:
    base = MANUAL_DATA_DIR / "sipsa"
    if not base.exists():
        return []
    patterns = ("*.csv", "*.xlsx", "*.xls", "*.parquet")
    files: list[Path] = []
    for pattern in patterns:
        files.extend(sorted(base.glob(pattern)))
    return files


def extract_sipsa() -> pd.DataFrame:
    """
    Carga microdatos SIPSA desde archivos manuales locales.
    La fuente oficial no ofrece un endpoint sencillo y estable para automatizar esta parte.
    """
    files = _find_candidate_files()
    if not files:
        logger.info("No se encontraron archivos SIPSA en %s", MANUAL_DATA_DIR / "sipsa")
        return pd.DataFrame()

    frames = []
    for path in files:
        logger.info("Leyendo archivo SIPSA manual: %s", path.name)
        suffix = path.suffix.lower()
        if suffix == ".csv":
            frames.append(pd.read_csv(path))
        elif suffix == ".parquet":
            frames.append(pd.read_parquet(path))
        else:
            frames.append(pd.read_excel(path))

    result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not result.empty:
        out = MANUAL_DATA_DIR.parent / "sipsa_raw_consolidado.parquet"
        result.to_parquet(out, index=False)
        logger.info("SIPSA manual: %s registros consolidados → %s", len(result), out)
    return result
