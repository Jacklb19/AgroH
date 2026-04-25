import logging
from pathlib import Path

import pandas as pd

from config.settings import MANUAL_DATA_DIR

logger = logging.getLogger(__name__)

_SKIP_SHEETS = {"índice", "indice", "index", "contents", "tabla de contenido"}
_HEADER_KEYWORDS = {"fecha", "producto", "mercado", "precio", "grupo", "cultivo"}


def _read_sipsa_excel(path: Path) -> pd.DataFrame:
    """
    Lee todas las hojas de datos de un archivo SIPSA DANE.
    Detecta automáticamente la fila de cabecera (contiene 'fecha', 'producto', etc.)
    ignorando las primeras filas de título/metadata.
    """
    try:
        xl = pd.ExcelFile(path)
    except Exception as exc:
        logger.warning("No se pudo abrir %s: %s", path.name, exc)
        return pd.DataFrame()

    frames = []
    for sheet in xl.sheet_names:
        if sheet.lower().strip() in _SKIP_SHEETS:
            continue
        try:
            # Leer sin header para detectar la fila correcta
            raw = pd.read_excel(path, sheet_name=sheet, header=None, nrows=15)
        except Exception:
            continue

        header_row = None
        for i, row in raw.iterrows():
            vals = {str(v).lower().strip() for v in row if pd.notna(v) and str(v).strip()}
            if vals & _HEADER_KEYWORDS:
                header_row = i
                break

        if header_row is None:
            continue

        try:
            df = pd.read_excel(path, sheet_name=sheet, header=header_row)
            df = df.dropna(how="all", axis=1).dropna(how="all", axis=0)
            if not df.empty:
                df["_sheet"] = sheet
                df["_file"] = path.stem
                frames.append(df)
        except Exception as exc:
            logger.warning("Error leyendo hoja '%s' de %s: %s", sheet, path.name, exc)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _find_candidate_files() -> list[Path]:
    base = MANUAL_DATA_DIR / "sipsa"
    if not base.exists():
        return []
    files: list[Path] = []
    for pattern in ("*.csv", "*.xlsx", "*.xls", "*.parquet"):
        files.extend(sorted(base.glob(pattern)))
    return files


def extract_sipsa() -> pd.DataFrame:
    """
    Carga microdatos SIPSA desde archivos manuales en data/raw/manual/sipsa/.
    Lee todas las hojas de datos de cada Excel, detectando la fila de cabecera.
    """
    files = _find_candidate_files()
    if not files:
        logger.info("No se encontraron archivos SIPSA en %s", MANUAL_DATA_DIR / "sipsa")
        return pd.DataFrame()

    frames = []
    for path in files:
        logger.info("Leyendo archivo SIPSA: %s", path.name)
        suffix = path.suffix.lower()
        if suffix == ".csv":
            frames.append(pd.read_csv(path))
        elif suffix == ".parquet":
            frames.append(pd.read_parquet(path))
        else:
            df = _read_sipsa_excel(path)
            if not df.empty:
                frames.append(df)
            else:
                logger.warning("  %s: no se encontraron datos en las hojas", path.name)

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)

    # Normalizar tipos mixtos para compatibilidad con Parquet
    for col in result.select_dtypes(include="object").columns:
        result[col] = result[col].astype(str).replace({"nan": None, "None": None})

    out = MANUAL_DATA_DIR.parent / "sipsa_raw_consolidado.parquet"
    result.to_parquet(out, index=False)
    logger.info("SIPSA: %s registros de %s archivos -> %s", len(result), len(frames), out)
    return result
