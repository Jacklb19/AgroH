"""
train_alerta_climatica.py — Clasificador de Riesgo Climático AgroIA Colombia

Genera alertas de nivel BAJO / MEDIO / ALTO por municipio y periodo,
cruzando datos climáticos IDEAM con la fase ENSO activa.

Escribe resultados en:
    - model_version     (registro de la versión con métricas)
    - pred_alerta_climatica (predicciones por municipio/tiempo)

Correcciones aplicadas:
  - Corrección 3.7 v2 (2026-04-29): Eliminación completa de sesgo circular.
    1. Expanding window z-score (no look-ahead).
    2. Ratio cosechada/sembrada como etiqueta primaria de fallback.
    3. Heurística ELIMINADA — filas sin etiqueta se descartan.
    4. Features fantasma (anomalia_pct, prob_deficit, prob_exceso) eliminadas.
    5. Agregación mensual→anual antes del merge con etiquetas.
    6. Split temporal (últimos 2 años como test).

Uso:
    python -m models.train_alerta_climatica
"""
import json
import logging
from datetime import datetime

import numpy as np
import pandas as pd

from load.db import get_engine

logger = logging.getLogger(__name__)

# ── SQL para construir el dataset climático MENSUAL ─────────────────────────
CLIMA_MENSUAL_SQL = """
SELECT
    fc.id_municipio,
    fc.id_tiempo,
    dt.anio,
    dt.mes,
    fc.precipitacion_mm,
    fc.temperatura_media_c,
    fc.temperatura_max_c,
    fc.temperatura_min_c,
    fc.humedad_relativa_pct,
    fc.brillo_solar_horas_dia,
    COALESCE(ae.fase_enso, 'Neutro') AS fase_enso,
    COALESCE(ae.indice_oni, 0)       AS indice_oni
FROM fact_clima_mensual fc
JOIN dim_tiempo dt ON dt.id_tiempo = fc.id_tiempo
LEFT JOIN (
    SELECT ae.id_tiempo, ae.id_region, ae.fase_enso, ae.indice_oni
    FROM fact_alerta_enso ae
) ae ON ae.id_tiempo = fc.id_tiempo
    AND ae.id_region = (
        SELECT m.id_region FROM dim_municipio m WHERE m.id_municipio = fc.id_municipio
    )
"""

# SQL para rendimiento y áreas por municipio-cultivo-año
RENDIMIENTO_SQL = """
SELECT
    fp.id_municipio,
    dc.id_cultivo,
    dt.anio,
    fp.rendimiento_t_ha,
    fp.area_sembrada_ha,
    fp.area_cosechada_ha
FROM fact_produccion_agricola fp
JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
JOIN dim_cultivo dc ON dc.id_cultivo = fp.id_cultivo
WHERE fp.rendimiento_t_ha IS NOT NULL AND fp.rendimiento_t_ha > 0
"""

# Mínimo de años históricos para calcular z-score confiable
MIN_YEARS_FOR_ZSCORE = 3


# ── Etiquetado: Expanding Window Z-Score (Corrección 1) ─────────────────────
def _etiquetar_expanding_window(
    df: pd.DataFrame,
    col_rend: str = "rendimiento_t_ha",
    umbral_alto: float = -1.5,
    umbral_medio: float = -0.5,
) -> pd.Series:
    """
    Z-score con ventana expansiva: solo usa datos de años ANTERIORES.
    Elimina look-ahead bias completamente.

    Para cada municipio, el z-score del año t se calcula con:
      media = mean(rendimiento[años < t])
      std   = std(rendimiento[años < t])

    Los primeros MIN_YEARS_FOR_ZSCORE años quedan con NaN (datos insuficientes).
    """
    df = df.sort_values(["id_municipio", "anio"])

    grouped = df.groupby("id_municipio")[col_rend]

    # shift(1) = excluir el año actual; expanding() = acumular desde el inicio
    mean_hist = grouped.transform(lambda x: x.shift(1).expanding(min_periods=MIN_YEARS_FOR_ZSCORE).mean())
    std_hist = grouped.transform(lambda x: x.shift(1).expanding(min_periods=MIN_YEARS_FOR_ZSCORE).std())
    std_hist = std_hist.clip(lower=0.01)

    z_score = (df[col_rend] - mean_hist) / std_hist

    labels = pd.cut(
        z_score,
        bins=[-np.inf, umbral_alto, umbral_medio, np.inf],
        labels=["ALTO", "MEDIO", "BAJO"],
    )

    n_valid = labels.notna().sum()
    n_discarded = labels.isna().sum()
    logger.info(
        "Expanding window z-score: %s etiquetas válidas, %s descartadas "
        "(primeros %s años por municipio sin suficiente historia)",
        n_valid, n_discarded, MIN_YEARS_FOR_ZSCORE
    )

    return labels


# ── Etiquetado: Ratio Cosechada/Sembrada (Corrección 2) ─────────────────────
def _etiquetar_por_ratio_perdida(
    df: pd.DataFrame,
    umbral_alto: float = 0.5,
    umbral_medio: float = 0.75,
) -> pd.Series:
    """
    Etiqueta basada en el ratio area_cosechada / area_sembrada.

    Si cosechada/sembrada < 0.5 → ALTO (pérdida severa: >50% no cosechado).
    Si cosechada/sembrada < 0.75 → MEDIO (pérdida moderada: 25-50%).
    Si cosechada/sembrada >= 0.75 → BAJO (normal).

    Es independiente del rendimiento y no requiere historia previa.
    """
    ratio = df["area_cosechada_ha"] / df["area_sembrada_ha"].clip(lower=1)

    labels = pd.cut(
        ratio,
        bins=[-np.inf, umbral_alto, umbral_medio, np.inf],
        labels=["ALTO", "MEDIO", "BAJO"],
    )

    n_valid = labels.notna().sum()
    logger.info(
        "Ratio cosechada/sembrada: %s etiquetas generadas. "
        "Distribución: %s",
        n_valid,
        labels.value_counts().to_dict()
    )
    return labels


# ── Agregación mensual→anual (Corrección 5) ─────────────────────────────────
def _agregar_clima_anual(df_mensual: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega datos climáticos mensuales a granularidad anual por municipio.
    Elimina las 12 copias duplicadas por año que inflaban métricas.
    """
    agg_dict = {
        "precipitacion_mm": "sum",       # lluvia anual acumulada
        "temperatura_media_c": "mean",   # promedio anual
        "temperatura_max_c": "max",      # máxima del año
        "temperatura_min_c": "min",      # mínima del año
        "humedad_relativa_pct": "mean",  # promedio anual
        "brillo_solar_horas_dia": "mean",# promedio anual
        "indice_oni": "mean",            # ONI promedio del año
    }

    # Tomar la fase ENSO dominante del año (moda)
    df_fase = (
        df_mensual.groupby(["id_municipio", "anio"])["fase_enso"]
        .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else "Neutro")
        .reset_index()
    )

    # Seleccionar solo columnas numéricas que existan
    valid_agg = {k: v for k, v in agg_dict.items() if k in df_mensual.columns}

    df_anual = (
        df_mensual.groupby(["id_municipio", "anio"])
        .agg(valid_agg)
        .reset_index()
    )

    df_anual = df_anual.merge(df_fase, on=["id_municipio", "anio"], how="left")

    logger.info(
        "Agregación mensual→anual: %s registros mensuales → %s anuales",
        len(df_mensual), len(df_anual)
    )
    return df_anual


def _encode_fase_enso(series: pd.Series) -> pd.Series:
    mapping = {"El Niño": 1, "La Niña": -1, "Neutro": 0}
    return series.map(mapping).fillna(0).astype(int)


def load_training_frame(engine) -> pd.DataFrame:
    try:
        df = pd.read_sql(CLIMA_MENSUAL_SQL, engine)
        logger.info("Dataset climático mensual: %s registros cargados", len(df))
        return df
    except Exception as exc:
        logger.error("Error cargando dataset de entrenamiento climático: %s", exc)
        return pd.DataFrame()


def train_and_report(engine=None) -> dict:
    """
    Entrena el clasificador de alerta climática y persiste resultados en la BD.

    Corrección 3.7 v2:
      1. Expanding window z-score (sin look-ahead).
      2. Ratio cosechada/sembrada como fallback.
      3. Sin heurística — filas sin etiqueta se descartan.
      4. Sin features fantasma (anomalia_pct, prob_deficit, prob_exceso).
      5. Agregación mensual→anual previa al merge.
      6. Split temporal (últimos 2 años).

    Returns:
        dict con model_name, metrics y n_predicciones
    """
    if engine is None:
        engine = get_engine()

    # ── 1. Cargar datos climáticos mensuales ──────────────────────────────────
    df_mensual = load_training_frame(engine)
    if df_mensual.empty:
        raise ValueError(
            "No hay datos climáticos suficientes para entrenar el modelo de alertas. "
            "Ejecuta primero el pipeline ETL core con datos IDEAM."
        )

    # ── 2. Corrección 5: Agregar a granularidad anual ────────────────────────
    df_clima_anual = _agregar_clima_anual(df_mensual)

    # ── 3. Cargar rendimiento y áreas ────────────────────────────────────────
    try:
        df_rend = pd.read_sql(RENDIMIENTO_SQL, engine)
    except Exception as e:
        logger.error("Error cargando rendimiento: %s", e)
        df_rend = pd.DataFrame()

    if df_rend.empty or len(df_rend) < 50:
        raise ValueError(
            f"Datos de rendimiento insuficientes ({len(df_rend) if not df_rend.empty else 0} registros). "
            "Se necesitan al menos 50 registros en fact_produccion_agricola para entrenar. "
            "No se puede entrenar sin etiquetas reales (heurística eliminada)."
        )

    # ── 4. Calcular etiquetas por municipio-año ──────────────────────────────
    # Agregar rendimiento a nivel municipio-año (promedio de todos los cultivos)
    rend_annual = df_rend.groupby(["id_municipio", "anio"]).agg(
        rendimiento_t_ha=("rendimiento_t_ha", "mean"),
        area_sembrada_ha=("area_sembrada_ha", "sum"),
        area_cosechada_ha=("area_cosechada_ha", "sum"),
    ).reset_index()

    # Corrección 1: Expanding window z-score
    rend_annual["label_zscore"] = _etiquetar_expanding_window(
        rend_annual, "rendimiento_t_ha"
    )

    # Corrección 2: Ratio cosechada/sembrada para filas sin z-score
    rend_annual["label_ratio"] = _etiquetar_por_ratio_perdida(rend_annual)

    # Estrategia de etiquetado: z-score primero, ratio como fallback
    rend_annual["nivel_riesgo"] = rend_annual["label_zscore"]
    mask_sin_zscore = rend_annual["nivel_riesgo"].isna()
    rend_annual.loc[mask_sin_zscore, "nivel_riesgo"] = rend_annual.loc[
        mask_sin_zscore, "label_ratio"
    ]

    # Registrar fuente de cada etiqueta
    rend_annual["fuente_etiqueta"] = "expanding_zscore"
    rend_annual.loc[mask_sin_zscore, "fuente_etiqueta"] = "ratio_cosechada_sembrada"

    # Corrección 3: DESCARTAR filas sin ninguna etiqueta (NO usar heurística)
    sin_etiqueta = rend_annual["nivel_riesgo"].isna().sum()
    if sin_etiqueta > 0:
        logger.warning(
            "DESCARTANDO %s filas sin etiqueta real (ni z-score ni ratio). "
            "NO se usa heurística — estas filas se excluyen del entrenamiento.",
            sin_etiqueta
        )
        rend_annual = rend_annual.dropna(subset=["nivel_riesgo"])

    logger.info(
        "Etiquetas finales: %s z-score, %s ratio_perdida, %s total",
        (rend_annual["fuente_etiqueta"] == "expanding_zscore").sum(),
        (rend_annual["fuente_etiqueta"] == "ratio_cosechada_sembrada").sum(),
        len(rend_annual),
    )
    logger.info(
        "Distribución de etiquetas:\n%s",
        rend_annual["nivel_riesgo"].value_counts().to_string()
    )

    # ── 5. Merge clima anual + etiquetas ─────────────────────────────────────
    df = df_clima_anual.merge(
        rend_annual[["id_municipio", "anio", "nivel_riesgo", "fuente_etiqueta"]],
        on=["id_municipio", "anio"],
        how="inner",  # solo filas con etiqueta real verificada
    )

    if df.empty:
        raise ValueError(
            "El cruce clima_anual × etiquetas resultó en 0 filas. "
            "Verificar que fact_produccion_agricola y fact_clima_mensual "
            "comparten municipios y años."
        )

    logger.info(
        "Dataset de entrenamiento: %s registros (municipio-año) "
        "después del merge clima×etiquetas",
        len(df)
    )

    # ── 6. Corrección 4: Features SIN anomalia_pct, prob_deficit, prob_exceso ─
    df["fase_enso_enc"] = _encode_fase_enso(df["fase_enso"])

    feature_cols = [
        "precipitacion_mm",       # lluvia anual acumulada
        "temperatura_media_c",    # temp promedio anual
        "temperatura_max_c",      # temp máxima del año
        "temperatura_min_c",      # temp mínima del año
        "humedad_relativa_pct",   # humedad promedio anual
        "brillo_solar_horas_dia", # brillo promedio anual
        "fase_enso_enc",          # El Niño=1, Neutro=0, La Niña=-1
        "indice_oni",             # ONI promedio del año
        "anio",
    ]
    # Solo usar columnas que existan en el DataFrame
    feature_cols = [c for c in feature_cols if c in df.columns]

    X = df[feature_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    label_map = {"BAJO": 0, "MEDIO": 1, "ALTO": 2}
    y = df["nivel_riesgo"].map(label_map)

    # ── Verificar que hay al menos 2 clases ──────────────────────────────────
    n_clases = y.nunique()
    if n_clases < 2:
        logger.error(
            "Solo una clase de riesgo presente (%s) después del etiquetado real. "
            "El modelo no puede aprender con una sola clase. "
            "Verificar fact_produccion_agricola para diversidad de rendimientos.",
            df["nivel_riesgo"].unique().tolist(),
        )
        return {
            "model_name": "error_single_class",
            "metrics": {"f1_weighted": 0.0, "note": "single_class_real_labels"},
            "n_predicciones": 0,
        }

    # ── Split temporal (Corrección 6 del plan / Etapa B original) ────────────
    cutoff = df["anio"].max() - 2
    mask_train = df["anio"] <= cutoff
    mask_test = df["anio"] > cutoff

    X_train = X[mask_train]
    X_test = X[mask_test]
    y_train = y[mask_train]
    y_test = y[mask_test]

    logger.info(
        "Split temporal: Train %s-%s (%s registros), Test %s-%s (%s registros)",
        df[mask_train]["anio"].min() if mask_train.any() else "?",
        df[mask_train]["anio"].max() if mask_train.any() else "?",
        len(X_train),
        df[mask_test]["anio"].min() if mask_test.any() else "?",
        df[mask_test]["anio"].max() if mask_test.any() else "?",
        len(X_test),
    )

    if len(X_test) == 0 or len(X_train) == 0:
        logger.error("Split temporal resultó en conjunto vacío. Abortando entrenamiento.")
        return {"model_name": "error", "metrics": {"f1_weighted": 0.0, "note": "empty_split"}}

    # ── Modelo ───────────────────────────────────────────────────────────────
    from sklearn.metrics import classification_report, f1_score

    try:
        from xgboost import XGBClassifier
        model = XGBClassifier(
            n_estimators=200,
            max_depth=5,
            learning_rate=0.05,
            subsample=0.85,
            colsample_bytree=0.85,
            random_state=42,
            eval_metric="mlogloss",
        )
        model_name = "xgboost_alerta_climatica"
    except ImportError:
        from sklearn.ensemble import RandomForestClassifier
        model = RandomForestClassifier(n_estimators=200, random_state=42, n_jobs=-1)
        model_name = "random_forest_alerta_climatica"

    model.fit(X_train, y_train)
    pred_test = model.predict(X_test)

    inv_label_map = {v: k for k, v in label_map.items()}
    labels_present = sorted(set(y_test.tolist()) | set(pred_test.tolist()))
    metrics = {
        "f1_weighted": float(f1_score(y_test, pred_test, average="weighted", zero_division=0)),
        "report": classification_report(
            y_test,
            pred_test,
            labels=labels_present,
            target_names=[inv_label_map[label] for label in labels_present],
            output_dict=True,
            zero_division=0,
        ),
        "n_train": int(len(X_train)),
        "n_test": int(len(X_test)),
        "etiquetado": "expanding_zscore + ratio_perdida (sin heuristica)",
        "split": "temporal",
        "cutoff_year": int(cutoff),
        "features_used": feature_cols,
        "n_descartados_sin_etiqueta": int(sin_etiqueta),
    }
    logger.info("Modelo %s — F1 ponderado: %.4f", model_name, metrics["f1_weighted"])

    # ── Registrar versión en model_version ───────────────────────────────────
    id_version = _registrar_version(engine, model_name, metrics)

    # ── Generar predicciones para todos los registros ────────────────────────
    pred_todas = model.predict(X)
    score_todas = model.predict_proba(X)

    # Necesitamos id_tiempo para guardar en pred_alerta_climatica.
    # Como ahora trabajamos a nivel anual, recuperar el id_tiempo de cierre.
    df_tiempo_cierre = pd.read_sql(
        "SELECT id_tiempo, anio FROM dim_tiempo WHERE es_cierre_anual = TRUE",
        engine
    )
    df_pred_base = df[["id_municipio", "anio"]].copy()
    df_pred_base["anio"] = pd.to_numeric(df_pred_base["anio"], errors="coerce").fillna(0).astype(int)
    df_pred_base = df_pred_base.merge(df_tiempo_cierre, on="anio", how="left")

    df_pred = df_pred_base.copy()
    df_pred["nivel_riesgo"] = [inv_label_map[p] for p in pred_todas]
    df_pred["tipo_evento"] = df["fase_enso"].values
    df_pred["score_probabilidad"] = score_todas.max(axis=1)
    df_pred["descripcion_generada"] = df_pred.apply(
        lambda r: (
            f"Alerta {r['nivel_riesgo']} — Fase ENSO: {r['tipo_evento']}. "
            f"Probabilidad estimada: {r['score_probabilidad']:.0%}."
        ),
        axis=1,
    )
    df_pred["activa"] = True
    df_pred["id_version"] = id_version

    # Solo guardar filas con id_tiempo válido
    df_pred = df_pred.dropna(subset=["id_tiempo"])
    _guardar_predicciones(engine, df_pred)

    return {"model_name": model_name, "metrics": metrics, "n_predicciones": len(df_pred)}


# ── Helpers BD ───────────────────────────────────────────────────────────────

def _registrar_version(engine, model_name: str, metrics: dict) -> int | None:
    from sqlalchemy import text
    # Desactivar versiones anteriores del mismo modelo
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE model_version SET activo = FALSE WHERE nombre_modelo = :nm"),
            {"nm": model_name},
        )
    with engine.begin() as conn:
        from sqlalchemy import text as sqltext
        result = conn.execute(
            sqltext(
                "INSERT INTO model_version (nombre_modelo, fecha_entrenamiento, metricas_json, activo) "
                "VALUES (:nombre_modelo, :fecha_entrenamiento, :metricas_json, :activo) "
                "RETURNING id_version"
            ),
            {
                "nombre_modelo": model_name,
                "fecha_entrenamiento": datetime.utcnow().isoformat(),
                "metricas_json": json.dumps(metrics, ensure_ascii=False),
                "activo": True,
            },
        )
        row = result.fetchone()
    id_version = int(row[0]) if row else None
    logger.info("model_version registrado: %s (id=%s)", model_name, id_version)
    return id_version


def _guardar_predicciones(engine, df_pred: pd.DataFrame) -> None:
    from load.db import upsert
    cols = [
        "id_municipio", "id_tiempo", "nivel_riesgo",
        "tipo_evento", "score_probabilidad",
        "descripcion_generada", "activa", "id_version",
    ]
    df_out = df_pred[cols].drop_duplicates(subset=["id_municipio", "id_tiempo"])
    upsert(engine, "pred_alerta_climatica", df_out, ["id_municipio", "id_tiempo"])
    logger.info("pred_alerta_climatica: %s predicciones guardadas", len(df_out))


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    result = train_and_report()
    print(json.dumps(
        {k: v for k, v in result.items() if k != "metrics"},
        indent=2, ensure_ascii=False,
    ))
    print(f"\nF1 ponderado: {result['metrics']['f1_weighted']:.4f}")
