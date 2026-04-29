"""
train_rendimiento.py — Modelo tabular de rendimiento agrícola AgroIA.

Correcciones aplicadas:
  - Corrección 3.6.a (2026-04-29): Eliminar data leakage en predicciones.
    Predicciones solo sobre test. Train marcado con split='train'.
    Métricas calculadas SOLO sobre conjunto de prueba.
  - Corrección 3.6.b (2026-04-29): Codificación determinista de id_municipio
    con OrdinalEncoder persistido en municipio_encoder.joblib.
"""
import json
import logging
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from load.db import get_engine
from config.settings import MODELS_DIR

logger = logging.getLogger(__name__)

from models.build_features import build_ml_features

# Corrección 3.6.b: Ruta del encoder persistido
ENCODER_PATH = MODELS_DIR / "municipio_encoder.joblib"


def _registrar_version(engine, model_name: str, metrics: dict) -> int:
    """
    Desactiva versiones anteriores del mismo modelo y registra la nueva.
    Retorna el id_version generado.
    """
    from sqlalchemy import text

    with engine.begin() as conn:
        conn.execute(
            text("UPDATE model_version SET activo = FALSE WHERE nombre_modelo = :nm"),
            {"nm": model_name},
        )
        result = conn.execute(
            text(
                "INSERT INTO model_version "
                "(nombre_modelo, fecha_entrenamiento, metricas_json, activo) "
                "VALUES (:nombre_modelo, :fecha_entrenamiento, :metricas_json, :activo) "
                "RETURNING id_version"
            ),
            {
                "nombre_modelo":       model_name,
                "fecha_entrenamiento": datetime.utcnow().isoformat(),
                "metricas_json":       json.dumps(metrics, ensure_ascii=False),
                "activo":              True,
            },
        )
        row = result.fetchone()
    id_version = int(row[0]) if row else None
    logger.info("model_version: id=%s (%s) registrado", id_version, model_name)
    return id_version


def _guardar_predicciones(engine, df_pred: pd.DataFrame, id_version: int | None) -> None:
    """
    Inserta/actualiza pred_rendimiento con las predicciones del modelo.
    """
    from load.db import upsert

    df_pred = df_pred.copy()
    df_pred["id_version"] = id_version
    df_pred = df_pred.replace({np.nan: None})

    cols = [
        "id_municipio",
        "id_cultivo",
        "id_tiempo",
        "rendimiento_predicho_t_ha",
        "intervalo_confianza_inferior",
        "intervalo_confianza_superior",
        "id_version",
    ]
    for c in cols:
        if c not in df_pred.columns:
            df_pred[c] = None

    df_out = df_pred[cols].drop_duplicates(subset=["id_municipio", "id_cultivo", "id_tiempo"])
    upsert(engine, "pred_rendimiento", df_out, ["id_municipio", "id_cultivo", "id_tiempo"])
    logger.info("pred_rendimiento: %s predicciones guardadas", len(df_out))


def train_and_report(engine=None) -> dict:
    """
    Entrena un modelo tabular de rendimiento agrícola.
    - Persiste la versión en model_version con métricas JSON.
    - Escribe predicciones SOLO sobre test y train por separado con flag de split.
    - Métricas de evaluación calculadas SOLO sobre el conjunto de prueba.

    Corrección 3.6.a: Eliminar data leakage.
    Corrección 3.6.b: Codificación determinista con OrdinalEncoder.
    """
    if engine is None:
        engine = get_engine()

    df = build_ml_features(engine)
    if df.empty:
        raise ValueError("No hay datos suficientes para entrenar el modelo")

    df = df.dropna(subset=["rendimiento_t_ha"])

    feature_cols = [
        "id_cultivo",        # feature más importante: el tipo de cultivo determina el rendimiento
        "id_municipio_enc",  # captura patrones territoriales
        "anio",
        "area_sembrada_ha",
        "temp_promedio_anual",
        "temp_maxima_anual",
        "lluvia_acumulada_anual",
    ]

    # Corrección 3.6.b: Codificación determinista de id_municipio
    from sklearn.preprocessing import OrdinalEncoder

    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    if ENCODER_PATH.exists():
        enc = joblib.load(ENCODER_PATH)
        df["id_municipio_enc"] = enc.transform(df[["id_municipio"]])
        logger.info("Encoder de municipios cargado desde %s", ENCODER_PATH)
    else:
        enc = OrdinalEncoder(
            handle_unknown="use_encoded_value",
            unknown_value=-1
        )
        df["id_municipio_enc"] = enc.fit_transform(df[["id_municipio"]])
        joblib.dump(enc, ENCODER_PATH)
        logger.info("Encoder de municipios guardado en %s", ENCODER_PATH)

    X = df[feature_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    y = df["rendimiento_t_ha"].astype(float)

    try:
        from xgboost import XGBRegressor
        model = XGBRegressor(
            n_estimators=250,
            max_depth=6,
            learning_rate=0.05,
            subsample=0.9,
            colsample_bytree=0.9,
            random_state=42,
        )
        model_name = "xgboost_rendimiento"
    except ImportError:
        from sklearn.ensemble import GradientBoostingRegressor
        model = GradientBoostingRegressor(random_state=42)
        model_name = "gradient_boosting_rendimiento"

    from sklearn.model_selection import train_test_split
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    model.fit(X_train, y_train)
    pred_test = model.predict(X_test)

    # Corrección 3.6.a: Métricas calculadas SOLO sobre conjunto de prueba
    mae = float(mean_absolute_error(y_test, pred_test))
    metrics = {
        "mae":    mae,
        "rmse":   float(mean_squared_error(y_test, pred_test) ** 0.5),
        "r2":     float(r2_score(y_test, pred_test)),
        "n_train": int(len(X_train)),
        "n_test":  int(len(X_test)),
    }
    logger.info("Modelo %s entrenado con métricas: %s", model_name, metrics)

    # ── Persistir versión en BD ──────────────────────────────────────────
    id_version = _registrar_version(engine, model_name, metrics)

    # ── Corrección 3.6.a: Predecir solo sobre test + train con flag ──────
    # Solo predecir sobre el conjunto de prueba
    df_test = df.iloc[X_test.index].copy()
    df_test["rendimiento_predicho_t_ha"] = model.predict(X_test)
    df_test["split"] = "test"

    # Para entrenamiento, guardar predicción in-sample solo con flag explícito
    df_train = df.iloc[X_train.index].copy()
    df_train["rendimiento_predicho_t_ha"] = model.predict(X_train)
    df_train["split"] = "train"  # marcar para excluir de evaluación real

    df_pred = pd.concat([df_test, df_train])

    # Obtener id_tiempo representativo por año (usando es_cierre_anual)
    df_tiempo = pd.read_sql(
        "SELECT id_tiempo, anio FROM dim_tiempo WHERE es_cierre_anual = TRUE",
        engine
    )
    df_pred = df_pred.merge(df_tiempo, on="anio", how="left")

    # Intervalo de confianza aproximado (± 1 MAE)
    df_pred["intervalo_confianza_inferior"] = df_pred["rendimiento_predicho_t_ha"] - mae
    df_pred["intervalo_confianza_superior"] = df_pred["rendimiento_predicho_t_ha"] + mae

    _guardar_predicciones(engine, df_pred, id_version)

    return {"model_name": model_name, "metrics": metrics}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    result = train_and_report()
    print(json.dumps(result, indent=2, ensure_ascii=False))
