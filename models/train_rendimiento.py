import json
import logging
from datetime import datetime

import numpy as np
import pandas as pd

from load.db import get_engine

logger = logging.getLogger(__name__)


from models.build_features import build_ml_features


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
    - Escribe predicciones sobre todo el dataset en pred_rendimiento.
    - Para un modelo final conviene sumar suelo, precios e insumos como features.
    """
    if engine is None:
        engine = get_engine()

    df = build_ml_features(engine)
    if df.empty:
        raise ValueError("No hay datos suficientes para entrenar el modelo")

    df = df.dropna(subset=["rendimiento_t_ha"])
    
    # NUEVAS VARIABLES LIMPIAS (SIN FUGA DE DATOS)
    feature_cols = [
        "anio",
        "area_sembrada_ha",
        "temp_promedio_anual",     # Del Feature Store
        "temp_maxima_anual",       # Del Feature Store
        "lluvia_acumulada_anual"   # Del Feature Store
    ]
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

    # Intervalo de confianza aproximado (± 1 MAE)
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

    # ── Predecir sobre todo el dataset ──────────────────────────────────────
    pred_todas = model.predict(X)
    df_pred = df[["id_municipio", "id_cultivo", "anio"]].copy()
    
    # Obtener un id_tiempo representativo por año (ej. mes 12)
    df_tiempo = pd.read_sql("SELECT id_tiempo, anio FROM dim_tiempo WHERE mes = 12", engine)
    df_pred = df_pred.merge(df_tiempo, on="anio", how="left")
    
    df_pred["rendimiento_predicho_t_ha"]      = pred_todas
    df_pred["intervalo_confianza_inferior"]   = pred_todas - mae
    df_pred["intervalo_confianza_superior"]   = pred_todas + mae
    _guardar_predicciones(engine, df_pred, id_version)

    return {"model_name": model_name, "metrics": metrics}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    result = train_and_report()
    print(json.dumps(result, indent=2, ensure_ascii=False))

