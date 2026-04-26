import json
import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from load.db import get_engine
from models.build_features import build_ml_features

logger = logging.getLogger(__name__)


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
                "nombre_modelo": model_name,
                "fecha_entrenamiento": datetime.now(timezone.utc).isoformat(),
                "metricas_json": json.dumps(metrics, ensure_ascii=False),
                "activo": True,
            },
        )
        row = result.fetchone()
    id_version = int(row[0]) if row else None
    logger.info("model_version: id=%s (%s) registrado", id_version, model_name)
    return id_version


def _guardar_predicciones(engine, df_pred: pd.DataFrame, id_version: int | None) -> None:
    """
    Inserta o actualiza pred_rendimiento con las predicciones del modelo.
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
    for column in cols:
        if column not in df_pred.columns:
            df_pred[column] = None

    df_out = df_pred[cols].drop_duplicates(subset=["id_municipio", "id_cultivo", "id_tiempo"])
    upsert(engine, "pred_rendimiento", df_out, ["id_municipio", "id_cultivo", "id_tiempo"])
    logger.info("pred_rendimiento: %s predicciones guardadas", len(df_out))


def _temporal_train_test_split(df: pd.DataFrame, min_test_years: int = 2):
    years = sorted(int(year) for year in df["anio"].dropna().unique())
    if len(years) < 3:
        raise ValueError("No hay suficientes anios para una validacion temporal robusta.")

    n_test_years = max(min_test_years, int(np.ceil(len(years) * 0.2)))
    n_test_years = min(n_test_years, max(1, len(years) - 1))
    test_years = years[-n_test_years:]

    train_mask = ~df["anio"].isin(test_years)
    test_mask = df["anio"].isin(test_years)
    if not train_mask.any() or not test_mask.any():
        raise ValueError("No fue posible construir particiones temporales de entrenamiento y prueba.")

    return train_mask, test_mask, test_years


def _feature_importance(model, feature_cols):
    importances = getattr(model, "feature_importances_", None)
    if importances is None:
        return {}
    pairs = sorted(
        zip(feature_cols, [float(value) for value in importances]),
        key=lambda item: item[1],
        reverse=True,
    )
    return {name: value for name, value in pairs[:10]}


def _historical_baseline(df: pd.DataFrame, train_mask: pd.Series, y_train: pd.Series) -> pd.Series:
    baseline = (
        df["rendimiento_lag_1"]
        .fillna(df["rendimiento_promedio_3"])
    )
    cultivo_mean = df.loc[train_mask].groupby("id_cultivo")["rendimiento_t_ha"].mean()
    baseline = baseline.fillna(df["id_cultivo"].map(cultivo_mean))
    baseline = baseline.fillna(float(y_train.mean()))
    return baseline


def train_and_report(engine=None) -> dict:
    """
    Entrena un modelo tabular de rendimiento agricola con validacion temporal.
    Persiste metricas, baselines y predicciones historicas.
    """
    if engine is None:
        engine = get_engine()

    df = build_ml_features(engine)
    if df.empty:
        raise ValueError("No hay datos suficientes para entrenar el modelo")

    df = df.dropna(subset=["rendimiento_t_ha"]).copy()

    feature_cols = [
        "anio",
        "id_cultivo",
        "id_region",
        "id_departamento_num",
        "id_municipio_num",
        "area_sembrada_ha",
        "area_cosechada_ha",
        "lluvia_acumulada_anual",
        "temp_promedio_anual",
        "temp_maxima_anual",
        "temp_minima_anual",
        "humedad_relativa_anual",
        "brillo_solar_anual",
        "precio_promedio_anual_cop_kg",
        "volumen_total_anual_ton",
        "costo_promedio_insumos_region",
        "n_insumos_sinteticos",
        "aptitud_score",
        "area_cultivos_permanentes_ha",
        "area_cultivos_transitorios_ha",
        "rendimiento_lag_1",
        "rendimiento_promedio_3",
        "area_sembrada_lag_1",
        "lluvia_lag_1",
        "temp_promedio_lag_1",
        "precio_lag_1",
        "costo_insumos_lag_1",
        "variacion_lluvia_interanual",
        "variacion_precio_interanual",
    ]

    X = df[feature_cols].apply(pd.to_numeric, errors="coerce")
    X = X.fillna(X.median(numeric_only=True)).fillna(0)
    y = df["rendimiento_t_ha"].astype(float)

    train_mask, test_mask, test_years = _temporal_train_test_split(df)
    X_train = X.loc[train_mask]
    X_test = X.loc[test_mask]
    y_train = y.loc[train_mask]
    y_test = y.loc[test_mask]

    try:
        from xgboost import XGBRegressor

        model = XGBRegressor(
            n_estimators=400,
            max_depth=8,
            learning_rate=0.04,
            subsample=0.85,
            colsample_bytree=0.85,
            reg_lambda=1.0,
            random_state=42,
            objective="reg:squarederror",
        )
        model_name = "xgboost_rendimiento"
    except ImportError:
        from sklearn.ensemble import GradientBoostingRegressor

        model = GradientBoostingRegressor(random_state=42)
        model_name = "gradient_boosting_rendimiento"

    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    baseline_hist = _historical_baseline(df, train_mask, y_train)
    baseline_train = baseline_hist.loc[train_mask]
    baseline_test = baseline_hist.loc[test_mask]

    residual_train = y_train - baseline_train
    model.fit(X_train, residual_train)
    pred_test = baseline_test + model.predict(X_test)

    mae = float(mean_absolute_error(y_test, pred_test))
    rmse = float(mean_squared_error(y_test, pred_test) ** 0.5)
    r2 = float(r2_score(y_test, pred_test))

    baseline_mean_pred = np.repeat(float(y_train.mean()), len(y_test))
    baseline_mean_mae = float(mean_absolute_error(y_test, baseline_mean_pred))
    baseline_mean_rmse = float(mean_squared_error(y_test, baseline_mean_pred) ** 0.5)

    cultivo_mean = df.loc[train_mask].groupby("id_cultivo")["rendimiento_t_ha"].mean()
    baseline_cultivo_pred = df.loc[test_mask, "id_cultivo"].map(cultivo_mean).fillna(float(y_train.mean()))
    baseline_cultivo_mae = float(mean_absolute_error(y_test, baseline_cultivo_pred))
    baseline_cultivo_rmse = float(mean_squared_error(y_test, baseline_cultivo_pred) ** 0.5)

    baseline_lag1_pred = baseline_test
    baseline_lag1_mae = float(mean_absolute_error(y_test, baseline_lag1_pred))
    baseline_lag1_rmse = float(mean_squared_error(y_test, baseline_lag1_pred) ** 0.5)

    champion_strategy = "lag1_historial" if baseline_lag1_rmse <= rmse else "xgboost_residual"
    champion_test_pred = baseline_lag1_pred if champion_strategy == "lag1_historial" else pred_test
    champion_mae = float(mean_absolute_error(y_test, champion_test_pred))
    champion_rmse = float(mean_squared_error(y_test, champion_test_pred) ** 0.5)
    interval_width = max(champion_mae, float(np.std(y_test - champion_test_pred)))
    metrics = {
        "mae": mae,
        "rmse": rmse,
        "r2": r2,
        "n_train": int(len(X_train)),
        "n_test": int(len(X_test)),
        "split_strategy": "temporal_holdout",
        "test_years": test_years,
        "champion_strategy": champion_strategy,
        "champion_mae": champion_mae,
        "champion_rmse": champion_rmse,
        "baselines": {
            "mean_train": {
                "mae": baseline_mean_mae,
                "rmse": baseline_mean_rmse,
            },
            "mean_by_cultivo": {
                "mae": baseline_cultivo_mae,
                "rmse": baseline_cultivo_rmse,
            },
            "lag1_historial": {
                "mae": baseline_lag1_mae,
                "rmse": baseline_lag1_rmse,
            },
        },
        "top_feature_importance": _feature_importance(model, feature_cols),
    }
    logger.info("Modelo %s entrenado con metricas: %s", model_name, metrics)

    id_version = _registrar_version(engine, model_name, metrics)

    pred_modelo_todas = baseline_hist + model.predict(X)
    pred_todas = baseline_hist if champion_strategy == "lag1_historial" else pred_modelo_todas
    df_pred = df[["id_municipio", "id_cultivo", "id_tiempo"]].copy()
    df_pred["rendimiento_predicho_t_ha"] = pred_todas
    df_pred["intervalo_confianza_inferior"] = pred_todas - interval_width
    df_pred["intervalo_confianza_superior"] = pred_todas + interval_width
    _guardar_predicciones(engine, df_pred, id_version)

    return {"model_name": model_name, "metrics": metrics}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    result = train_and_report()
    print(json.dumps(result, indent=2, ensure_ascii=False))
