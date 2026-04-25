import json
import logging

import pandas as pd

from load.db import get_engine

logger = logging.getLogger(__name__)


TRAIN_SQL = """
WITH clima_anual AS (
    SELECT
        fc.id_municipio,
        dt.anio,
        SUM(fc.precipitacion_mm) AS precipitacion_mm
    FROM fact_clima_mensual fc
    JOIN dim_tiempo dt ON dt.id_tiempo = fc.id_tiempo
    GROUP BY fc.id_municipio, dt.anio
)
SELECT
    fp.id_municipio,
    fp.id_cultivo,
    dt.anio,
    fp.rendimiento_t_ha,
    fp.area_sembrada_ha,
    fp.area_cosechada_ha,
    fp.produccion_total_ton,
    ca.precipitacion_mm
FROM fact_produccion_agricola fp
JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
LEFT JOIN clima_anual ca
    ON ca.id_municipio = fp.id_municipio
   AND ca.anio = dt.anio

"""


def load_training_frame(engine) -> pd.DataFrame:
    return pd.read_sql(TRAIN_SQL, engine)


def train_and_report(engine=None) -> dict:
    """
    Entrena un primer modelo tabular de rendimiento.
    Requiere producción + clima; para un modelo final conviene sumar suelo o precios.
    """
    if engine is None:
        engine = get_engine()

    df = load_training_frame(engine)
    if df.empty:
        raise ValueError("No hay datos suficientes para entrenar el modelo")

    df = df.dropna(subset=["rendimiento_t_ha"])
    feature_cols = [
        "anio",
        "area_sembrada_ha",
        "area_cosechada_ha",
        "produccion_total_ton",
        "precipitacion_mm",
    ]
    X = df[feature_cols].fillna(0)
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
    pred = model.predict(X_test)

    metrics = {
        "mae": float(mean_absolute_error(y_test, pred)),
        "rmse": float(mean_squared_error(y_test, pred) ** 0.5),
        "r2": float(r2_score(y_test, pred)),
        "n_train": int(len(X_train)),
        "n_test": int(len(X_test)),
    }
    logger.info("Modelo %s entrenado con métricas: %s", model_name, metrics)
    return {"model_name": model_name, "metrics": metrics}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    result = train_and_report()
    print(json.dumps(result, indent=2, ensure_ascii=False))
