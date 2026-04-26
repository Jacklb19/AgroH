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
        SUM(fc.precipitacion_mm) AS precipitacion_mm,
        AVG(fc.temperatura_media_c) AS temperatura_media_c,
        AVG(fc.humedad_relativa_pct) AS humedad_relativa_pct,
        AVG(fc.brillo_solar_horas_dia) AS brillo_solar_horas_dia
    FROM fact_clima_mensual fc
    JOIN dim_tiempo dt ON dt.id_tiempo = fc.id_tiempo
    GROUP BY fc.id_municipio, dt.anio
),
enso_anual AS (
    SELECT
        dt.anio,
        dr.nombre_region,
        MODE() WITHIN GROUP (ORDER BY fe.fase_enso) AS fase_enso_dominante
    FROM fact_alerta_enso fe
    JOIN dim_tiempo dt ON dt.id_tiempo = fe.id_tiempo
    JOIN dim_region_natural dr ON dr.id_region = fe.id_region
    GROUP BY dt.anio, dr.nombre_region
)
SELECT
    fp.id_municipio,
    fp.id_cultivo,
    dt.anio,
    fp.rendimiento_t_ha,
    fp.area_sembrada_ha,
    fp.area_cosechada_ha,
    fp.produccion_total_ton,
    ca.precipitacion_mm,
    ca.temperatura_media_c,
    ca.humedad_relativa_pct,
    ca.brillo_solar_horas_dia,
    CASE WHEN ea.fase_enso_dominante = 'El Niño' THEN 1
         WHEN ea.fase_enso_dominante = 'La Niña' THEN -1
         ELSE 0 END AS fase_enso_num,
    CASE WHEN fas.clase_aptitud = 'alta' THEN 3
         WHEN fas.clase_aptitud = 'moderada' THEN 2
         WHEN fas.clase_aptitud = 'marginal' THEN 1
         ELSE 0 END AS aptitud_suelo_num
FROM fact_produccion_agricola fp
JOIN dim_tiempo dt ON dt.id_tiempo = fp.id_tiempo
JOIN dim_municipio dm ON dm.id_municipio = fp.id_municipio
LEFT JOIN clima_anual ca
    ON ca.id_municipio = fp.id_municipio
   AND ca.anio = dt.anio
LEFT JOIN dim_region_natural drn ON drn.id_region = dm.id_region
LEFT JOIN enso_anual ea
    ON ea.anio = dt.anio
   AND ea.nombre_region = drn.nombre_region
LEFT JOIN fact_aptitud_suelo fas
    ON fas.id_municipio = fp.id_municipio
   AND fas.id_cultivo = fp.id_cultivo

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
        "temperatura_media_c",
        "humedad_relativa_pct",
        "brillo_solar_horas_dia",
        "fase_enso_num",
        "aptitud_suelo_num",
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
    pred_test = model.predict(X_test)

    metrics = {
        "mae": float(mean_absolute_error(y_test, pred_test)),
        "rmse": float(mean_squared_error(y_test, pred_test) ** 0.5),
        "r2": float(r2_score(y_test, pred_test)),
        "n_train": int(len(X_train)),
        "n_test": int(len(X_test)),
    }
    logger.info("Modelo %s entrenado con métricas: %s", model_name, metrics)
    
    # --- PERSISTENCIA ---
    save_model_to_db(engine, model, model_name, metrics, df, feature_cols)
    
    return {"model_name": model_name, "metrics": metrics}


def save_model_to_db(engine, model, model_name, metrics, df_full, feature_cols):
    import joblib
    from datetime import datetime
    from sqlalchemy import text
    import os

    # 1. Guardar binario
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_dir = "models/saved"
    os.makedirs(model_dir, exist_ok=True)
    model_path = f"{model_dir}/{model_name}_{timestamp}.joblib"
    joblib.dump(model, model_path)

    # 2. Registrar versión
    with engine.begin() as conn:
        # Desactivar versiones previas
        conn.execute(text("UPDATE model_version SET activo = false WHERE nombre_modelo = :name"), {"name": model_name})
        
        # Insertar nueva
        res = conn.execute(text("""
            INSERT INTO model_version (nombre_modelo, metricas_json, activo)
            VALUES (:name, :metrics, true)
            RETURNING id_version
        """), {
            "name": model_name,
            "metrics": json.dumps(metrics)
        })
        id_version = res.fetchone()[0]

        # 3. Guardar predicciones históricas (para auditoría/visualización)
        X_full = df_full[feature_cols].fillna(0)
        df_full["rendimiento_predicho_t_ha"] = model.predict(X_full)
        df_full["id_version"] = id_version

        logger.info("Mapeando tiempos para persistencia de predicciones...")
        # Mapeamos a un id_tiempo representativo por año (mes 6) para la visualización anual
        df_tiempo = pd.read_sql("SELECT id_tiempo, anio FROM dim_tiempo WHERE mes = 6", engine)
        df_to_load = df_full.merge(df_tiempo, on="anio", how="inner")
        
        load_cols = ["id_municipio", "id_cultivo", "id_tiempo", "rendimiento_predicho_t_ha", "id_version"]
        
        logger.info(f"Insertando {len(df_to_load)} predicciones en pred_rendimiento...")
        # Limpiar predicciones previas de este modelo si existen
        conn.execute(text("DELETE FROM pred_rendimiento WHERE id_version IN (SELECT id_version FROM model_version WHERE nombre_modelo = :name)"), {"name": model_name})
        
        df_to_load[load_cols].to_sql(
            "pred_rendimiento", 
            engine, 
            if_exists="append", 
            index=False, 
            chunksize=100
        )

    logger.info("Predicciones y versión del modelo guardadas exitosamente.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    result = train_and_report()
    print(json.dumps(result, indent=2, ensure_ascii=False))
