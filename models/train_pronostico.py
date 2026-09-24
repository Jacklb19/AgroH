"""
train_pronostico.py — Modelo de PRONÓSTICO de rendimiento (municipio × cultivo × año).

A diferencia de train_rendimiento.py (que ajusta años ya observados), este modelo
solo usa información disponible ANTES de la cosecha que se quiere predecir:

  - rendimiento de años anteriores (lags, promedio histórico, tendencia),
  - contexto del cultivo y del territorio (departamento, región, coordenadas),
  - clima típico del municipio (climatología, no el clima del año objetivo),
  - aptitud del suelo (UPRA),
  - fase ENSO del año objetivo, tratada como ESCENARIO (Neutral / El Niño / La Niña).

El rendimiento objetivo se calcula como producción ÷ área cosechada, porque la
columna rendimiento_t_ha de fact_produccion_agricola quedó inflada al sumar
semestres en load_facts.py (corregido en el cargue).

Formulación: el punto de partida es el rendimiento del año anterior (o el
promedio histórico si falta) y el modelo aprende el CAMBIO esperado en escala
logarítmica, con pérdida absoluta (mediana). El cambio se atenúa con ALPHA, lo
que reduce los errores grandes sin empeorar el error típico frente a la
referencia "igual que el año pasado" (muchos municipios repiten cifras).

Validación: backtest de origen móvil (para cada año Y se entrena con años < Y y
se predice Y). Las métricas y el rango probable por cultivo salen de ese backtest.

Salida (solo inserciones, no modifica datos existentes):
  - model_version: nueva fila 'xgboost_pronostico'.
  - pred_pronostico: backtest (con valor real) y pronósticos por escenario.

Uso:
  python -m models.train_pronostico            # entrena y evalúa, NO escribe
  python -m models.train_pronostico --write    # además guarda en la BD
"""
import argparse
import json
import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sqlalchemy import text

from load.db import get_engine

logger = logging.getLogger(__name__)

MODEL_NAME   = "xgboost_pronostico"
RANDOM_SEED  = 42
BACKTEST_YEARS = [2022, 2023, 2024]
FORECAST_YEARS = [2025, 2026, 2027]
ESCENARIOS = {"Neutral": 0.0, "El Niño": 1.0, "La Niña": -1.0}   # ONI anual típico de un evento moderado
APTITUD = {"alta": 3, "moderada": 2, "marginal": 1, "no_apta": 0}

FEATURES = [
    "y_lag1", "y_lag2", "y_hist_mean", "y_hist_std", "n_prev", "y_trend",
    "area_lag1_log", "crop_nat_lag1", "crop_dept_prior", "crop_nat_prior",
    "id_cultivo", "dept_code", "id_region", "lat", "lon", "permanente",
    "aptitud", "clim_lluvia", "clim_temp", "oni", "oni_lag1",
]

# Elegidos comparando 4 configuraciones en el backtest (ver docs/modelo_pronostico.md)
PARAMS = dict(n_estimators=1200, max_depth=7, learning_rate=0.02, subsample=0.8, colsample_bytree=0.7, min_child_weight=8)
ALPHA  = 0.5


# ── Datos ───────────────────────────────────────────────────────────────
def cargar_datos(engine) -> dict:
    prod = pd.read_sql("""
        SELECT f.id_municipio, f.id_cultivo, t.anio,
               f.area_cosechada_ha,
               CASE WHEN f.area_cosechada_ha > 0 AND f.produccion_total_ton > 0
                    THEN f.produccion_total_ton / f.area_cosechada_ha END AS rendimiento
        FROM fact_produccion_agricola f
        JOIN dim_tiempo t ON t.id_tiempo = f.id_tiempo
    """, engine)
    muni = pd.read_sql("""
        SELECT id_municipio, id_departamento, id_region,
               latitud_centroide AS lat, longitud_centroide AS lon
        FROM dim_municipio
    """, engine)
    cult = pd.read_sql("SELECT id_cultivo, tipo_ciclo FROM dim_cultivo", engine)
    apt  = pd.read_sql("SELECT DISTINCT ON (id_municipio, id_cultivo) id_municipio, id_cultivo, clase_aptitud FROM fact_aptitud_suelo", engine)
    oni  = pd.read_sql("""
        SELECT t.anio, AVG(e.indice_oni) AS oni
        FROM fact_alerta_enso e JOIN dim_tiempo t ON t.id_tiempo = e.id_tiempo
        GROUP BY t.anio
    """, engine)
    clim = pd.read_sql("""
        SELECT id_municipio, AVG(lluvia) AS clim_lluvia, AVG(temp) AS clim_temp FROM (
            SELECT fc.id_municipio, t.anio,
                   SUM(fc.precipitacion_mm) * 12.0 / COUNT(DISTINCT t.mes) AS lluvia,
                   AVG(fc.temperatura_media_c) AS temp
            FROM fact_clima_mensual fc JOIN dim_tiempo t ON t.id_tiempo = fc.id_tiempo
            GROUP BY fc.id_municipio, t.anio
        ) a GROUP BY id_municipio
    """, engine)
    return dict(prod=prod, muni=muni, cult=cult, apt=apt, oni=oni, clim=clim)


def limpiar_objetivo(prod: pd.DataFrame) -> pd.DataFrame:
    """Descarta rendimientos imposibles: > 3.5 desviaciones robustas (MAD) en log dentro del cultivo."""
    p = prod.copy()
    p["log_y"] = np.log(p["rendimiento"])
    med = p.groupby("id_cultivo")["log_y"].transform("median")
    mad = p.groupby("id_cultivo")["log_y"].transform(lambda s: (s - s.median()).abs().median())
    z = (p["log_y"] - med) / (1.4826 * mad.replace(0, np.nan))
    p.loc[z.abs() > 3.5, "rendimiento"] = np.nan
    p.loc[p["rendimiento"] > 500, "rendimiento"] = np.nan

    # Error típico de reporte: el área cosechada se desploma mientras la producción se
    # mantiene, y el rendimiento se dispara. Referencia: años "normales" de la misma
    # combinación (área ≥ 50 % de su máximo).
    g = p.groupby(["id_municipio", "id_cultivo"])
    area_max = g["area_cosechada_ha"].transform("max")
    normal = p["area_cosechada_ha"] >= 0.5 * area_max
    ref = p["rendimiento"].where(normal).groupby([p["id_municipio"], p["id_cultivo"]]).transform("median")
    salto = (p["area_cosechada_ha"] < 0.4 * area_max) & (p["rendimiento"] > 2.5 * ref)
    p.loc[salto, "rendimiento"] = np.nan
    return p.drop(columns="log_y")


# ── Features ────────────────────────────────────────────────────────────
def construir_features(panel: pd.DataFrame, ctx: dict, anios_objetivo) -> pd.DataFrame:
    """
    panel: filas (id_municipio, id_cultivo, anio, rendimiento, area_cosechada_ha) con la
    historia conocida. Devuelve una fila por combinación × año objetivo, usando SOLO
    información de años anteriores al objetivo.
    """
    hist = panel.dropna(subset=["rendimiento"])
    combos = panel[["id_municipio", "id_cultivo"]].drop_duplicates()
    filas = []
    for Y in anios_objetivo:
        prev = hist[hist["anio"] < Y]
        if prev.empty:
            continue
        g = prev.groupby(["id_municipio", "id_cultivo"])
        agg = g["rendimiento"].agg(y_hist_mean="mean", y_hist_std="std", n_prev="count").reset_index()
        last = prev[prev["anio"] == Y - 1][["id_municipio", "id_cultivo", "rendimiento", "area_cosechada_ha"]] \
            .rename(columns={"rendimiento": "y_lag1", "area_cosechada_ha": "area_lag1"})
        last2 = prev[prev["anio"] == Y - 2][["id_municipio", "id_cultivo", "rendimiento"]].rename(columns={"rendimiento": "y_lag2"})
        f = combos.merge(agg, how="left").merge(last, how="left").merge(last2, how="left")
        f["anio"] = Y
        f["y_trend"] = f["y_lag1"] / f["y_hist_mean"]
        f["area_lag1_log"] = np.log1p(f["area_lag1"])

        nat_lag = prev[prev["anio"] == Y - 1].groupby("id_cultivo")["rendimiento"].median().rename("crop_nat_lag1")
        nat_prior = prev.groupby("id_cultivo")["rendimiento"].median().rename("crop_nat_prior")
        f = f.merge(nat_lag, on="id_cultivo", how="left").merge(nat_prior, on="id_cultivo", how="left")
        dept = prev.merge(ctx["muni"][["id_municipio", "id_departamento"]], on="id_municipio") \
            .groupby(["id_departamento", "id_cultivo"])["rendimiento"].median().rename("crop_dept_prior").reset_index()
        f = f.merge(ctx["muni"], on="id_municipio", how="left").merge(dept, on=["id_departamento", "id_cultivo"], how="left")
        filas.append(f)

    if not filas:
        return pd.DataFrame(columns=["id_municipio", "id_cultivo", "anio"] + FEATURES)
    df = pd.concat(filas, ignore_index=True)
    df = df.merge(ctx["cult"], on="id_cultivo", how="left")
    df["permanente"] = (df["tipo_ciclo"] == "permanente").astype(int)
    df = df.merge(ctx["apt"], on=["id_municipio", "id_cultivo"], how="left")
    df["aptitud"] = df["clase_aptitud"].map(APTITUD)
    df = df.merge(ctx["clim"], on="id_municipio", how="left")
    oni = ctx["oni"].set_index("anio")["oni"]
    df["oni"] = df["anio"].map(oni)
    df["oni_lag1"] = (df["anio"] - 1).map(oni)
    df["dept_code"] = pd.to_numeric(df["id_departamento"], errors="coerce")
    # Solo tiene sentido pronosticar combinaciones con al menos un año de historia
    return df[df["n_prev"].fillna(0) > 0].reset_index(drop=True)


# ── Modelo ──────────────────────────────────────────────────────────────
def _modelo(params):
    from xgboost import XGBRegressor
    return XGBRegressor(objective="reg:absoluteerror", base_score=0.0, tree_method="hist",
                        random_state=RANDOM_SEED, n_jobs=-1, **params)


def _base(df):
    """Punto de partida: rendimiento del año anterior o, si falta, el promedio histórico."""
    return df["y_lag1"].fillna(df["y_hist_mean"])


def _fit(params, df):
    return _modelo(params).fit(_X(df), np.log(df["rendimiento"] / _base(df)))


def _predict(model, df):
    return _base(df).to_numpy() * np.exp(ALPHA * model.predict(_X(df)))


def _X(df):
    return df[FEATURES].apply(pd.to_numeric, errors="coerce").astype(float)


def _metricas(y, yhat) -> dict:
    y, yhat = np.asarray(y, float), np.asarray(yhat, float)
    err = np.abs(y - yhat)
    ss_res = ((y - yhat) ** 2).sum()
    ss_tot = ((y - y.mean()) ** 2).sum()
    return {
        "r2":   float(1 - ss_res / ss_tot),
        "mae":  float(err.mean()),
        "rmse": float(np.sqrt(((y - yhat) ** 2).mean())),
        "error_relativo_mediano": float(np.median(err / np.maximum(y, 1e-6))),
        "n":    int(len(y)),
    }


def backtest(panel, ctx, params):
    """Origen móvil: para cada año Y entrena con objetivos < Y y predice Y."""
    anios = sorted(panel["anio"].unique())
    feats = construir_features(panel, ctx, anios)
    feats = feats.merge(panel[["id_municipio", "id_cultivo", "anio", "rendimiento"]],
                        on=["id_municipio", "id_cultivo", "anio"], how="left")
    salida = []
    for Y in BACKTEST_YEARS:
        tr = feats[(feats["anio"] < Y) & feats["rendimiento"].notna()]
        te = feats[(feats["anio"] == Y) & feats["rendimiento"].notna()].copy()
        te["yhat"] = _predict(_fit(params, tr), te)
        salida.append(te)
    return feats, pd.concat(salida, ignore_index=True)


def rangos_por_cultivo(bt: pd.DataFrame) -> pd.DataFrame:
    """Cuantiles 5 % y 95 % del error logarítmico por cultivo (rango probable del 90 %)."""
    bt = bt.assign(lr=np.log(bt["rendimiento"] / bt["yhat"]))
    g_lo, g_hi = bt["lr"].quantile(0.05), bt["lr"].quantile(0.95)
    q = bt.groupby("id_cultivo")["lr"].agg(n="count", q05=lambda s: s.quantile(0.05), q95=lambda s: s.quantile(0.95)).reset_index()
    q.loc[q["n"] < 40, ["q05", "q95"]] = [g_lo, g_hi]
    return q[["id_cultivo", "q05", "q95"]], (float(g_lo), float(g_hi))


def _shap_top(model, X: pd.DataFrame, yhat: np.ndarray):
    """Top-3 factores que mueven el pronóstico respecto al punto de partida
    (xgboost pred_contribs en escala log, convertidos a t/ha aproximadas)."""
    from xgboost import DMatrix
    contrib = ALPHA * model.get_booster().predict(DMatrix(X), pred_contribs=True)[:, :-1]
    top = []
    for i in range(contrib.shape[0]):
        idx = np.argsort(np.abs(contrib[i]))[::-1][:3]
        top.append([{"feature": FEATURES[j], "shap": round(float(yhat[i] * (1 - np.exp(-contrib[i, j]))), 4)}
                    for j in idx if abs(contrib[i, j]) > 1e-4])
    return top


def pronosticar(panel, ctx, params, rangos, rango_global):
    """Entrena con toda la historia y pronostica FORECAST_YEARS de forma recursiva."""
    anios = sorted(panel["anio"].unique())
    feats = construir_features(panel, ctx, anios)
    feats = feats.merge(panel[["id_municipio", "id_cultivo", "anio", "rendimiento"]],
                        on=["id_municipio", "id_cultivo", "anio"], how="left")
    tr = feats[feats["rendimiento"].notna()]
    model = _fit(params, tr)

    importancia = sorted(zip(FEATURES, model.feature_importances_.tolist()), key=lambda kv: -kv[1])
    trabajo = panel.copy()
    salida = []
    for Y in FORECAST_YEARS:
        base = construir_features(trabajo, ctx, [Y])
        # Si el ONI del año ya se conoce completo (año cerrado), no hay escenarios: se usa el real.
        oni_real = ctx["oni"].set_index("anio")["oni"].get(Y)
        escenarios = {"Neutral": oni_real} if Y < datetime.now().year and oni_real is not None else ESCENARIOS
        for esc, oni in escenarios.items():
            f = base.copy()
            f["oni"] = oni
            X = _X(f)
            yhat = _predict(model, f)
            f["yhat"] = yhat
            f["base"] = _base(f)
            f["escenario"] = esc
            f["shap_top"] = _shap_top(model, X, yhat)
            salida.append(f)
            if esc == "Neutral":
                # Recursivo: el pronóstico neutral alimenta los lags del año siguiente
                nuevo = f[["id_municipio", "id_cultivo", "anio"]].assign(rendimiento=yhat, area_cosechada_ha=f["area_lag1"])
                trabajo = pd.concat([trabajo, nuevo], ignore_index=True)

    out = pd.concat(salida, ignore_index=True).merge(rangos, on="id_cultivo", how="left")
    out["q05"] = out["q05"].fillna(rango_global[0])
    out["q95"] = out["q95"].fillna(rango_global[1])
    out["lim_inf"] = out["yhat"] * np.exp(out["q05"])
    out["lim_sup"] = out["yhat"] * np.exp(out["q95"])
    return out, importancia


# ── Persistencia (solo inserciones) ─────────────────────────────────────
DDL = """
CREATE TABLE IF NOT EXISTS pred_pronostico (
    id                  BIGSERIAL PRIMARY KEY,
    id_version          INT NOT NULL REFERENCES model_version(id_version),
    id_municipio        CHAR(5) NOT NULL REFERENCES dim_municipio(id_municipio),
    id_cultivo          INT NOT NULL REFERENCES dim_cultivo(id_cultivo),
    anio                SMALLINT NOT NULL,
    tipo                VARCHAR(12) NOT NULL CHECK (tipo IN ('backtest', 'pronostico')),
    escenario           VARCHAR(10) NOT NULL,
    rendimiento_predicho DOUBLE PRECISION NOT NULL,
    limite_inferior     DOUBLE PRECISION,
    limite_superior     DOUBLE PRECISION,
    rendimiento_real    DOUBLE PRECISION,
    shap_top            JSONB,
    UNIQUE (id_version, id_municipio, id_cultivo, anio, escenario)
);
CREATE INDEX IF NOT EXISTS idx_pred_pronostico_consulta
    ON pred_pronostico (id_version, id_municipio, id_cultivo, anio);
"""


def guardar(engine, metricas, bt, fc):
    from psycopg2.extras import execute_values

    with engine.begin() as conn:
        conn.execute(text(DDL))
        conn.execute(text("UPDATE model_version SET activo = FALSE WHERE nombre_modelo = :n"), {"n": MODEL_NAME})
        id_version = conn.execute(text("""
            INSERT INTO model_version (nombre_modelo, fecha_entrenamiento, metricas_json, activo)
            VALUES (:n, :f, CAST(:m AS JSONB), TRUE) RETURNING id_version
        """), {"n": MODEL_NAME, "f": datetime.now(timezone.utc), "m": json.dumps(metricas, ensure_ascii=False)}).scalar()

    filas = [
        (id_version, r.id_municipio, int(r.id_cultivo), int(r.anio), "backtest", "Real",
         float(r.yhat), float(r.lim_inf), float(r.lim_sup), float(r.rendimiento), None)
        for r in bt.itertuples()
    ] + [
        (id_version, r.id_municipio, int(r.id_cultivo), int(r.anio), "pronostico", r.escenario,
         float(r.yhat), float(r.lim_inf), float(r.lim_sup), None, json.dumps(r.shap_top, ensure_ascii=False))
        for r in fc.itertuples()
    ]
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            execute_values(cur, """
                INSERT INTO pred_pronostico (id_version, id_municipio, id_cultivo, anio, tipo, escenario,
                    rendimiento_predicho, limite_inferior, limite_superior, rendimiento_real, shap_top)
                VALUES %s
            """, filas, page_size=5000)
        raw.commit()
    finally:
        raw.close()
    logger.info("pred_pronostico: %s filas insertadas (versión %s)", len(filas), id_version)
    return id_version


# ── Orquestación ────────────────────────────────────────────────────────
def train_and_forecast(engine=None, write: bool = False) -> dict:
    engine = engine or get_engine()
    ctx = cargar_datos(engine)
    panel = limpiar_objetivo(ctx["prod"])
    logger.info("Panel: %s filas, %s con rendimiento válido", len(panel), panel["rendimiento"].notna().sum())

    params = PARAMS
    _, bt = backtest(panel, ctx, params)
    rangos, rango_global = rangos_por_cultivo(bt)
    bt = bt.merge(rangos, on="id_cultivo", how="left")
    bt["lim_inf"] = bt["yhat"] * np.exp(bt["q05"].fillna(rango_global[0]))
    bt["lim_sup"] = bt["yhat"] * np.exp(bt["q95"].fillna(rango_global[1]))

    naive = bt.dropna(subset=["y_lag1"])
    metricas = {
        "tipo": "pronostico_un_anio_adelante",
        "validacion": f"backtest de origen móvil, años {BACKTEST_YEARS}",
        "modelo": _metricas(bt["rendimiento"], bt["yhat"]),
        "referencias": {
            "anio_anterior": _metricas(naive["rendimiento"], naive["y_lag1"]),
            "modelo_mismas_filas": _metricas(naive["rendimiento"], naive["yhat"]),
            "promedio_historico": _metricas(bt["rendimiento"], bt["y_hist_mean"]),
        },
        "cobertura_rango_90": float(((bt["rendimiento"] >= bt["lim_inf"]) & (bt["rendimiento"] <= bt["lim_sup"])).mean()),
        "params": params,
        "alpha": ALPHA,
        "features": FEATURES,
        "escenarios_oni": ESCENARIOS,
        "anios_datos": [int(panel["anio"].min()), int(panel["anio"].max())],
        "anios_pronostico": FORECAST_YEARS,
    }
    fc, importancia = pronosticar(panel, ctx, params, rangos, rango_global)
    metricas["importancia"] = [{"feature": f, "peso": round(v, 4)} for f, v in importancia[:10]]
    metricas["combinaciones_pronosticadas"] = int(fc[["id_municipio", "id_cultivo"]].drop_duplicates().shape[0])

    logger.info("Métricas: %s", json.dumps(metricas["modelo"]))
    id_version = guardar(engine, metricas, bt, fc) if write else None
    return {"metricas": metricas, "id_version": id_version, "backtest": bt, "pronostico": fc}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true", help="Guardar versión y pronósticos en la BD")
    args = ap.parse_args()
    r = train_and_forecast(write=args.write)
    print(json.dumps({k: v for k, v in r["metricas"].items() if k not in ("features",)}, ensure_ascii=False, indent=2))
    print("id_version:", r["id_version"])
