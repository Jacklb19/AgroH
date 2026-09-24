"""Tests del modelo de pronóstico y del cargue de producción (sin BD)."""
import numpy as np
import pandas as pd

from models import train_pronostico as tp


def _ctx():
    muni = pd.DataFrame({"id_municipio": ["00001", "00002"], "id_departamento": ["01", "01"],
                         "id_region": [1, 1], "lat": [4.4, 4.5], "lon": [-75.2, -75.1]})
    return {
        "muni": muni,
        "cult": pd.DataFrame({"id_cultivo": [1], "tipo_ciclo": ["transitorio"]}),
        "apt":  pd.DataFrame({"id_municipio": ["00001"], "id_cultivo": [1], "clase_aptitud": ["alta"]}),
        "oni":  pd.DataFrame({"anio": [2019, 2020, 2021, 2022], "oni": [0.5, -0.3, -0.6, -0.8]}),
        "clim": pd.DataFrame({"id_municipio": ["00001"], "clim_lluvia": [1800.0], "clim_temp": [24.0]}),
    }


def _panel():
    rows = []
    for m, base in [("00001", 5.0), ("00002", 8.0)]:
        for i, y in enumerate([2019, 2020, 2021, 2022]):
            rows.append({"id_municipio": m, "id_cultivo": 1, "anio": y,
                         "rendimiento": base + i, "area_cosechada_ha": 100.0})
    return pd.DataFrame(rows)


def test_features_solo_usan_anios_anteriores():
    f = tp.construir_features(_panel(), _ctx(), [2022])
    fila = f[f["id_municipio"] == "00001"].iloc[0]
    # 2022 es el objetivo: el lag debe ser 2021 (=7) y el promedio de 2019-2021 (=6)
    assert fila["y_lag1"] == 7.0
    assert fila["y_lag2"] == 6.0
    assert fila["y_hist_mean"] == 6.0
    assert fila["n_prev"] == 3
    assert fila["aptitud"] == 3
    assert fila["oni"] == -0.8


def test_sin_historia_no_se_pronostica():
    f = tp.construir_features(_panel(), _ctx(), [2019])
    assert f.empty


def test_limpiar_objetivo_descarta_imposibles():
    p = pd.DataFrame({"id_municipio": ["a"] * 12, "id_cultivo": [1] * 12,
                      "anio": range(2010, 2022), "rendimiento": [5.0] * 11 + [900.0],
                      "area_cosechada_ha": [10.0] * 12})
    p.loc[0:10, "rendimiento"] = np.linspace(4.5, 5.5, 11)
    limpio = tp.limpiar_objetivo(p)
    assert np.isnan(limpio["rendimiento"].iloc[-1])
    assert limpio["rendimiento"].iloc[:-1].notna().all()


def test_cargue_no_suma_rendimientos(monkeypatch):
    """Dos semestres del mismo año: el rendimiento es producción total ÷ área total, no la suma."""
    from load import load_facts

    capturado = {}
    monkeypatch.setattr("load.db.upsert", lambda engine, table, df, keys: capturado.setdefault(table, df))
    monkeypatch.setattr(load_facts.pd, "read_sql", lambda sql, engine: (
        pd.DataFrame({"id_cultivo": [1], "nombre_normalizado": ["ARROZ"]}) if "dim_cultivo" in sql
        else pd.DataFrame({"id_tiempo": [7], "anio": [2020]})
    ))
    df = pd.DataFrame({
        "id_municipio": ["73001", "73001"], "cultivo": ["Arroz", "Arroz"], "anio": [2020, 2020],
        "area_sembrada_ha": [100, 100], "area_cosechada_ha": [100, 100],
        "produccion_total_ton": [700, 800], "rendimiento_t_ha": [7.0, 8.0],
    })
    load_facts.load_all_facts(None, df, pd.DataFrame())
    out = capturado["fact_produccion_agricola"].iloc[0]
    assert out["produccion_total_ton"] == 1500
    assert out["rendimiento_t_ha"] == 7.5


def test_limpiar_objetivo_detecta_area_desplomada():
    """Área que cae a una fracción y rendimiento que se dispara: error de reporte (caso Puerto Concordia)."""
    p = pd.DataFrame({
        "id_municipio": ["50450"] * 6, "id_cultivo": [19] * 6, "anio": range(2019, 2025),
        "area_cosechada_ha": [766, 606, 611, 70, 164.5, 100],
        "rendimiento": [5.1, 6.4, 7.0, 37.1, 21.6, 31.9],
    })
    limpio = tp.limpiar_objetivo(p)
    assert limpio["rendimiento"].iloc[:3].notna().all()
    assert limpio["rendimiento"].iloc[3:].isna().all()
