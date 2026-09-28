/* Traduce las variables del modelo de pronóstico (models/train_pronostico.py)
   a lenguaje claro para el panel "Por qué este resultado". */

const ETIQUETAS = {
  y_lag1:          "Rendimiento del año anterior",
  y_lag2:          "Rendimiento de hace dos años",
  y_hist_mean:     "Promedio histórico en el municipio",
  y_hist_std:      "Qué tanto ha variado el rendimiento",
  n_prev:          "Años con datos registrados",
  y_trend:         "Tendencia reciente",
  area_lag1_log:   "Área cosechada el año anterior",
  crop_nat_lag1:   "Rendimiento nacional del cultivo el año anterior",
  crop_dept_prior: "Rendimiento típico en el departamento",
  crop_nat_prior:  "Rendimiento típico nacional del cultivo",
  id_cultivo:      "Tipo de cultivo",
  dept_code:       "Departamento",
  id_region:       "Región natural",
  lat:             "Ubicación (latitud)",
  lon:             "Ubicación (longitud)",
  permanente:      "Cultivo permanente o transitorio",
  aptitud:         "Aptitud del suelo (UPRA)",
  clim_lluvia:     "Lluvia típica del municipio",
  clim_temp:       "Temperatura típica del municipio",
  oni:             "Fenómeno El Niño / La Niña",
  oni_lag1:        "El Niño / La Niña del año anterior",
};

export function labelFeature(key) {
  return ETIQUETAS[key] || String(key || "").replace(/_/g, " ").replace(/^\w/, (c) => c.toUpperCase());
}
