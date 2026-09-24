/* Traduce los nombres de variables del modelo (build_features.py) a
   lenguaje claro para el panel "Por qué este resultado". */

const EXACT = {
  lluvia_acumulada_anual:    "Lluvia acumulada del año",
  temp_promedio_anual:       "Temperatura promedio del año",
  humedad_promedio_anual:    "Humedad del aire",
  brillo_solar_promedio:     "Horas de sol",
  lluvia_semestre_a:         "Lluvia de enero a junio",
  lluvia_semestre_b:         "Lluvia de julio a diciembre",
  spi_promedio:              "Índice de sequía (SPI)",
  anomalia_lluvia_pct:       "Lluvia frente a lo normal",
  prob_deficit:              "Probabilidad de sequía",
  prob_exceso:               "Probabilidad de exceso de lluvia",
  es_anio_nino_int:          "Año de El Niño",
  es_anio_nino:              "Año de El Niño",
  precio_promedio_cop_kg:    "Precio de venta del producto",
  precio_insumo_promedio:    "Precio de los insumos",
  clase_aptitud_score:       "Aptitud del suelo para el cultivo",
  id_municipio_enc:          "Características del municipio",
  id_departamento_enc:       "Características del departamento",
  id_region:                 "Región natural",
  rendimiento_t_ha_hist_avg: "Rendimiento histórico de la zona",
  anio:                      "Tendencia de los años",
};

const RULES = [
  [/lluvia|precip/,    "Lluvia"],
  [/temp/,             "Temperatura"],
  [/humedad/,          "Humedad"],
  [/brillo|sol/,       "Horas de sol"],
  [/nino|enso|oni/,    "Fenómeno El Niño / La Niña"],
  [/deficit/,          "Probabilidad de sequía"],
  [/exceso/,           "Probabilidad de exceso de lluvia"],
  [/insumo|fertil/,    "Precio de los insumos"],
  [/precio/,           "Precio de venta"],
  [/aptitud|suelo/,    "Aptitud del suelo"],
  [/area|hect/,        "Área sembrada"],
  [/rendimiento/,      "Rendimiento histórico"],
];

export function labelFeature(key) {
  const k = String(key || "").toLowerCase();
  const lag = k.match(/_lag(\d+)$/);
  const base = lag ? k.replace(/_lag\d+$/, "") : k;
  let label = EXACT[base];
  if (!label) {
    const hit = RULES.find(([re]) => re.test(base));
    label = hit ? hit[1] : base.replace(/_/g, " ").replace(/^\w/, (c) => c.toUpperCase());
  }
  if (lag) label += lag[1] === "1" ? " (año anterior)" : ` (hace ${lag[1]} años)`;
  return label;
}
