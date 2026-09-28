/* Utilidades compartidas por las APIs que leen el modelo de pronóstico
   (models/train_pronostico.py → tablas model_version y pred_pronostico). */

export const MODELO = "xgboost_pronostico";

/* id de la versión activa del modelo de pronóstico, para usar como subconsulta. */
export const VERSION_ACTIVA = `(
  SELECT id_version FROM model_version
  WHERE nombre_modelo = '${MODELO}' AND activo
  ORDER BY id_version DESC LIMIT 1
)`;

/* Rendimiento real corregido: producción ÷ área cosechada. La columna
   rendimiento_t_ha de datos cargados antes de la corrección está inflada. */
export const RENDIMIENTO_REAL = `CASE WHEN f.area_cosechada_ha > 0 AND f.produccion_total_ton > 0
  THEN f.produccion_total_ton / f.area_cosechada_ha END`;

const MINUSCULAS = new Set(["de", "del", "la", "las", "los", "el", "y", "e", "en"]);

/* "SAN JOSÉ DE CÚCUTA" → "San José de Cúcuta" */
export function titulo(s) {
  return String(s || "")
    .toLowerCase()
    .split(/(\s+|-)/)
    .map((w, i) => (i > 0 && MINUSCULAS.has(w) ? w : w.charAt(0).toUpperCase() + w.slice(1)))
    .join("");
}

/* Variabilidad histórica del rendimiento (coeficiente de variación) → nivel legible. */
export function variabilidad(valores) {
  const v = valores.filter((x) => x != null && Number.isFinite(x));
  if (v.length < 3) return null;
  const media = v.reduce((a, b) => a + b, 0) / v.length;
  const sd = Math.sqrt(v.reduce((a, b) => a + (b - media) ** 2, 0) / (v.length - 1));
  const cv = media > 0 ? sd / media : 0;
  const nivel = cv < 0.15 ? "BAJO" : cv < 0.35 ? "MEDIO" : "ALTO";
  return { nivel, cv: +(cv * 100).toFixed(0), anios: v.length };
}

export function errorBD(ruta, err) {
  console.error(`[${ruta}] DB error:`, err.message);
  return Response.json(
    { error: "La base de datos no está disponible en este momento.", fromDB: false },
    { status: 503 },
  );
}
