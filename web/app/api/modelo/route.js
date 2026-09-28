import pool from "@/lib/db";
import { MODELO, VERSION_ACTIVA, errorBD } from "@/lib/modelo";

/* Revalida cada 5 min: evita que la respuesta quede congelada en el build. */
export const revalidate = 300;

/* Versión activa del modelo de pronóstico: métricas reales del backtest,
   años con pronóstico y fase ENSO más reciente (NOAA). */
export async function GET() {
  try {
    const [version, anios, enso, alertas] = await Promise.all([
      pool.query(`
        SELECT id_version, fecha_entrenamiento, metricas_json
        FROM model_version WHERE id_version = ${VERSION_ACTIVA}
      `),
      pool.query(`
        SELECT anio, COUNT(DISTINCT (id_municipio, id_cultivo))::int AS combinaciones,
               COUNT(DISTINCT id_municipio)::int AS municipios,
               COUNT(DISTINCT id_cultivo)::int AS cultivos,
               ARRAY_AGG(DISTINCT escenario) AS escenarios
        FROM pred_pronostico
        WHERE id_version = ${VERSION_ACTIVA} AND tipo = 'pronostico'
        GROUP BY anio ORDER BY anio
      `),
      pool.query(`
        SELECT t.anio, t.mes, t.nombre_mes, e.fase_enso, ROUND(AVG(e.indice_oni)::numeric, 2) AS oni
        FROM fact_alerta_enso e JOIN dim_tiempo t ON t.id_tiempo = e.id_tiempo
        GROUP BY t.anio, t.mes, t.nombre_mes, e.fase_enso
        ORDER BY t.anio DESC, t.mes DESC LIMIT 1
      `),
      pool.query(`
        SELECT metricas_json FROM model_version
        WHERE nombre_modelo = 'xgboost_alerta_climatica' AND activo
        ORDER BY id_version DESC LIMIT 1
      `),
    ]);

    if (!version.rows.length) {
      return Response.json({ fromDB: true, sin_modelo: true, error: `No hay una versión activa de ${MODELO}.` });
    }
    const v = version.rows[0];
    const alerta = alertas.rows[0]?.metricas_json || null;

    return Response.json({
      fromDB: true,
      version: { id: v.id_version, fecha: v.fecha_entrenamiento },
      metricas: v.metricas_json,
      anios: anios.rows,
      enso: enso.rows[0]
        ? { anio: enso.rows[0].anio, mes: enso.rows[0].mes, nombre_mes: enso.rows[0].nombre_mes,
            fase: enso.rows[0].fase_enso, oni: parseFloat(enso.rows[0].oni) }
        : null,
      modelo_alertas: alerta
        ? { exactitud: alerta.report?.accuracy ?? null, recall_alto: alerta.report?.ALTO?.recall ?? null }
        : null,
    });
  } catch (err) {
    return errorBD("modelo", err);
  }
}
