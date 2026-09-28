import pool from "@/lib/db";
import { VERSION_ACTIVA, RENDIMIENTO_REAL, titulo, variabilidad, errorBD } from "@/lib/modelo";

/* Pronóstico de rendimiento para municipio × cultivo × año (models/train_pronostico.py).
   Body: { id_municipio, id_cultivo, anio }.
   Nunca inventa valores: si la combinación no tiene pronóstico responde sin_datos. */
export async function POST(request) {
  const { id_municipio, id_cultivo, anio } = await request.json();
  const muni = String(id_municipio || "").trim();
  const cultivo = parseInt(id_cultivo, 10);
  const year = parseInt(anio, 10);
  if (!muni || !cultivo || !year) {
    return Response.json({ error: "Faltan municipio, cultivo o año." }, { status: 400 });
  }

  try {
    const [info, pron, historia, precision] = await Promise.all([
      pool.query(`
        SELECT m.nombre_municipio, m.nombre_departamento, c.nombre_cultivo, c.tipo_ciclo
        FROM dim_municipio m, dim_cultivo c
        WHERE m.id_municipio = $1 AND c.id_cultivo = $2
      `, [muni, cultivo]),
      pool.query(`
        SELECT escenario, rendimiento_predicho AS yhat, limite_inferior AS low,
               limite_superior AS high, shap_top
        FROM pred_pronostico
        WHERE id_version = ${VERSION_ACTIVA} AND tipo = 'pronostico'
          AND id_municipio = $1 AND id_cultivo = $2 AND anio = $3
      `, [muni, cultivo, year]),
      pool.query(`
        SELECT t.anio, ${RENDIMIENTO_REAL} AS real, b.rendimiento_predicho AS backtest
        FROM fact_produccion_agricola f
        JOIN dim_tiempo t ON t.id_tiempo = f.id_tiempo
        LEFT JOIN pred_pronostico b
          ON b.id_version = ${VERSION_ACTIVA} AND b.tipo = 'backtest'
         AND b.id_municipio = f.id_municipio AND b.id_cultivo = f.id_cultivo AND b.anio = t.anio
        WHERE f.id_municipio = $1 AND f.id_cultivo = $2
        ORDER BY t.anio
      `, [muni, cultivo]),
      pool.query(`
        SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (
                 ORDER BY ABS(rendimiento_real - rendimiento_predicho) / NULLIF(rendimiento_real, 0)
               ) AS error_rel,
               AVG(CASE WHEN rendimiento_real BETWEEN limite_inferior AND limite_superior THEN 1.0 ELSE 0 END) AS cobertura,
               COUNT(*)::int AS n
        FROM pred_pronostico
        WHERE id_version = ${VERSION_ACTIVA} AND tipo = 'backtest' AND id_cultivo = $1
      `, [cultivo]),
    ]);

    if (!info.rows.length) {
      return Response.json({ fromDB: true, sin_datos: true, motivo: "Municipio o cultivo no encontrado." });
    }
    const i = info.rows[0];
    const base = {
      fromDB: true,
      municipio: { id: muni, nombre: titulo(i.nombre_municipio), departamento: titulo(i.nombre_departamento) },
      cultivo: { id: cultivo, nombre: i.nombre_cultivo, ciclo: i.tipo_ciclo },
      anio: year,
    };
    if (!pron.rows.length) {
      return Response.json({ ...base, sin_datos: true, motivo: "No hay pronóstico para esta combinación y año." });
    }

    const num = (v) => (v == null ? null : +parseFloat(v).toFixed(2));
    const escenarios = pron.rows.map((r) => ({
      escenario: r.escenario, yhat: num(r.yhat), low: num(r.low), high: num(r.high),
    }));
    const neutral = pron.rows.find((r) => r.escenario === "Neutral") || pron.rows[0];
    const serie = historia.rows.map((r) => ({ anio: r.anio, real: num(r.real), backtest: num(r.backtest) }));
    const ultimo = [...serie].reverse().find((r) => r.real != null) || null;
    const p = precision.rows[0];

    return Response.json({
      ...base,
      yhat: num(neutral.yhat),
      low: num(neutral.low),
      high: num(neutral.high),
      escenarios,
      shap: neutral.shap_top || [],
      historia: serie,
      ultimo_real: ultimo ? { anio: ultimo.anio, valor: ultimo.real } : null,
      variabilidad: variabilidad(serie.map((r) => r.real)),
      precision_cultivo: p && p.n >= 20
        ? { error_relativo: +(parseFloat(p.error_rel) * 100).toFixed(0), cobertura: +(parseFloat(p.cobertura) * 100).toFixed(0), n: p.n }
        : null,
    });
  } catch (err) {
    return errorBD("prediccion", err);
  }
}
