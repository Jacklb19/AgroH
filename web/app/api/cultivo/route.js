import pool from "@/lib/db";
import { VERSION_ACTIVA, RENDIMIENTO_REAL, titulo, errorBD } from "@/lib/modelo";

export const dynamic = "force-dynamic";

/* Sin ?id: cultivos con más municipios pronosticados (para el selector).
   Con ?id=<id_cultivo>&anio=<año>: serie nacional (real, backtest y pronóstico),
   municipios con mayor rendimiento esperado y cuántos suben o bajan. */
export async function GET(request) {
  const q = new URL(request.url).searchParams;
  const id = parseInt(q.get("id"), 10);
  const anio = parseInt(q.get("anio"), 10) || new Date().getFullYear();

  try {
    if (!id) {
      const { rows } = await pool.query(`
        SELECT c.id_cultivo, c.nombre_cultivo, COUNT(DISTINCT p.id_municipio)::int AS municipios
        FROM pred_pronostico p JOIN dim_cultivo c ON c.id_cultivo = p.id_cultivo
        WHERE p.id_version = ${VERSION_ACTIVA} AND p.tipo = 'pronostico'
        GROUP BY c.id_cultivo, c.nombre_cultivo
        ORDER BY municipios DESC LIMIT 12
      `);
      return Response.json(rows.map((r) => ({ id: r.id_cultivo, nombre: r.nombre_cultivo, municipios: r.municipios })));
    }

    const [real, modelo, top, cambios] = await Promise.all([
      pool.query(`
        SELECT t.anio, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ${RENDIMIENTO_REAL}) AS real,
               COUNT(*)::int AS municipios
        FROM fact_produccion_agricola f JOIN dim_tiempo t ON t.id_tiempo = f.id_tiempo
        WHERE f.id_cultivo = $1 AND ${RENDIMIENTO_REAL} IS NOT NULL
        GROUP BY t.anio ORDER BY t.anio
      `, [id]),
      pool.query(`
        SELECT anio, tipo,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rendimiento_predicho) AS pred,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rendimiento_real)     AS real_mismas_filas,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY limite_inferior)      AS low,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY limite_superior)      AS high
        FROM pred_pronostico
        WHERE id_version = ${VERSION_ACTIVA} AND id_cultivo = $1
          AND (tipo = 'backtest' OR escenario = 'Neutral')
        GROUP BY anio, tipo ORDER BY anio
      `, [id]),
      /* Ranking solo con valores creíbles: se excluyen pronósticos por encima del
         percentil 95 de lo registrado para el cultivo (suelen venir de errores de reporte). */
      pool.query(`
        WITH lim AS (
          SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ${RENDIMIENTO_REAL}) AS p95
          FROM fact_produccion_agricola f WHERE f.id_cultivo = $1 AND ${RENDIMIENTO_REAL} IS NOT NULL
        )
        SELECT m.nombre_municipio, m.nombre_departamento, p.rendimiento_predicho AS yhat
        FROM pred_pronostico p
        JOIN dim_municipio m ON m.id_municipio = p.id_municipio
        CROSS JOIN lim
        WHERE p.id_version = ${VERSION_ACTIVA} AND p.tipo = 'pronostico' AND p.escenario = 'Neutral'
          AND p.id_cultivo = $1 AND p.anio = $2 AND p.rendimiento_predicho <= lim.p95
          AND (SELECT COUNT(*) FROM fact_produccion_agricola f
               WHERE f.id_municipio = p.id_municipio AND f.id_cultivo = p.id_cultivo
                 AND ${RENDIMIENTO_REAL} IS NOT NULL) >= 3
        ORDER BY p.rendimiento_predicho DESC LIMIT 8
      `, [id, anio]),
      pool.query(`
        WITH ultimo AS (
          SELECT DISTINCT ON (f.id_municipio) f.id_municipio, ${RENDIMIENTO_REAL} AS real
          FROM fact_produccion_agricola f JOIN dim_tiempo t ON t.id_tiempo = f.id_tiempo
          WHERE f.id_cultivo = $1 AND ${RENDIMIENTO_REAL} IS NOT NULL
          ORDER BY f.id_municipio, t.anio DESC
        )
        SELECT
          SUM(CASE WHEN p.rendimiento_predicho > u.real * 1.05 THEN 1 ELSE 0 END)::int AS sube,
          SUM(CASE WHEN p.rendimiento_predicho < u.real * 0.95 THEN 1 ELSE 0 END)::int AS baja,
          COUNT(*)::int AS total
        FROM pred_pronostico p JOIN ultimo u ON u.id_municipio = p.id_municipio
        WHERE p.id_version = ${VERSION_ACTIVA} AND p.tipo = 'pronostico' AND p.escenario = 'Neutral'
          AND p.id_cultivo = $1 AND p.anio = $2
      `, [id, anio]),
    ]);

    const num = (v) => (v == null ? null : +parseFloat(v).toFixed(2));
    const porAnio = new Map();
    const fila = (a) => porAnio.get(a) || porAnio.set(a, { anio: a }).get(a);
    real.rows.forEach((r) => Object.assign(fila(r.anio), { real: num(r.real), municipios: r.municipios }));
    modelo.rows.forEach((r) => {
      if (r.tipo === "backtest") Object.assign(fila(r.anio), { backtest: num(r.pred) });
      else Object.assign(fila(r.anio), { pronostico: num(r.pred), low: num(r.low), high: num(r.high) });
    });
    const c = cambios.rows[0] || { sube: 0, baja: 0, total: 0 };

    return Response.json({
      fromDB: true,
      anio,
      serie: [...porAnio.values()].sort((a, b) => a.anio - b.anio),
      top: top.rows.map((r) => ({
        municipio: `${titulo(r.nombre_municipio)}, ${titulo(r.nombre_departamento)}`,
        rendimiento: num(r.yhat),
      })),
      cambios: { sube: c.sube, baja: c.baja, estable: c.total - c.sube - c.baja, total: c.total },
    });
  } catch (err) {
    return errorBD("cultivo", err);
  }
}
