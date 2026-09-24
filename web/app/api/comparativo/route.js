import pool from "@/lib/db";
import { VERSION_ACTIVA, RENDIMIENTO_REAL, titulo, errorBD } from "@/lib/modelo";

export const dynamic = "force-dynamic";

/* Pronóstico (escenario neutral) del mismo cultivo en los municipios del mismo
   departamento. Devuelve los 6 mejores e incluye siempre el municipio consultado. */
const MAX_FILAS = 6;

export async function GET(request) {
  const q = new URL(request.url).searchParams;
  const muni = (q.get("muni") || "").trim();
  const cultivo = parseInt(q.get("cultivo"), 10);
  const anio = parseInt(q.get("anio"), 10);
  if (!muni || !cultivo || !anio) return Response.json({ error: "Faltan parámetros." }, { status: 400 });

  try {
    const { rows } = await pool.query(`
      WITH dep AS (SELECT id_departamento, nombre_departamento FROM dim_municipio WHERE id_municipio = $1),
      ultimo AS (
        SELECT DISTINCT ON (f.id_municipio) f.id_municipio, ${RENDIMIENTO_REAL} AS real
        FROM fact_produccion_agricola f JOIN dim_tiempo t ON t.id_tiempo = f.id_tiempo
        WHERE f.id_cultivo = $2 AND ${RENDIMIENTO_REAL} IS NOT NULL
        ORDER BY f.id_municipio, t.anio DESC
      )
      SELECT m.id_municipio, m.nombre_municipio, dep.nombre_departamento,
             p.rendimiento_predicho AS yhat, u.real AS ultimo_real
      FROM pred_pronostico p
      JOIN dim_municipio m ON m.id_municipio = p.id_municipio
      JOIN dep ON dep.id_departamento = m.id_departamento
      LEFT JOIN ultimo u ON u.id_municipio = p.id_municipio
      WHERE p.id_version = ${VERSION_ACTIVA} AND p.tipo = 'pronostico' AND p.escenario = 'Neutral'
        AND p.id_cultivo = $2 AND p.anio = $3
      ORDER BY p.rendimiento_predicho DESC
    `, [muni, cultivo, anio]);

    const todas = rows.map((r) => ({
      id: r.id_municipio.trim(),
      municipio: titulo(r.nombre_municipio),
      rendimiento: +parseFloat(r.yhat).toFixed(2),
      cambio_pct: r.ultimo_real ? +(((r.yhat - r.ultimo_real) / r.ultimo_real) * 100).toFixed(1) : null,
      actual: r.id_municipio.trim() === muni,
    }));
    let filas = todas.slice(0, MAX_FILAS);
    const actual = todas.find((f) => f.actual);
    if (actual && !filas.includes(actual)) filas = [...filas.slice(0, MAX_FILAS - 1), actual];

    return Response.json({
      fromDB: true,
      departamento: rows[0] ? titulo(rows[0].nombre_departamento) : null,
      total: todas.length,
      posicion: actual ? todas.indexOf(actual) + 1 : null,
      filas,
    });
  } catch (err) {
    return errorBD("comparativo", err);
  }
}
