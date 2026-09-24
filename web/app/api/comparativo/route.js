import pool from "@/lib/db";

/* Compara el rendimiento predicho de un cultivo entre los municipios del
   mismo departamento. Devuelve los 6 mejores y siempre incluye el municipio
   consultado. Sin datos (o sin BD) responde filas vacías: el frontend oculta
   la tabla en vez de inventar vecinos. */

const MAX_FILAS = 6;

export async function GET(request) {
  const url     = new URL(request.url);
  const muni    = url.searchParams.get("muni") || "";
  const cultivo = url.searchParams.get("cultivo") || "";
  const [nombreMuni = "", depto = ""] = muni.split(",").map((s) => s.trim());

  if (!nombreMuni || !depto || !cultivo) {
    return Response.json({ fromDB: false, departamento: depto || null, filas: [] });
  }

  try {
    const { rows } = await pool.query(`
      SELECT m.nombre_municipio AS municipio,
             ROUND(AVG(pr.rendimiento_predicho_t_ha)::numeric, 2) AS rendimiento,
             (SELECT AVG(fp.rendimiento_t_ha)
                FROM fact_produccion_agricola fp
               WHERE fp.id_municipio = m.id_municipio
                 AND fp.id_cultivo   = c.id_cultivo) AS hist,
             (SELECT pa.nivel_riesgo
                FROM pred_alerta_climatica pa
               WHERE pa.id_municipio = m.id_municipio AND pa.activa = TRUE
               ORDER BY pa.score_probabilidad DESC
               LIMIT 1) AS riesgo
        FROM pred_rendimiento pr
        JOIN dim_municipio m ON m.id_municipio = pr.id_municipio
        JOIN dim_cultivo   c ON c.id_cultivo   = pr.id_cultivo
       WHERE m.nombre_departamento ILIKE $1
         AND c.nombre_cultivo      ILIKE $2
       GROUP BY m.id_municipio, m.nombre_municipio, c.id_cultivo
       ORDER BY rendimiento DESC
    `, [depto, cultivo]);

    const todas = rows.map((r) => {
      const rendimiento = parseFloat(r.rendimiento);
      const hist        = r.hist != null ? parseFloat(r.hist) : null;
      return {
        municipio:   r.municipio,
        rendimiento,
        riesgo:      r.riesgo || null,
        vs_hist_pct: hist ? +(((rendimiento - hist) / hist) * 100).toFixed(1) : null,
        actual:      r.municipio.toLowerCase() === nombreMuni.toLowerCase(),
      };
    });

    let filas = todas.slice(0, MAX_FILAS);
    const actual = todas.find((f) => f.actual);
    if (actual && !filas.includes(actual)) filas = [...filas.slice(0, MAX_FILAS - 1), actual];

    return Response.json({
      fromDB:       true,
      departamento: depto,
      total:        todas.length,
      posicion:     actual ? todas.indexOf(actual) + 1 : null,
      filas,
    });
  } catch (err) {
    console.error("[comparativo] DB error:", err.message);
    return Response.json({ fromDB: false, departamento: depto, filas: [] });
  }
}
