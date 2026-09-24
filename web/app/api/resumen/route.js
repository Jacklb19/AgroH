import pool from "@/lib/db";
import { VERSION_ACTIVA, errorBD } from "@/lib/modelo";

/* Revalida cada hora: evita que la respuesta quede congelada en el build. */
export const revalidate = 3600;

/* Cifras de cobertura para el Inicio, calculadas sobre la BD. */
export async function GET() {
  try {
    const { rows } = await pool.query(`
      SELECT
        (SELECT COUNT(DISTINCT id_municipio)::int FROM pred_pronostico WHERE id_version = ${VERSION_ACTIVA}) AS municipios,
        (SELECT COUNT(DISTINCT id_cultivo)::int   FROM pred_pronostico WHERE id_version = ${VERSION_ACTIVA}) AS cultivos,
        (SELECT COUNT(DISTINCT (id_municipio, id_cultivo))::int FROM pred_pronostico
          WHERE id_version = ${VERSION_ACTIVA} AND tipo = 'pronostico') AS combinaciones,
        (SELECT MIN(t.anio) FROM fact_produccion_agricola f JOIN dim_tiempo t ON t.id_tiempo = f.id_tiempo) AS anio_desde,
        (SELECT MAX(t.anio) FROM fact_produccion_agricola f JOIN dim_tiempo t ON t.id_tiempo = f.id_tiempo) AS anio_hasta,
        (SELECT COUNT(*)::int FROM fact_produccion_agricola) AS registros
    `);
    return Response.json({ fromDB: true, ...rows[0] });
  } catch (err) {
    return errorBD("resumen", err);
  }
}
