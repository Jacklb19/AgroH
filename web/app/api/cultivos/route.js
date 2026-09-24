import pool from "@/lib/db";
import { VERSION_ACTIVA, errorBD } from "@/lib/modelo";

export const dynamic = "force-dynamic";

/* Cultivos con pronóstico. Con ?muni=<código DIVIPOLA> devuelve solo los que
   tienen pronóstico en ese municipio, para no ofrecer combinaciones sin datos. */
export async function GET(request) {
  const muni = new URL(request.url).searchParams.get("muni");
  try {
    const { rows } = await pool.query(`
      SELECT c.id_cultivo, c.nombre_cultivo, c.tipo_ciclo
      FROM dim_cultivo c
      WHERE EXISTS (
        SELECT 1 FROM pred_pronostico p
        WHERE p.id_cultivo = c.id_cultivo AND p.id_version = ${VERSION_ACTIVA}
          AND ($1::text IS NULL OR p.id_municipio = $1)
      )
      ORDER BY c.nombre_cultivo
    `, [muni || null]);
    return Response.json(rows.map((r) => ({ id: r.id_cultivo, nombre: r.nombre_cultivo, ciclo: r.tipo_ciclo })));
  } catch (err) {
    return errorBD("cultivos", err);
  }
}
