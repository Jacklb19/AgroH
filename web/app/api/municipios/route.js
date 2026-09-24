import pool from "@/lib/db";
import { VERSION_ACTIVA, titulo, errorBD } from "@/lib/modelo";

/* Revalida cada hora: evita que la respuesta quede congelada en el build. */
export const revalidate = 3600;

/* Municipios con al menos un pronóstico en la versión activa del modelo. */
export async function GET() {
  try {
    const { rows } = await pool.query(`
      SELECT m.id_municipio, m.nombre_municipio, m.nombre_departamento
      FROM dim_municipio m
      WHERE EXISTS (
        SELECT 1 FROM pred_pronostico p
        WHERE p.id_municipio = m.id_municipio AND p.id_version = ${VERSION_ACTIVA}
      )
    `);
    const lista = rows
      .map((r) => ({
        id: r.id_municipio.trim(),
        nombre: titulo(r.nombre_municipio),
        departamento: titulo(r.nombre_departamento),
      }))
      .sort((a, b) => a.nombre.localeCompare(b.nombre, "es") || a.departamento.localeCompare(b.departamento, "es"));
    return Response.json(lista);
  } catch (err) {
    return errorBD("municipios", err);
  }
}
