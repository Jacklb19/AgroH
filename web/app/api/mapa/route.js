import pool from "@/lib/db";
import { VERSION_ACTIVA, RENDIMIENTO_REAL, titulo, errorBD } from "@/lib/modelo";

/* Revalida cada hora: evita que la respuesta quede congelada en el build. */
export const revalidate = 3600;

/* Pronóstico por municipio para ?anio= (por defecto el año en curso): cambio
   mediano esperado (escenario neutral) frente al último rendimiento real, sobre
   todos los cultivos del municipio. */
export async function GET(request) {
  const anio = parseInt(new URL(request.url).searchParams.get("anio"), 10) || new Date().getFullYear();
  try {
    const { rows } = await pool.query(`
      WITH ultimo AS (
        SELECT DISTINCT ON (f.id_municipio, f.id_cultivo) f.id_municipio, f.id_cultivo, ${RENDIMIENTO_REAL} AS real
        FROM fact_produccion_agricola f JOIN dim_tiempo t ON t.id_tiempo = f.id_tiempo
        WHERE ${RENDIMIENTO_REAL} IS NOT NULL
        ORDER BY f.id_municipio, f.id_cultivo, t.anio DESC
      )
      SELECT m.nombre_municipio, m.nombre_departamento,
             m.latitud_centroide AS lat, m.longitud_centroide AS lon,
             PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY p.rendimiento_predicho / u.real - 1) AS cambio,
             COUNT(*)::int AS cultivos
      FROM pred_pronostico p
      JOIN ultimo u ON u.id_municipio = p.id_municipio AND u.id_cultivo = p.id_cultivo
      JOIN dim_municipio m ON m.id_municipio = p.id_municipio
      WHERE p.id_version = ${VERSION_ACTIVA} AND p.tipo = 'pronostico' AND p.escenario = 'Neutral'
        AND p.anio = $1 AND m.latitud_centroide IS NOT NULL
      GROUP BY m.id_municipio, m.nombre_municipio, m.nombre_departamento, m.latitud_centroide, m.longitud_centroide
    `, [anio]);

    const puntos = rows.map((r) => {
      const cambio = +(parseFloat(r.cambio) * 100).toFixed(1);
      return {
        municipio: titulo(r.nombre_municipio),
        departamento: titulo(r.nombre_departamento),
        lat: parseFloat(r.lat), lon: parseFloat(r.lon),
        cambio, cultivos: r.cultivos,
        tendencia: cambio > 3 ? "sube" : cambio < -3 ? "baja" : "estable",
      };
    });
    const cuenta = (t) => puntos.filter((p) => p.tendencia === t).length;
    return Response.json({
      fromDB: true, anio, puntos,
      resumen: { sube: cuenta("sube"), estable: cuenta("estable"), baja: cuenta("baja"), total: puntos.length },
    });
  } catch (err) {
    return errorBD("mapa", err);
  }
}
