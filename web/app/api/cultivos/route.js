import pool from "@/lib/db";

/* Revalida cada hora: evita que la respuesta quede congelada en el build. */
export const revalidate = 3600;

export async function GET() {
  try {
    const { rows } = await pool.query(`
      SELECT DISTINCT nombre_cultivo
      FROM dim_cultivo
      ORDER BY nombre_cultivo
    `);
    const list = rows.map((r) => r.nombre_cultivo);
    return Response.json(list);
  } catch (err) {
    return Response.json(
      ["Maíz tecnificado","Arroz riego","Café arábica","Caña panelera","Plátano","Papa Diacol","Aguacate Hass"],
      { status: 200 }
    );
  }
}
