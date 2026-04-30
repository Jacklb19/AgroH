import pool from "@/lib/db";

export async function GET() {
  try {
    const { rows } = await pool.query(`
      SELECT DISTINCT c.nombre_cultivo
      FROM dim_cultivo c
      WHERE EXISTS (
        SELECT 1 FROM pred_rendimiento pr WHERE pr.id_cultivo = c.id_cultivo
      )
      ORDER BY c.nombre_cultivo
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
