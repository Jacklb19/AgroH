import pool from "@/lib/db";

/* Siempre en vivo: un healthcheck prerenderizado respondería con el estado del build. */
export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const { rows } = await pool.query("SELECT NOW() AS hora, current_database() AS bd");
    return Response.json({ ok: true, ...rows[0] });
  } catch (err) {
    return Response.json({
      ok:     false,
      error:  err.message,
      code:   err.code,
      detail: err.detail,
    }, { status: 500 });
  }
}
