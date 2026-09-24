import pool from "@/lib/db";
import { titulo, errorBD } from "@/lib/modelo";

/* Revalida cada hora: evita que la respuesta quede congelada en el build. */
export const revalidate = 3600;

/* Precios e insumos:
   - Índice de precios de insumos agrícolas (IPIA, datos.gov.co y5zy-x4ky). Los
     valores son un ÍNDICE (no pesos): se presentan como tal.
   - Precios mayoristas SIPSA (COP/kg) del último mes disponible. */

const NOMBRES = {
  urea_46: "Urea 46 %", urea_sulfato: "Urea + sulfato", dap_18_46: "DAP 18-46", kcl_0_0_60: "Cloruro de potasio 0-0-60",
  sam: "Sulfato de amonio", _2_4_d: "2,4-D", _2_4_d_picloram: "2,4-D + picloram", aminopiralid_2_4_d: "Aminopiralid + 2,4-D",
};

const FERTILIZANTE = /^(_\d+(_\d+)+$|urea|dap|kcl|sam$)/;

function nombreInsumo(clave) {
  if (NOMBRES[clave]) return NOMBRES[clave];
  if (/^_\d+(_\d+)+$/.test(clave)) return `Fertilizante ${clave.slice(1).replace(/_/g, "-")}`;
  return clave.split("_").map((w) => w.charAt(0).toUpperCase() + w.slice(1)).join(" ").replace(/ (De|Y) /g, (m) => m.toLowerCase());
}

export async function GET() {
  try {
    const [serie, insumos, mayoristas] = await Promise.all([
      pool.query(`
        SELECT t.anio, t.mes,
               AVG(i.precio_cop_unidad) FILTER (WHERE i.nombre_insumo ~ '^(_[0-9_]+$|urea|dap|kcl|sam$)') AS fertilizantes,
               AVG(i.precio_cop_unidad) FILTER (WHERE NOT i.nombre_insumo ~ '^(_[0-9_]+$|urea|dap|kcl|sam$)') AS agroquimicos
        FROM fact_precios_insumos i JOIN dim_tiempo t ON t.id_tiempo = i.id_tiempo
        GROUP BY t.anio, t.mes ORDER BY t.anio, t.mes
      `),
      pool.query(`
        WITH s AS (
          SELECT i.nombre_insumo, t.fecha, AVG(i.precio_cop_unidad) AS v
          FROM fact_precios_insumos i JOIN dim_tiempo t ON t.id_tiempo = i.id_tiempo
          GROUP BY 1, 2
        ), ult AS (SELECT DISTINCT ON (nombre_insumo) nombre_insumo, fecha, v FROM s ORDER BY nombre_insumo, fecha DESC)
        SELECT u.nombre_insumo, u.fecha, u.v AS actual,
               (SELECT v FROM s WHERE s.nombre_insumo = u.nombre_insumo AND s.fecha <= u.fecha - INTERVAL '12 months'
                ORDER BY s.fecha DESC LIMIT 1) AS hace_un_anio,
               (SELECT MAX(v) FROM s WHERE s.nombre_insumo = u.nombre_insumo) AS maximo
        FROM ult u
      `),
      pool.query(`
        SELECT c.nombre_cultivo, t.anio, t.nombre_mes,
               AVG(p.precio_promedio_cop_kg) AS promedio,
               MIN(p.precio_promedio_cop_kg) AS minimo,
               MAX(p.precio_promedio_cop_kg) AS maximo,
               COUNT(DISTINCT p.id_central)::int AS centrales,
               STRING_AGG(DISTINCT d.ciudad, ', ') AS ciudades
        FROM fact_precios_mayoristas p
        JOIN dim_cultivo c ON c.id_cultivo = p.id_cultivo
        JOIN dim_tiempo t ON t.id_tiempo = p.id_tiempo
        LEFT JOIN dim_central_abastos d ON d.id_central = p.id_central
        WHERE p.id_tiempo = (SELECT MAX(id_tiempo) FROM fact_precios_mayoristas)
        GROUP BY c.nombre_cultivo, t.anio, t.nombre_mes
        ORDER BY promedio DESC
      `),
    ]);

    const num = (v, d = 1) => (v == null ? null : +parseFloat(v).toFixed(d));
    const m0 = mayoristas.rows[0];

    return Response.json({
      fromDB: true,
      indice: serie.rows.map((r) => ({
        periodo: `${r.anio}-${String(r.mes).padStart(2, "0")}`,
        anio: r.anio, mes: r.mes,
        fertilizantes: num(r.fertilizantes), agroquimicos: num(r.agroquimicos),
      })),
      insumos: insumos.rows
        .map((r) => ({
          clave: r.nombre_insumo,
          nombre: nombreInsumo(r.nombre_insumo),
          grupo: FERTILIZANTE.test(r.nombre_insumo) ? "Fertilizantes" : "Agroquímicos",
          actual: num(r.actual),
          cambio_12m: r.hace_un_anio ? num(((r.actual - r.hace_un_anio) / r.hace_un_anio) * 100) : null,
          desde_maximo: r.maximo ? num(((r.actual - r.maximo) / r.maximo) * 100) : null,
          fecha: r.fecha,
        }))
        .sort((a, b) => a.grupo.localeCompare(b.grupo) || a.nombre.localeCompare(b.nombre, "es")),
      mayoristas: {
        periodo: m0 ? `${m0.nombre_mes} de ${m0.anio}` : null,
        ciudades: [...new Set(mayoristas.rows.flatMap((r) => (r.ciudades || "").split(", ")).filter(Boolean))].map(titulo),
        productos: mayoristas.rows.map((r) => ({
          producto: r.nombre_cultivo,
          promedio: num(r.promedio, 0), minimo: num(r.minimo, 0), maximo: num(r.maximo, 0),
          centrales: r.centrales,
        })),
      },
    });
  } catch (err) {
    return errorBD("economia", err);
  }
}
