import pool from "@/lib/db";

/* Sección Economía: precios mayoristas (SIPSA) + precios de insumos (IPIA).
   Devuelve KPIs, precios agregados por producto y últimos precios por insumo. */

const FALLBACK = {
  fromDB: false,
  kpis: {
    productos_monitoreados: 0,
    centrales_abastos:      0,
    insumos_monitoreados:   0,
    registros_insumos:      0,
  },
  mayoristas: [],
  insumos:    [],
  tipos_insumo: [],
};

export async function GET() {
  try {
    const [kpis, mayoristas, insumos, tipos] = await Promise.all([
      pool.query(`
        SELECT
          (SELECT COUNT(DISTINCT id_cultivo)::int  FROM fact_precios_mayoristas) AS productos_monitoreados,
          (SELECT COUNT(DISTINCT id_central)::int  FROM fact_precios_mayoristas) AS centrales_abastos,
          (SELECT COUNT(DISTINCT nombre_insumo)::int FROM fact_precios_insumos)  AS insumos_monitoreados,
          (SELECT COUNT(*)::int                    FROM fact_precios_insumos)    AS registros_insumos
      `),
      pool.query(`
        SELECT c.nombre_cultivo                                    AS producto,
               ROUND(MIN(pm.precio_min_cop_kg)::numeric)           AS precio_min,
               ROUND(AVG(pm.precio_promedio_cop_kg)::numeric)      AS precio_promedio,
               ROUND(MAX(pm.precio_max_cop_kg)::numeric)           AS precio_max,
               ROUND(SUM(pm.volumen_abastecimiento_ton)::numeric)  AS volumen_ton,
               COUNT(DISTINCT pm.id_central)::int                  AS num_centrales
        FROM fact_precios_mayoristas pm
        JOIN dim_cultivo c ON c.id_cultivo = pm.id_cultivo
        GROUP BY c.nombre_cultivo
        ORDER BY volumen_ton DESC NULLS LAST
        LIMIT 30
      `),
      pool.query(`
        SELECT DISTINCT ON (fi.nombre_insumo)
               fi.nombre_insumo,
               fi.tipo_insumo,
               ROUND(fi.precio_cop_unidad::numeric) AS precio,
               fi.unidad_medida,
               r.nombre_region,
               t.anio, t.mes
        FROM fact_precios_insumos fi
        LEFT JOIN dim_tiempo t         ON t.id_tiempo = fi.id_tiempo
        LEFT JOIN dim_region_natural r ON r.id_region = fi.id_region
        ORDER BY fi.nombre_insumo, t.anio DESC NULLS LAST, t.mes DESC NULLS LAST
        LIMIT 80
      `),
      pool.query(`
        SELECT COALESCE(tipo_insumo, 'Sin clasificar') AS tipo,
               COUNT(*)::int AS total
        FROM fact_precios_insumos
        GROUP BY tipo_insumo
        ORDER BY total DESC
      `),
    ]);

    return Response.json({
      fromDB: true,
      kpis: kpis.rows[0],
      mayoristas: mayoristas.rows.map((r) => ({
        producto:        r.producto,
        precio_min:      r.precio_min      != null ? Number(r.precio_min)      : null,
        precio_promedio: r.precio_promedio != null ? Number(r.precio_promedio) : null,
        precio_max:      r.precio_max      != null ? Number(r.precio_max)      : null,
        volumen_ton:     r.volumen_ton     != null ? Number(r.volumen_ton)     : null,
        num_centrales:   r.num_centrales,
      })),
      insumos: insumos.rows.map((r) => ({
        nombre:  r.nombre_insumo,
        tipo:    r.tipo_insumo || "Sin clasificar",
        precio:  r.precio != null ? Number(r.precio) : null,
        unidad:  r.unidad_medida || "",
        region:  r.nombre_region || null,
        periodo: r.anio ? `${r.anio}-${String(r.mes).padStart(2, "0")}` : null,
      })),
      tipos_insumo: tipos.rows,
    });
  } catch (err) {
    console.error("[economia] DB error:", err.message);
    return Response.json(FALLBACK);
  }
}
