import pool from "@/lib/db";
import { errorBD } from "@/lib/modelo";

/* Recomendaciones basadas en datos reales de la BD (aptitud UPRA, fase ENSO de
   NOAA, variabilidad histórica) más un calendario típico de siembra para los
   cultivos transitorios más comunes. Body: { id_municipio, id_cultivo, semestre }. */

const CALENDARIO = {
  "Arroz":    { A: ["marzo", "abril"],     B: ["agosto", "septiembre"] },
  "Maíz":     { A: ["marzo", "abril"],     B: ["septiembre", "octubre"] },
  "Frijol":   { A: ["marzo", "abril"],     B: ["septiembre", "octubre"] },
  "Papa":     { A: ["marzo", "abril"],     B: ["septiembre", "octubre"] },
  "Arveja":   { A: ["marzo", "abril"],     B: ["septiembre", "octubre"] },
  "Sorgo":    { A: ["abril", "mayo"],      B: ["agosto", "septiembre"] },
  "Soya":     { A: ["abril", "mayo"],      B: ["agosto", "septiembre"] },
  "Algodón":  { A: ["febrero", "marzo"],   B: ["julio", "agosto"] },
  "Tomate":   { A: ["febrero", "marzo"],   B: ["agosto", "septiembre"] },
  "Yuca":     { A: ["marzo", "abril"],     B: ["septiembre", "octubre"] },
};

const APTITUD_TXT = {
  alta:     "El suelo tiene aptitud alta para este cultivo según la UPRA.",
  moderada: "El suelo tiene aptitud moderada: es viable con buen manejo de suelo y agua.",
  marginal: "El suelo tiene aptitud marginal: espera rendimientos menores o mayores costos de manejo.",
  no_apta:  "La UPRA clasifica el suelo como no apto para este cultivo en este municipio.",
};

const ENSO_TXT = {
  "El Niño": "Fase El Niño: suele traer menos lluvia en las regiones Andina y Caribe. Planea riego de apoyo y conserva la humedad del suelo.",
  "La Niña": "Fase La Niña: suele traer más lluvia. Revisa el drenaje y vigila enfermedades por exceso de humedad.",
  "Neutro":  "Fase neutral: sin señal fuerte de El Niño o La Niña. Sigue el pronóstico mensual del IDEAM.",
};

export async function POST(request) {
  const { id_municipio, id_cultivo, semestre = "A" } = await request.json();
  const muni = String(id_municipio || "").trim();
  const cultivo = parseInt(id_cultivo, 10);

  try {
    const [cult, apt, enso] = await Promise.all([
      pool.query("SELECT nombre_cultivo, tipo_ciclo FROM dim_cultivo WHERE id_cultivo = $1", [cultivo]),
      pool.query("SELECT clase_aptitud FROM fact_aptitud_suelo WHERE id_municipio = $1 AND id_cultivo = $2 LIMIT 1", [muni, cultivo]),
      pool.query(`
        SELECT t.anio, t.nombre_mes, e.fase_enso
        FROM fact_alerta_enso e JOIN dim_tiempo t ON t.id_tiempo = e.id_tiempo
        ORDER BY t.anio DESC, t.mes DESC LIMIT 1
      `),
    ]);
    const nombre = cult.rows[0]?.nombre_cultivo || "";
    const permanente = cult.rows[0]?.tipo_ciclo === "permanente";
    const aptitud = apt.rows[0]?.clase_aptitud || null;
    const fase = enso.rows[0] || null;
    const cal = CALENDARIO[Object.keys(CALENDARIO).find((k) => nombre.startsWith(k))];

    const recomendaciones = [
      {
        tipo: "calendario",
        titulo: permanente ? "Cultivo permanente" : "Ventana típica de siembra",
        detalle: permanente
          ? `${nombre} es un cultivo permanente: no se siembra cada semestre. Las decisiones clave son renovación, fertilización y cosecha.`
          : cal
            ? `En el semestre ${semestre}, la siembra de ${nombre.toLowerCase()} suele hacerse en ${cal[semestre].join(" y ")}.`
            : `No tenemos un calendario específico para ${nombre.toLowerCase()}. Consulta con un asistente técnico local.`,
        fuente: "Calendario agrícola típico (referencia general)",
      },
      {
        tipo: "suelo",
        alerta: aptitud === "no_apta" || aptitud === "marginal",
        titulo: "Aptitud del suelo",
        detalle: aptitud ? APTITUD_TXT[aptitud] : "La UPRA no tiene clasificación de aptitud para este cultivo en este municipio.",
        fuente: "UPRA · SIPRA",
      },
      {
        tipo: "clima",
        alerta: fase?.fase_enso && fase.fase_enso !== "Neutro",
        titulo: "El Niño / La Niña hoy",
        detalle: fase ? ENSO_TXT[fase.fase_enso] || `Fase actual: ${fase.fase_enso}.` : "Sin dato reciente de la fase ENSO.",
        fuente: fase ? `NOAA · último dato: ${fase.nombre_mes?.toLowerCase()} de ${fase.anio}` : "NOAA",
      },
    ];

    return Response.json({ fromDB: true, cultivo: nombre, aptitud, enso: fase?.fase_enso || null, recomendaciones });
  } catch (err) {
    return errorBD("recomendacion", err);
  }
}
