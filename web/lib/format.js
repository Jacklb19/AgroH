/* Utilidades de formato compartidas por las páginas (es-CO). */

export function fmtCompact(n) {
  if (n == null || Number.isNaN(Number(n))) return "—";
  const v = Number(n);
  if (v >= 1_000_000) return (v / 1_000_000).toFixed(1).replace(/\.0$/, "").replace(".", ",") + " M";
  if (v >= 10_000)    return Math.round(v / 1_000).toLocaleString("es-CO") + " mil";
  return v.toLocaleString("es-CO");
}

export function fmtNum(n, dec = 1) {
  if (n == null || Number.isNaN(Number(n))) return "—";
  return Number(n).toLocaleString("es-CO", { minimumFractionDigits: dec, maximumFractionDigits: dec });
}

export function signed(n, dec = 1) {
  if (n == null) return "—";
  return `${n >= 0 ? "+" : "−"}${fmtNum(Math.abs(n), dec)}`;
}

export const SEMESTRE = { A: "enero – junio", B: "julio – diciembre" };

/* Temporadas de siembra que todavía están abiertas, según la fecha de hoy.
   Ventana típica: semestre A hasta abril, semestre B hasta octubre. Solo se
   ofrecen años para los que el modelo tiene pronóstico. */
const CIERRE_VENTANA = { A: 4, B: 10 };

export function temporadasDisponibles(aniosConPronostico, hoy = new Date()) {
  const anioHoy = hoy.getFullYear();
  const mesHoy = hoy.getMonth() + 1;
  const out = [];
  for (const anio of [...aniosConPronostico].sort((a, b) => a - b)) {
    if (anio < anioHoy) continue;
    for (const sem of ["A", "B"]) {
      if (anio === anioHoy && mesHoy > CIERRE_VENTANA[sem]) continue;
      const enCurso = anio === anioHoy && (sem === "A" ? mesHoy <= 6 : mesHoy >= 7);
      out.push({
        id: `${anio}-${sem}`, anio, semestre: sem, enCurso,
        label: `${sem === "A" ? "1.er" : "2.º"} semestre ${anio} (${sem === "A" ? "ene–jun" : "jul–dic"})${enCurso ? " · en curso" : ""}`,
      });
    }
  }
  return out;
}
