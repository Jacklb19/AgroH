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

/* Nivel de riesgo → texto claro para cualquier persona. */
export const RIESGO = {
  BAJO:  { cls: "bajo",  label: "Bajo",  texto: "Condiciones favorables" },
  MEDIO: { cls: "medio", label: "Medio", texto: "Hay factores a vigilar" },
  ALTO:  { cls: "alto",  label: "Alto",  texto: "Se recomiendan precauciones" },
};

export function riesgoInfo(nivel) {
  return RIESGO[String(nivel || "").toUpperCase()] || RIESGO.MEDIO;
}

export const SEMESTRE = { A: "enero – junio", B: "julio – diciembre" };
