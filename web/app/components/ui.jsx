"use client";

/* Encabezado de sección estándar: etiqueta + título + bajada. */
export function SectionHead({ eyebrow, tone, title, children, center = false }) {
  return (
    <div className={`section-head ${center ? "center" : ""}`}>
      {eyebrow && <span className={`eyebrow ${tone || ""}`}>{eyebrow}</span>}
      <h2>{title}</h2>
      {children && <p>{children}</p>}
    </div>
  );
}

/* Línea de fuente legible, en lugar de nombres de tablas. */
export function Source({ children }) {
  return <div className="source-line">Fuente: {children}</div>;
}

export function RiskBadge({ nivel, label }) {
  const k = String(nivel || "").toLowerCase();
  return <span className={`badge-risk ${k}`}>{label || `Riesgo ${k}`}</span>;
}
