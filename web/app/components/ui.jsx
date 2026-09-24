"use client";
import { Icon } from "./icons";

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

/* Aviso único y discreto cuando la base de datos no responde y se muestran
   cifras de ejemplo. Se usa igual en todas las páginas. */
export function DemoNotice({ show, inverse = false }) {
  if (!show) return null;
  return (
    <span className={`demo-notice ${inverse ? "inverse" : ""}`} title="La base de datos no está disponible en este momento; se muestran valores de ejemplo.">
      <Icon.info size={13} /> Datos de ejemplo
    </span>
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
