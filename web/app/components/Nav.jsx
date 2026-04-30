"use client";
import { Icon } from "./icons";

const LINKS = [
  { id: "inicio",     label: "Inicio" },
  { id: "dashboards", label: "Dashboards" },
  { id: "prediccion", label: "Predicción" },
  { id: "asistente",  label: "Asistente IA" },
  { id: "impacto",    label: "Impacto" },
];

export default function Nav({ active, onNav }) {
  return (
    <header className="nav">
      <div className="container nav-inner">
        <div className="brand">
          <div className="brand-mark">A</div>
          <div className="brand-text">
            <strong>AgroIA Colombia</strong>
            <span>Inteligencia Agro-Climática</span>
          </div>
        </div>
        <nav className="nav-links">
          {LINKS.map((l) => (
            <button
              key={l.id}
              className={`nav-link ${active === l.id ? "active" : ""}`}
              onClick={() => onNav(l.id)}
            >
              {l.label}
            </button>
          ))}
        </nav>
        <button className="nav-cta" onClick={() => onNav("prediccion")}>
          Consultar Predicción <Icon.arrow className="arrow" />
        </button>
      </div>
    </header>
  );
}
