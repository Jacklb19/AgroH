import Link from "next/link";
import { REPO_URL, DOCS_URL } from "@/lib/content";

const FUENTES = [
  { label: "datos.gov.co", href: "https://www.datos.gov.co" },
  { label: "DANE",         href: "https://www.dane.gov.co" },
  { label: "IDEAM",        href: "https://www.ideam.gov.co" },
  { label: "UPRA",         href: "https://www.upra.gov.co" },
  { label: "NOAA · ENSO",  href: "https://www.cpc.ncep.noaa.gov/" },
];

export default function Footer() {
  return (
    <footer className="footer">
      <div className="container">
        <div className="footer-grid">
          <div>
            <div className="footer-brand">AgroIA Colombia</div>
            <p className="footer-tagline">
              Predicción de rendimientos y riesgo climático para el agro colombiano, construida solo con datos abiertos.
            </p>
          </div>
          <div className="footer-col">
            <h5>Plataforma</h5>
            <Link href="/prediccion">Predicción</Link>
            <Link href="/datos">Explorar datos</Link>
            <Link href="/asistente">Asistente</Link>
            <Link href="/metodologia">Cómo funciona</Link>
          </div>
          <div className="footer-col">
            <h5>Proyecto</h5>
            <a href={REPO_URL} target="_blank" rel="noopener noreferrer">Código en GitHub</a>
            <a href={DOCS_URL} target="_blank" rel="noopener noreferrer">Documentación</a>
            <a href="/api/openapi" target="_blank" rel="noopener noreferrer">API pública</a>
          </div>
          <div className="footer-col">
            <h5>Fuentes de datos</h5>
            {FUENTES.map((f) => (
              <a key={f.label} href={f.href} target="_blank" rel="noopener noreferrer">{f.label}</a>
            ))}
          </div>
        </div>
        <div className="footer-bottom">
          <span>© 2026 AgroIA Colombia · Proyecto académico de Ciencia de Datos</span>
          <span>Hecho con datos abiertos</span>
        </div>
      </div>
    </footer>
  );
}
