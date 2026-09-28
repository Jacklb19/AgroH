"use client";
import { useEffect, useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { Icon } from "./icons";

export const LINKS = [
  { href: "/",            label: "Inicio" },
  { href: "/prediccion",  label: "Predicción" },
  { href: "/datos",       label: "Explorar datos" },
  { href: "/asistente",   label: "Asistente" },
  { href: "/metodologia", label: "Cómo funciona" },
];

export default function Nav() {
  const pathname = usePathname();
  const [open, setOpen] = useState(false);

  useEffect(() => { setOpen(false); }, [pathname]);

  const isActive = (href) => (href === "/" ? pathname === "/" : pathname.startsWith(href));

  return (
    <header className="nav">
      <div className="container nav-inner">
        <Link href="/" className="brand" aria-label="AgroIA Colombia, inicio">
          <span className="brand-mark"><Icon.sprout size={20} /></span>
          <span className="brand-text">
            <strong>AgroIA Colombia</strong>
            <span>Inteligencia agroclimática</span>
          </span>
        </Link>

        <nav className="nav-links" aria-label="Principal">
          {LINKS.map((l) => (
            <Link
              key={l.href}
              href={l.href}
              className={`nav-link ${isActive(l.href) ? "active" : ""}`}
              aria-current={isActive(l.href) ? "page" : undefined}
            >
              {l.label}
            </Link>
          ))}
        </nav>

        <Link href="/prediccion" className="nav-cta">
          Probar predicción <Icon.arrow className="arrow" />
        </Link>

        <button
          className={`nav-hamburger ${open ? "open" : ""}`}
          onClick={() => setOpen(!open)}
          aria-label={open ? "Cerrar menú" : "Abrir menú"}
          aria-expanded={open}
        >
          <span /><span /><span />
        </button>
      </div>

      {open && (
        <nav className="nav-mobile" aria-label="Principal móvil">
          {LINKS.map((l) => (
            <Link key={l.href} href={l.href} className={`nav-mobile-link ${isActive(l.href) ? "active" : ""}`}>
              {l.label}
            </Link>
          ))}
          <Link href="/prediccion" className="nav-mobile-cta">Probar predicción</Link>
        </nav>
      )}
    </header>
  );
}
