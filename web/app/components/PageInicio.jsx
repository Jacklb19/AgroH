"use client";
import { useEffect, useState } from "react";
import Link from "next/link";
import ColombiaMap from "./charts/ColombiaMap";
import { Icon } from "./icons";
import { SectionHead } from "./ui";
import { fmtCompact } from "@/lib/format";
import { PROBLEMA, ANOVA_TESTS, PARA_QUIEN, FUENTES_RESUMEN } from "@/lib/content";

const USOS = [
  {
    href: "/prediccion", icon: "target", tono: "green",
    titulo: "Pronostica un cultivo",
    texto: "Elige municipio, cultivo y temporada. Obtén el rendimiento esperado, su rango probable y qué tan estable ha sido.",
    cta: "Probar predicción",
  },
  {
    href: "/prediccion", icon: "sliders", tono: "blue",
    titulo: "Compara escenarios",
    texto: "Mira qué espera el modelo con El Niño, La Niña o un año normal, y simula cambios de lluvia y temperatura.",
    cta: "Abrir predicción",
  },
  {
    href: "/asistente", icon: "message", tono: "amber",
    titulo: "Pregunta en lenguaje natural",
    texto: "“¿Qué sembrar en Manizales?” o “compara Espinal y Saldaña”. El asistente responde con los datos.",
    cta: "Hablar con el asistente",
  },
  {
    href: "/datos", icon: "barChart", tono: "violet",
    titulo: "Explora los datos",
    texto: "Qué tan bien predice el modelo, qué se espera por cultivo, precios de insumos y tableros interactivos.",
    cta: "Explorar datos",
  },
];

function PronosticoMapa({ mapa }) {
  const r = mapa?.resumen;
  const pct = (v) => (r?.total ? Math.round((v / r.total) * 100) : 0);
  const filas = r
    ? [
        { k: "sube",    label: "Sube",    v: r.sube },
        { k: "estable", label: "Estable", v: r.estable },
        { k: "baja",    label: "Baja",    v: r.baja },
      ]
    : [];
  return (
    <div className="hero-panel">
      <div className="hero-panel-head">
        <div>
          <div className="hero-panel-title">Pronóstico de cosecha {mapa?.anio || ""}</div>
          <div className="hero-panel-sub">Cambio esperado frente al último año registrado, por municipio</div>
        </div>
      </div>
      <div className="map-frame">
        {mapa?.puntos
          ? <ColombiaMap puntos={mapa.puntos} height={260} />
          : <div className="map-loading">{mapa?.error ? "Mapa no disponible en este momento" : "Cargando mapa…"}</div>}
      </div>
      {r && (
        <>
          <div className="stack-legend">
            {filas.map((f) => (
              <span key={f.k}><i className={f.k} /> {f.label} <strong>{f.v.toLocaleString("es-CO")}</strong></span>
            ))}
          </div>
          <p className="hero-panel-note">
            {pct(r.estable) >= 60
              ? `En el ${pct(r.estable)} % de los municipios se espera un rendimiento similar al último registrado (±3 %).`
              : `${pct(r.sube)} % de los municipios suben y ${pct(r.baja)} % bajan frente al último año registrado.`}
          </p>
        </>
      )}
    </div>
  );
}

export default function PageInicio() {
  const [stats, setStats] = useState(null);
  const [mapa, setMapa] = useState(null);

  useEffect(() => {
    fetch("/api/resumen").then((r) => r.json()).then(setStats).catch(() => {});
    fetch(`/api/mapa?anio=${new Date().getFullYear()}`).then((r) => r.json()).then(setMapa).catch(() => setMapa({ error: true }));
  }, []);

  return (
    <>
      {/* ── Hero ─────────────────────────────────────────────── */}
      <section className="hero">
        <div className="container hero-grid">
          <div className="hero-copy">
            <div className="badge-glass"><span className="dot" /> Datos abiertos de Colombia</div>
            <h1>
              Sabe qué esperar de tu cosecha <em>antes de sembrar</em>
            </h1>
            <p className="lede">
              AgroIA cruza la historia de producción oficial, el suelo, el clima y el fenómeno de El Niño para pronosticar
              el rendimiento de cada cultivo en cada municipio del país, con un rango honesto de incertidumbre.
            </p>
            <div className="hero-actions">
              <Link href="/prediccion" className="btn-primary">
                Probar predicción <Icon.arrow className="arrow" />
              </Link>
              <Link href="/datos" className="btn-outline-white">Explorar datos</Link>
            </div>
            <div className="hero-stats">
              <div className="hero-stat"><div className="num">{stats?.municipios ? fmtCompact(stats.municipios) : "—"}</div><div className="lbl">Municipios con pronóstico</div></div>
              <div className="hero-stat"><div className="num">{stats?.cultivos ? fmtCompact(stats.cultivos) : "—"}</div><div className="lbl">Cultivos</div></div>
              <div className="hero-stat"><div className="num">{stats?.combinaciones ? fmtCompact(stats.combinaciones) : "—"}</div><div className="lbl">Pronósticos municipio × cultivo</div></div>
              <div className="hero-stat"><div className="num">{stats?.anio_desde ? `${stats.anio_desde}–${String(stats.anio_hasta).slice(2)}` : "—"}</div><div className="lbl">Cosechas registradas</div></div>
            </div>
          </div>
          <PronosticoMapa mapa={mapa} />
        </div>
      </section>

      {/* ── El problema ──────────────────────────────────────── */}
      <section className="section">
        <div className="container">
          <SectionHead eyebrow="El problema" tone="amber" title="El agro colombiano enfrenta el clima con poca información">
            Colombia tiene más de 1.100 municipios con actividad agropecuaria y cinco regiones con climas muy distintos.
            Tres brechas impiden anticiparse a una mala temporada.
          </SectionHead>
          <div className="grid-3">
            {PROBLEMA.map((p) => {
              const I = Icon[p.icon];
              return (
                <article key={p.titulo} className={`problem-card ${p.tono}`}>
                  <div className="problem-top">
                    <span className="icon-chip"><I size={18} /></span>
                    <span className="problem-figure">{p.cifra}</span>
                  </div>
                  <h3>{p.titulo}</h3>
                  <p>{p.texto}</p>
                </article>
              );
            })}
          </div>
          <p className="solution-line">
            <Icon.checkCircle size={18} />
            <span><strong>AgroIA reúne esas fuentes en un solo lugar</strong> y las convierte en un pronóstico por municipio y cultivo, explicado en lenguaje simple.</span>
          </p>
        </div>
      </section>

      {/* ── Qué puedes hacer ─────────────────────────────────── */}
      <section className="section section-gray">
        <div className="container">
          <SectionHead eyebrow="Qué puedes hacer" title="Cuatro formas de usar AgroIA" />
          <div className="grid-4">
            {USOS.map((u) => {
              const I = Icon[u.icon];
              return (
                <Link key={u.titulo} href={u.href} className={`use-card ${u.tono}`}>
                  <span className="icon-chip"><I size={20} /></span>
                  <h3>{u.titulo}</h3>
                  <p>{u.texto}</p>
                  <span className="use-cta">{u.cta} <Icon.arrow className="arrow" /></span>
                </Link>
              );
            })}
          </div>
        </div>
      </section>

      {/* ── Hallazgos ────────────────────────────────────────── */}
      <section className="section">
        <div className="container">
          <div className="head-row">
            <SectionHead eyebrow="Lo que dicen los datos" tone="blue" title="Hallazgos confirmados con evidencia estadística">
              Antes de predecir, comprobamos qué factores realmente importan. Las cuatro pruebas dieron diferencias
              significativas: la probabilidad de que sean casualidad es menor al 0,1 %.
            </SectionHead>
            <Link href="/metodologia#evidencia" className="link-arrow">Ver la evidencia completa <Icon.arrow className="arrow" /></Link>
          </div>
          <div className="grid-4">
            {ANOVA_TESTS.map((t) => (
              <article key={t.id} className={`finding-card ${t.tono}`}>
                <span className="finding-num">0{t.id}</span>
                <h3>{t.titulo}</h3>
                <p>{t.hallazgo}</p>
                <div className="finding-foot">
                  <span className="sig-pill"><Icon.check size={12} /> p &lt; 0,001</span>
                  <span className="finding-src">{t.fuente}</span>
                </div>
              </article>
            ))}
          </div>
        </div>
      </section>

      {/* ── Cómo funciona ────────────────────────────────────── */}
      <section className="section section-gray">
        <div className="container">
          <SectionHead eyebrow="Cómo funciona" title="De datos públicos a una decisión en tres pasos" />
          <div className="steps">
            <div className="step">
              <div className="step-head"><div className="step-num">1</div><span className="step-pill">Datos</span></div>
              <h3>Reunimos datos abiertos</h3>
              <p>Producción agrícola oficial, clima de estaciones del IDEAM, precios, aptitud de suelos y el índice El Niño / La Niña, unificados por municipio.</p>
            </div>
            <div className="step">
              <div className="step-head"><div className="step-num">2</div><span className="step-pill">Modelo</span></div>
              <h3>Un modelo aprende del pasado</h3>
              <p>Un modelo de aprendizaje automático (XGBoost) aprende de las cosechas de 2019 a 2024 y se prueba con años que no vio antes de usarse.</p>
            </div>
            <div className="step">
              <div className="step-head"><div className="step-num">3</div><span className="step-pill">Decisión</span></div>
              <h3>Tú decides con más información</h3>
              <p>Recibes el rendimiento esperado, su rango probable, qué tan estable ha sido el cultivo y recomendaciones de siembra.</p>
            </div>
          </div>
          <div className="sources-strip">
            <span className="sources-label">Fuentes</span>
            {FUENTES_RESUMEN.map((f) => <span key={f} className="source-chip">{f}</span>)}
            <Link href="/metodologia#fuentes" className="link-arrow small">Ver todas las fuentes <Icon.arrow className="arrow" /></Link>
          </div>
        </div>
      </section>

      {/* ── Para quién ───────────────────────────────────────── */}
      <section className="section">
        <div className="container">
          <SectionHead eyebrow="Para quién" title="Pensado para quienes deciden sobre el campo">
            Una misma fuente de información para quien siembra, quien financia y quien planifica.
          </SectionHead>
          <div className="grid-4">
            {PARA_QUIEN.map((p) => {
              const I = Icon[p.icon];
              return (
                <article key={p.titulo} className="audience-card">
                  <span className="icon-chip"><I size={20} /></span>
                  <h3>{p.titulo}</h3>
                  <p>{p.texto}</p>
                </article>
              );
            })}
          </div>
        </div>
      </section>

      {/* ── CTA final ────────────────────────────────────────── */}
      <section className="section-tight">
        <div className="container">
          <div className="cta-band">
            <div>
              <h2>Prueba un pronóstico en menos de un minuto</h2>
              <p>Elige un municipio y un cultivo. Sin registro.</p>
            </div>
            <Link href="/prediccion" className="btn-primary">
              Probar predicción <Icon.arrow className="arrow" />
            </Link>
          </div>
        </div>
      </section>
    </>
  );
}
