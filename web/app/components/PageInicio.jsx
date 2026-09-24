"use client";
import { useEffect, useState } from "react";
import Link from "next/link";
import ColombiaMap from "./charts/ColombiaMap";
import { Icon } from "./icons";
import { SectionHead, DemoNotice } from "./ui";
import { fmtCompact } from "@/lib/format";
import { PROBLEMA, ANOVA_TESTS, PARA_QUIEN, FUENTES_RESUMEN } from "@/lib/content";

const USOS = [
  {
    href: "/prediccion", icon: "target", tono: "green",
    titulo: "Predice un cultivo",
    texto: "Elige municipio, cultivo y semestre. Obtén el rendimiento esperado, su rango probable y el nivel de riesgo.",
    cta: "Probar predicción",
  },
  {
    href: "/prediccion", icon: "sliders", tono: "blue",
    titulo: "Simula El Niño o La Niña",
    texto: "Mueve lluvia, temperatura y fertilización para ver cómo cambiaría la cosecha en cada escenario.",
    cta: "Abrir simulador",
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
    texto: "Rendimiento real frente al predicho, alertas activas y tableros interactivos por región y cultivo.",
    cta: "Explorar datos",
  },
];

function RiesgoHoy({ puntos, semaforo, demo }) {
  const total = semaforo ? semaforo.bajo + semaforo.medio + semaforo.alto : 0;
  const pct = (v) => (total ? Math.round((v / total) * 100) : 0);
  const filas = semaforo
    ? [
        { k: "alto",  label: "Alto",  v: semaforo.alto },
        { k: "medio", label: "Medio", v: semaforo.medio },
        { k: "bajo",  label: "Bajo",  v: semaforo.bajo },
      ]
    : [];

  return (
    <div className="hero-panel">
      <div className="hero-panel-head">
        <div>
          <div className="hero-panel-title">Riesgo climático actual</div>
          <div className="hero-panel-sub">Alertas activas por municipio</div>
        </div>
        <DemoNotice show={demo} />
      </div>
      <div className="map-frame">
        <ColombiaMap puntos={puntos} height={250} />
      </div>
      {total > 0 && (
        <>
          <div className="stack-bar" role="img" aria-label={`Alto ${pct(semaforo.alto)}%, medio ${pct(semaforo.medio)}%, bajo ${pct(semaforo.bajo)}%`}>
            {filas.map((f) => <span key={f.k} className={f.k} style={{ width: `${pct(f.v)}%` }} />)}
          </div>
          <div className="stack-legend">
            {filas.map((f) => (
              <span key={f.k}><i className={f.k} /> {f.label} <strong>{pct(f.v)}%</strong></span>
            ))}
          </div>
        </>
      )}
    </div>
  );
}

export default function PageInicio() {
  const [stats,    setStats]    = useState(null);
  const [mapa,     setMapa]     = useState({ puntos: [], fromDB: true });
  const [semaforo, setSemaforo] = useState(null);
  const [dashDemo, setDashDemo] = useState(false);

  useEffect(() => {
    fetch("/api/impacto").then((r) => r.json()).then(setStats).catch(() => {});
    fetch("/api/mapa").then((r) => r.json()).then(setMapa).catch(() => {});
    fetch("/api/dashboards").then((r) => r.json())
      .then((d) => { setSemaforo(d.semaforo); setDashDemo(!d.fromDB); })
      .catch(() => {});
  }, []);

  const demo = stats?.fromDB === false;

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
              AgroIA cruza clima, suelos, precios e historia de producción para anticipar el rendimiento
              de un cultivo y su riesgo climático en cualquier municipio del país.
            </p>
            <div className="hero-actions">
              <Link href="/prediccion" className="btn-primary">
                Probar predicción <Icon.arrow className="arrow" />
              </Link>
              <Link href="/datos" className="btn-outline-white">Explorar datos</Link>
            </div>
            <div className="hero-stats">
              <div className="hero-stat"><div className="num">{fmtCompact(stats?.municipios_cubiertos)}</div><div className="lbl">Municipios</div></div>
              <div className="hero-stat"><div className="num">{fmtCompact(stats?.cultivos_monitoreados)}</div><div className="lbl">Cultivos</div></div>
              <div className="hero-stat"><div className="num">{fmtCompact(stats?.hectareas_cobertura)}</div><div className="lbl">Hectáreas en registros</div></div>
              <div className="hero-stat"><div className="num">2007–25</div><div className="lbl">Historia de producción</div></div>
            </div>
            {demo && <div style={{ marginTop: 14 }}><DemoNotice show inverse /></div>}
          </div>
          <RiesgoHoy puntos={mapa.puntos} semaforo={semaforo} demo={!mapa.fromDB || dashDemo} />
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
            <span><strong>AgroIA reúne esas fuentes en un solo lugar</strong> y las convierte en una predicción por municipio y cultivo, explicada en lenguaje simple.</span>
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
              <p>Producción agrícola, clima de 991 estaciones, precios, aptitud de suelos y el índice El Niño / La Niña, unificados por municipio.</p>
            </div>
            <div className="step">
              <div className="step-head"><div className="step-num">2</div><span className="step-pill">Modelo</span></div>
              <h3>Un modelo aprende del pasado</h3>
              <p>Un modelo de aprendizaje automático (XGBoost) estudia 18 años de cosechas y clima para estimar el rendimiento de la próxima temporada.</p>
            </div>
            <div className="step">
              <div className="step-head"><div className="step-num">3</div><span className="step-pill">Decisión</span></div>
              <h3>Tú decides con más información</h3>
              <p>Recibes el rendimiento esperado, el riesgo climático, los factores que más pesan y recomendaciones de siembra.</p>
            </div>
          </div>
          <div className="sources-strip">
            <span className="sources-label">Fuentes</span>
            {FUENTES_RESUMEN.map((f) => <span key={f} className="source-chip">{f}</span>)}
            <Link href="/metodologia#fuentes" className="link-arrow small">Ver las 14 fuentes <Icon.arrow className="arrow" /></Link>
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
              <h2>Prueba una predicción en menos de un minuto</h2>
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
