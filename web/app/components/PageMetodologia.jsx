"use client";
import { useEffect, useState } from "react";
import { Icon } from "./icons";
import { SectionHead } from "./ui";
import { fmtCompact, fmtNum } from "@/lib/format";
import { ANOVA_TESTS, METRICAS_MODELO, LIMITACIONES, TRABAJO_FUTURO, REPO_URL, DOCS_URL } from "@/lib/content";
import anova from "@/public/anova_data.json";

const PROCESO = [
  {
    icon: "database", titulo: "Reunimos", tono: "blue",
    texto: "14 fuentes abiertas: producción agrícola (DANE), clima de 991 estaciones (IDEAM), satélite (NASA), suelos (UPRA), precios (SIPSA) y el índice El Niño / La Niña (NOAA).",
  },
  {
    icon: "filter", titulo: "Limpiamos y unimos", tono: "green",
    texto: "Cada registro se asocia a su municipio con el código oficial DIVIPOLA, se descartan valores imposibles y duplicados, y todo queda en una sola base de datos.",
  },
  {
    icon: "cpu", titulo: "Entrenamos el modelo", tono: "amber",
    texto: "XGBoost aprende de ~35 variables por municipio, cultivo y año. Se probaron 200 configuraciones y se eligió la de menor error.",
  },
  {
    icon: "checkCircle", titulo: "Validamos", tono: "violet",
    texto: "Se evalúa con los años más recientes, que el modelo nunca vio al entrenar: así se simula predecir el futuro de verdad.",
  },
];

const VARIABLES = [
  { icon: "cloudRain", t: "Clima",       d: "Lluvia acumulada, temperatura, humedad y horas de sol, por semestre." },
  { icon: "sun",       t: "El Niño / La Niña", d: "Fase del fenómeno, anomalía de lluvia y probabilidad de sequía o exceso." },
  { icon: "layers",    t: "Suelo",       d: "Aptitud del suelo para cada cultivo según la UPRA." },
  { icon: "trend",     t: "Mercado",     d: "Precio mayorista del producto y precio de los insumos." },
  { icon: "calendar",  t: "Historia",    d: "Rendimiento de años anteriores y clima de 1 y 3 años atrás." },
  { icon: "map",       t: "Territorio",  d: "Municipio, departamento y región natural." },
];

function AnovaCard({ test, row }) {
  const [open, setOpen] = useState(false);
  const p = row ? parseFloat(row["p-valor"]) : null;
  return (
    <article className={`anova-card ${test.tono}`}>
      <header>
        <span className="anova-num">Prueba {test.id}</span>
        <h3>{test.titulo}</h3>
        <p className="anova-q">{test.pregunta}</p>
      </header>
      <div className="anova-img">
        <img src={`/images/${test.imagen}`} alt={`Diagrama de cajas: ${test.titulo}`} loading="lazy" />
      </div>
      <div className="anova-body">
        {row && <span className="sig-pill"><Icon.check size={12} /> Diferencia real confirmada</span>}
        <p>{test.explicacion}</p>
        <div className="anova-src">Datos: {test.fuente}</div>
        {row && (
          <>
            <button className="btn-link" onClick={() => setOpen(!open)} aria-expanded={open}>
              <Icon.chevron style={{ transform: open ? "rotate(180deg)" : "none" }} /> {open ? "Ocultar" : "Ver"} estadísticas
            </button>
            {open && (
              <dl className="stats-grid">
                <dt>Estadístico F</dt><dd>{fmtNum(parseFloat(row["F"]), 2)}</dd>
                <dt>Valor p</dt><dd>{p < 0.0001 ? "< 0,0001" : row["p-valor"]}</dd>
                <dt>Grupos comparados</dt><dd>{row["Grupos"]}</dd>
                <dt>Registros</dt><dd>{parseInt(row["N total"], 10).toLocaleString("es-CO")}</dd>
              </dl>
            )}
          </>
        )}
      </div>
    </article>
  );
}

export default function PageMetodologia() {
  const [fuentes, setFuentes] = useState([]);
  const [calidad, setCalidad] = useState([]);

  useEffect(() => {
    fetch("/api/catalogo").then((r) => r.json()).then(setFuentes).catch(() => setFuentes([]));
    fetch("/api/calidad").then((r) => r.json()).then((d) => setCalidad(d.reportes || [])).catch(() => setCalidad([]));
  }, []);

  const pruebas = anova.pruebas || [];
  const hayConteos = fuentes.some((f) => f.filas != null);

  return (
    <>
      {/* ── Proceso ──────────────────────────────────────────── */}
      <section className="section page-top">
        <div className="container">
          <SectionHead eyebrow="Cómo funciona" tone="blue" title="Del dato público a la predicción, paso a paso">
            Todo el proceso es abierto y reproducible: las fuentes, el código y las métricas se pueden auditar.
          </SectionHead>
          <ol className="process">
            {PROCESO.map((p, i) => {
              const I = Icon[p.icon];
              return (
                <li key={p.titulo} className={`process-step ${p.tono}`}>
                  <div className="process-top">
                    <span className="icon-chip"><I size={18} /></span>
                    <span className="process-n">0{i + 1}</span>
                  </div>
                  <h3>{p.titulo}</h3>
                  <p>{p.texto}</p>
                </li>
              );
            })}
          </ol>
        </div>
      </section>

      {/* ── Evidencia ────────────────────────────────────────── */}
      <section id="evidencia" className="section section-gray">
        <div className="container">
          <SectionHead eyebrow="Evidencia estadística" title="Lo que comprobamos antes de predecir">
            Usamos pruebas ANOVA, que indican si las diferencias entre grupos son reales o fruto del azar. En las cuatro,
            la probabilidad de que la diferencia sea casualidad es menor al 0,1 %.
          </SectionHead>
          <div className="anova-grid">
            {ANOVA_TESTS.map((t) => (
              <AnovaCard key={t.id} test={t} row={pruebas.find((r) => r["Prueba"]?.includes(t.clave))} />
            ))}
          </div>
          <p className="fine-print">
            Protocolo: prueba de Levene para comparar varianzas, ANOVA de una vía y prueba post-hoc de Tukey para identificar
            qué grupos difieren. Script reproducible en <code>validate/anova_tests.py</code>.
          </p>
        </div>
      </section>

      {/* ── El modelo ────────────────────────────────────────── */}
      <section className="section">
        <div className="container">
          <SectionHead eyebrow="El modelo" tone="amber" title="Qué predice y qué tan bien lo hace">
            El modelo estima el rendimiento, en toneladas por hectárea, de un cultivo en un municipio y un año concretos.
            Cada predicción viene con un rango probable y con los factores que más la explican.
          </SectionHead>

          <div className="model-grid">
            <div>
              <h3 className="sub-title">Qué información usa</h3>
              <div className="var-grid">
                {VARIABLES.map((v) => {
                  const I = Icon[v.icon];
                  return (
                    <div key={v.t} className="var-item">
                      <span className="icon-chip sm"><I size={15} /></span>
                      <div><strong>{v.t}</strong><p>{v.d}</p></div>
                    </div>
                  );
                })}
              </div>
              <p className="callout">
                <Icon.info size={16} />
                <span>Los factores que más pesan son la <strong>lluvia acumulada del año</strong>, la <strong>aptitud del suelo</strong> y la <strong>temperatura promedio</strong>.</span>
              </p>
            </div>

            <div>
              <h3 className="sub-title">Qué tan preciso es</h3>
              <div className="metric-tiles">
                {METRICAS_MODELO.map((m) => (
                  <div key={m.l} className="metric-tile">
                    <div className="v">{m.v}</div>
                    <div className="l">{m.l}</div>
                    <p>{m.d}</p>
                  </div>
                ))}
              </div>
              <p className="fine-print">
                Medido sobre el 20 % de años más recientes, que el modelo no vio al entrenar. Los ajustes del modelo se eligieron
                con validación cruzada temporal de 5 bloques. Las métricas exactas de cada entrenamiento quedan registradas en la base de datos.
              </p>
              <h3 className="sub-title" style={{ marginTop: 28 }}>Sin caja negra</h3>
              <p className="body-text">
                Con la técnica SHAP, cada predicción muestra cuánto sumó o restó cada factor. Lo verás en la página de
                Predicción, en el panel “Por qué este resultado”.
              </p>
            </div>
          </div>
        </div>
      </section>

      {/* ── Fuentes ──────────────────────────────────────────── */}
      <section id="fuentes" className="section section-gray">
        <div className="container">
          <SectionHead eyebrow="Fuentes de datos" title={`${fuentes.length || 14} conjuntos de datos abiertos`}>
            Los marcados como estratégicos forman parte de la Hoja de Ruta Sectorial Agropecuaria del Plan Nacional de
            Datos Abiertos.
          </SectionHead>
          <div className="card">
            <div className="table-scroll">
              <table className="data-table">
                <thead>
                  <tr><th>Conjunto de datos</th><th>Entidad</th>{hayConteos && <th>Registros</th>}<th>Enlace</th></tr>
                </thead>
                <tbody>
                  {fuentes.length === 0 && (
                    <tr><td colSpan={4} className="muted">Cargando fuentes…</td></tr>
                  )}
                  {fuentes.map((f) => (
                    <tr key={f.id}>
                      <td>
                        <strong>{f.titulo}</strong>
                        {f.estrategico && <span className="tag-strategic">Estratégico</span>}
                      </td>
                      <td>{f.entidad}</td>
                      {hayConteos && <td className="num">{f.filas != null ? fmtCompact(f.filas) : "—"}</td>}
                      <td>
                        <a href={f.uri} target="_blank" rel="noopener noreferrer" className="ext-link">
                          Ver fuente <Icon.external />
                        </a>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </section>

      {/* ── Limitaciones ─────────────────────────────────────── */}
      <section className="section">
        <div className="container">
          <SectionHead eyebrow="Limitaciones" tone="amber" title="Lo que el modelo no puede ver">
            Conocer los límites es parte de usar bien la herramienta. Estas son las principales, y lo que planeamos para
            superarlas.
          </SectionHead>
          <div className="limits-grid">
            <div className="limits-list">
              {LIMITACIONES.map((l) => (
                <div key={l.titulo} className="limit-item">
                  <Icon.alert size={16} />
                  <div><strong>{l.titulo}</strong><p>{l.texto}</p></div>
                </div>
              ))}
            </div>
            <div className="future-card">
              <h3><Icon.sparkles size={16} /> Próximos pasos</h3>
              <ul>
                {TRABAJO_FUTURO.map((t) => <li key={t}>{t}</li>)}
              </ul>
            </div>
          </div>
        </div>
      </section>

      {/* ── Sostenibilidad + detalle técnico ─────────────────── */}
      <section className="section section-gray">
        <div className="container">
          <SectionHead eyebrow="Abierto y sostenible" title="Hecho para durar más allá del equipo original" />
          <div className="grid-3">
            <div className="audience-card">
              <span className="icon-chip"><Icon.code size={18} /></span>
              <h3>Código abierto</h3>
              <p>El pipeline de datos, los modelos y esta web están en GitHub. Los modelos se reentrenan con un solo comando.</p>
            </div>
            <div className="audience-card">
              <span className="icon-chip"><Icon.database size={18} /></span>
              <h3>Solo datos públicos</h3>
              <p>No depende de datos privados ni de licencias: cualquier entidad puede replicarlo o extenderlo.</p>
            </div>
            <div className="audience-card">
              <span className="icon-chip"><Icon.layers size={18} /></span>
              <h3>Bajo costo de operación</h3>
              <p>Funciona sobre servicios en la nube de bajo costo: menos de 45 dólares al mes en su fase actual.</p>
            </div>
          </div>

          <details className="dev-details">
            <summary><Icon.code size={16} /> Detalles técnicos para desarrolladores <Icon.chevron className="chev" /></summary>
            <div className="dev-body">
              <div className="dev-grid">
                <div>
                  <h4>Arquitectura</h4>
                  <ul>
                    <li>Pipeline ETL en Python: extracción Socrata/GeoServer, limpieza y carga (<code>run_pipeline.py</code>).</li>
                    <li>PostgreSQL con esquema estrella: 6 dimensiones, 7 tablas de hechos y 2 de predicción (<code>load/schema.sql</code>).</li>
                    <li>XGBoost + Optuna (200 trials, TimeSeriesSplit 5-fold); SHAP persistido por predicción.</li>
                    <li>IsolationForest para detectar predicciones atípicas.</li>
                    <li>Web en Next.js; asistente con Claude y consultas SQL mediante herramientas.</li>
                  </ul>
                </div>
                <div>
                  <h4>Recursos</h4>
                  <ul className="dev-links">
                    <li><a href={REPO_URL} target="_blank" rel="noopener noreferrer"><Icon.github size={14} /> Repositorio en GitHub</a></li>
                    <li><a href={DOCS_URL} target="_blank" rel="noopener noreferrer"><Icon.book size={14} /> Documentación (diccionario de datos, arquitectura, validación)</a></li>
                    <li><a href="/api/openapi" target="_blank" rel="noopener noreferrer"><Icon.code size={14} /> API pública (OpenAPI 3.1)</a></li>
                  </ul>
                  <h4 style={{ marginTop: 18 }}>Reproducir</h4>
                  <pre className="code-block">{`python run_pipeline.py --mode all --once
python -m validate.anova_tests --verbose
cd web && npm install && npm run dev`}</pre>
                </div>
              </div>

              {calidad.length > 0 && (
                <>
                  <h4 style={{ marginTop: 24 }}>Calidad por fuente (última extracción)</h4>
                  <div className="table-scroll">
                    <table className="data-table compact">
                      <thead>
                        <tr><th>Fuente</th><th>Filas</th><th>Columnas</th><th>Completitud media</th><th>Duplicados</th><th>Extraído</th></tr>
                      </thead>
                      <tbody>
                        {calidad.map((c) => (
                          <tr key={c.fuente}>
                            <td><code>{c.fuente}</code></td>
                            <td className="num">{c.filas?.toLocaleString("es-CO")}</td>
                            <td className="num">{c.columnas}</td>
                            <td className={`num ${c.completitud_media >= 95 ? "pos" : c.completitud_media < 80 ? "neg" : ""}`}>{c.completitud_media}%</td>
                            <td className={`num ${c.duplicados > 0 ? "neg" : ""}`}>{c.duplicados}</td>
                            <td className="num">{c.extraido_at}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </>
              )}
            </div>
          </details>
        </div>
      </section>
    </>
  );
}
