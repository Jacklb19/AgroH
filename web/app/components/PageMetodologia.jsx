"use client";
import { useEffect, useState } from "react";
import { Icon } from "./icons";

function fmtFilas(n) {
  if (n == null) return "—";
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1).replace(/\.0$/, "") + "M";
  if (n >= 1_000)     return (n / 1_000).toFixed(1).replace(/\.0$/, "") + "k";
  return n.toLocaleString("es-CO");
}

export default function PageMetodologia() {
  const [fuentes, setFuentes] = useState([]);
  const [calidad, setCalidad] = useState([]);
  useEffect(() => {
    fetch("/api/catalogo").then((r) => r.json()).then(setFuentes).catch(() => setFuentes([]));
    fetch("/api/calidad").then((r) => r.json()).then((d) => setCalidad(d.reportes || [])).catch(() => setCalidad([]));
  }, []);

  return (
    <section className="section">
      <div className="container">
        <div className="section-head">
          <span className="eyebrow blue">Metodología</span>
          <h2>Pipeline reproducible y auditable</h2>
          <p>Cada paso del modelado está documentado, versionado y validado contra datos retenidos. Métricas calculadas en tiempo de entrenamiento y persistidas en <code>model_version</code>.</p>
        </div>

        <div className="card" style={{ marginBottom: 28 }}>
          <div className="card-head">
            <div>
              <h3>Alineación con la Hoja de Ruta Sectorial Agropecuaria</h3>
              <div className="panel-sub">Datos abiertos estratégicos · Plan Nacional de Datos Abiertos (MinTIC)</div>
            </div>
            <span className="src-badge">datos.gov.co</span>
          </div>
          <div className="card-body">
            <p style={{ marginBottom: 12, fontSize: 13, color: "var(--gray-700)" }}>
              AgroIA prioriza la integración de los conjuntos definidos como <strong>estratégicos</strong> en la Hoja de Ruta Sectorial Agropecuaria y en el Plan Nacional de Datos Abiertos. Cada fuente entra al pipeline ETL con su URI Socrata o GeoServer y queda trazable en <code>config/settings.py</code>.
            </p>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))", gap: 10 }}>
              {(fuentes.length > 0 ? fuentes : []).map((f) => (
                <div key={f.id} style={{ padding: "10px 12px", border: "1px solid var(--gray-200)", borderRadius: 8, background: "white", display: "flex", flexDirection: "column", gap: 4 }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 8 }}>
                    <a href={f.uri} target="_blank" rel="noopener noreferrer" style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--blue-700)", textDecoration: "none" }}>
                      {f.id} ↗
                    </a>
                    {f.estrategico && (
                      <span style={{ fontSize: 10, fontWeight: 600, padding: "2px 6px", borderRadius: 4, background: "#1a7a4a", color: "white", textTransform: "uppercase", letterSpacing: 0.4 }}>
                        Estratégico
                      </span>
                    )}
                  </div>
                  <div style={{ fontSize: 13, fontWeight: 500 }}>{f.titulo}</div>
                  <div style={{ fontSize: 11, color: "var(--gray-600)", display: "flex", justifyContent: "space-between" }}>
                    <span>{f.entidad}</span>
                    {f.tabla && <code style={{ fontSize: 10 }}>{f.tabla}</code>}
                  </div>
                  <div style={{ fontSize: 11, color: f.filas != null ? "#1a7a4a" : "var(--gray-500)" }}>
                    {f.filas != null ? `${fmtFilas(f.filas)} filas en BD` : "Sin datos cargados"}
                  </div>
                </div>
              ))}
              {fuentes.length === 0 && (
                <div style={{ gridColumn: "1 / -1", fontSize: 12, color: "var(--gray-500)", textAlign: "center", padding: 12 }}>
                  Cargando catálogo de fuentes…
                </div>
              )}
            </div>
          </div>
        </div>

        <div className="method-grid">
          <div className="method-card">
            <div className="icon-wrap"><Icon.database /></div>
            <h3>Fuentes de datos abiertos</h3>
            <ul>
              <li>DANE — evaluaciones agropecuarias municipales (A04/A05)</li>
              <li>IDEAM — series climáticas (precipitación, temperatura)</li>
              <li>UPRA — SIPRA aptitud agrícola por suelo</li>
              <li>DANE — SIPSA precios mayoristas + IPIA insumos</li>
              <li>NOAA — índice ENSO mensual</li>
            </ul>
          </div>
          <div className="method-card">
            <div className="icon-wrap"><Icon.brush /></div>
            <h3>Flujo ETL</h3>
            <ul>
              <li>Extracción Socrata + GeoServer + scraping institucional</li>
              <li>Armonización municipio · cultivo · ciclo (DIVIPOLA)</li>
              <li>Detección de outliers por z-score robusto</li>
              <li>Feature store en Parquet versionado</li>
            </ul>
          </div>
          <div className="method-card">
            <div className="icon-wrap"><Icon.layers /></div>
            <h3>Star schema</h3>
            <ul>
              <li>6 dimensiones · 7 hechos · 2 tablas de predicción</li>
              <li>fact_produccion_agricola · fact_clima_mensual</li>
              <li>fact_precios_mayoristas · fact_precios_insumos</li>
              <li>fact_alerta_enso · fact_aptitud_suelo · fact_censo</li>
            </ul>
          </div>
          <div className="method-card green">
            <div className="icon-wrap"><Icon.tree /></div>
            <h3>Modelo principal</h3>
            <ul>
              <li>XGBoost con tuning bayesiano (Optuna · 200 trials)</li>
              <li>~40 features: clima estacional + lags 1y/3y + ENSO + SIPSA + SIPRA</li>
              <li>Hold-out temporal por año + TimeSeriesSplit 5-fold</li>
              <li>Inferencia sobre municipios × cultivos del star schema</li>
            </ul>
          </div>
          <div className="method-card amber">
            <div className="icon-wrap"><Icon.ruler /></div>
            <h3>Validación y métricas</h3>
            <ul>
              <li>Métricas reales en <code>model_version.metricas_json</code></li>
              <li>Train: años &lt; 80% percentil · Test: últimos 20%</li>
              <li>Reporta MAE, RMSE, R², CV-MAE de Optuna</li>
              <li>Banda de confianza ± MAE en cada predicción</li>
            </ul>
          </div>
          <div className="method-card red">
            <div className="icon-wrap"><Icon.shield /></div>
            <h3>Explicabilidad e interpretabilidad</h3>
            <ul>
              <li>SHAP TreeExplainer · top-10 importancia global</li>
              <li>Top-3 factores con signo por predicción</li>
              <li>Persistido en <code>pred_rendimiento.shap_top</code> (JSONB)</li>
              <li>Visualizado en panel "Por qué esta predicción"</li>
            </ul>
          </div>
        </div>

        <div className="metrics-table-wrap">
          <div className="head">
            <div>
              <h3>XGBoost vs. baselines</h3>
              <p>Hold-out temporal · evaluación sobre los últimos años disponibles.</p>
            </div>
            <span className="src-badge">model_version · activo</span>
          </div>
          <table className="metrics-table">
            <thead>
              <tr><th>Modelo</th><th colSpan={3}>Métricas</th><th>Notas</th></tr>
            </thead>
            <tbody>
              <tr>
                <td><strong>XGBoost — campeón</strong> <span className="winner-cell">activo</span></td>
                <td className="num best" colSpan={3} style={{ textAlign: "left", fontFamily: "var(--font-mono)", fontSize: 12 }}>
                  Métricas dinámicas — ver <code>model_version.metricas_json</code> tras cada entrenamiento.
                </td>
                <td className="num best" style={{ fontSize: 11 }}>Optuna 200 trials · TS-Split 5</td>
              </tr>
              <tr>
                <td>Regresión lineal multivariada</td>
                <td className="num mid" colSpan={3}>Baseline interpretable</td>
                <td className="num mid" style={{ fontSize: 11 }}>Referencia</td>
              </tr>
              <tr>
                <td>Promedio móvil quinquenal (naive)</td>
                <td className="num worst" colSpan={3}>Baseline mínimo</td>
                <td className="num worst" style={{ fontSize: 11 }}>Línea base</td>
              </tr>
            </tbody>
          </table>
          <div style={{ fontSize: 12, color: "var(--gray-600)", marginTop: 10 }}>
            ℹ Las métricas exactas se calculan en cada corrida de <code>models/train_rendimiento.py</code> y se persisten en la tabla <code>model_version</code>. Para ver el último valor activo: <code>SELECT metricas_json FROM model_version WHERE activo = TRUE</code>.
          </div>
          <div style={{ fontSize: 12, color: "var(--gray-600)", marginTop: 8 }}>
            🛠 API pública documentada en <a href="/api/openapi" target="_blank" rel="noopener noreferrer"><code>/api/openapi</code></a> (OpenAPI 3.1).
          </div>
        </div>

        {/* ── Auditoría de calidad de fuentes ─────────────────────── */}
        {calidad.length > 0 && (
          <div className="card" style={{ marginTop: 28 }}>
            <div className="card-head">
              <div>
                <h3>Auditoría de calidad por fuente</h3>
                <div className="panel-sub">Reportes generados durante la extracción · `data/quality_reports/`</div>
              </div>
              <span className="src-badge">utils/extraction_quality</span>
            </div>
            <div className="card-body" style={{ overflowX: "auto" }}>
              <table className="metrics-table" style={{ minWidth: 720 }}>
                <thead>
                  <tr>
                    <th>Fuente</th>
                    <th>Filas</th>
                    <th>Columnas</th>
                    <th>Completitud media</th>
                    <th>Duplicados</th>
                    <th>Última extracción</th>
                  </tr>
                </thead>
                <tbody>
                  {calidad.map((c) => {
                    const cls = c.completitud_media >= 95 ? "best" : c.completitud_media >= 80 ? "mid" : "worst";
                    return (
                      <tr key={c.fuente}>
                        <td><code style={{ fontSize: 11 }}>{c.fuente}</code></td>
                        <td className="num">{c.filas?.toLocaleString("es-CO")}</td>
                        <td className="num">{c.columnas}</td>
                        <td className={`num ${cls}`}>{c.completitud_media}%</td>
                        <td className={`num ${c.duplicados > 0 ? "worst" : "best"}`}>{c.duplicados}</td>
                        <td style={{ fontSize: 11, fontFamily: "var(--font-mono)" }}>{c.extraido_at}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
              <div style={{ fontSize: 11, color: "var(--gray-600)", marginTop: 8 }}>
                Cada extractor pasa por <code>utils.extraction_quality.standardize</code>: validación de schema, coerción de tipos, descarte de NULL en columnas críticas, filtro de rangos sanos y deduplicación por clave natural.
              </div>
            </div>
          </div>
        )}
      </div>
    </section>
  );
}
