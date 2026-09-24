"use client";
import { useEffect, useState } from "react";
import DualLineChart from "./charts/DualLineChart";
import Donut from "./charts/Donut";
import HBars from "./charts/HBars";
import { Icon } from "./icons";
import { SectionHead, Source } from "./ui";

const COLORS = ["#d97706", "#1e4d7b", "#dc2626", "#1a7a4a", "#7c3aed", "#0891b2"];

const BASE_PBI = "https://app.powerbi.com/view?r=eyJrIjoiYTU5ODY5MmMtNzVmNy00MTQ0LWFhODItZTg0ODIzNDI0MTk5IiwidCI6IjhkMzY4MzZlLTZiNzUtNGRlNi1iYWI5LTVmNGIxNzc1NDI3ZiIsImMiOjR9";

const TABS = [
  { id: "resumen",    label: "Resumen",          icon: Icon.barChart },
  { id: "panorama",   label: "Panorama general", icon: Icon.target, pageName: "56b0cd0922bc5dea7a03",
    guia: [
      "Cuántos municipios, cultivos, predicciones y alertas activas hay.",
      "Mapa por municipio y evolución del rendimiento promedio.",
      "Filtra por año, departamento, municipio o cultivo.",
    ] },
  { id: "produccion", label: "Producción",       icon: Icon.wheat, pageName: "d1e8cfb98623cb22537b",
    guia: [
      "Rendimiento histórico por año, producción total y área sembrada.",
      "Lo que ocurrió frente a lo que predijo el modelo.",
      "Municipios y cultivos con mejor rendimiento.",
    ] },
  { id: "clima",      label: "Clima y alertas",  icon: Icon.cloudRain, pageName: "763fb90f5adea074007a",
    guia: [
      "Lluvia y temperatura mensual.",
      "Fase de El Niño / La Niña en cada periodo.",
      "Mapa y distribución del riesgo climático: bajo, medio y alto.",
    ] },
];

const pbiUrl = (t) => (t.pageName ? `${BASE_PBI}&pageName=${t.pageName}` : BASE_PBI);

/* ── Resumen con gráficas nativas (datos de /api/dashboards) ──────────── */
function Resumen() {
  const [data, setData] = useState(null);
  const [err,  setErr]  = useState(false);

  useEffect(() => {
    fetch("/api/dashboards").then((r) => r.json()).then(setData).catch(() => setErr(true));
  }, []);

  if (err)   return <div className="panel-empty">No fue posible cargar los datos. Intenta recargar la página.</div>;
  if (!data) return <div className="panel-empty">Cargando datos…</div>;

  const totalAlertas = data.alertas_por_tipo.reduce((s, a) => s + a.total, 0);
  const sem = data.semaforo;
  const semTotal = sem.bajo + sem.medio + sem.alto || 1;
  const semRows = [
    { k: "alto",  l: "Alto",  d: "Se recomiendan precauciones", v: sem.alto },
    { k: "medio", l: "Medio", d: "Hay factores a vigilar",      v: sem.medio },
    { k: "bajo",  l: "Bajo",  d: "Condiciones favorables",      v: sem.bajo },
  ];

  return (
    <div className="fade-in">
      {!data.fromDB && (
        <div className="notice-bar"><Icon.info size={15} /> La base de datos no está disponible en este momento; se muestran valores de ejemplo.</div>
      )}

      <div className="panel-grid-2">
        <div className="card">
          <div className="card-head">
            <div>
              <h3>¿Acierta el modelo?</h3>
              <div className="panel-sub">Rendimiento medio nacional (t/ha): lo que ocurrió frente a lo que predijo el modelo. Cuanto más juntas las líneas, mejor.</div>
            </div>
          </div>
          <div className="card-body">
            <DualLineChart height={280} data={data.serie_rendimiento} />
            <Source>DANE (producción agrícola) · predicciones AgroIA</Source>
          </div>
        </div>

        <div className="card">
          <div className="card-head">
            <div>
              <h3>Nivel de riesgo climático</h3>
              <div className="panel-sub">Distribución de las alertas activas por nivel.</div>
            </div>
          </div>
          <div className="card-body">
            {semRows.map((s) => {
              const pct = Math.round((s.v / semTotal) * 100);
              return (
                <div key={s.k} className={`risk-row ${s.k}`}>
                  <span className={`risk-dot ${s.k}`} />
                  <div className="risk-info">
                    <div className="lbl">Riesgo {s.l.toLowerCase()}</div>
                    <div className="desc">{s.d}</div>
                    <div className="risk-bar"><span className={s.k} style={{ width: `${pct}%` }} /></div>
                  </div>
                  <div className="risk-pct">{pct}%</div>
                </div>
              );
            })}
            <Source>Alertas climáticas AgroIA · IDEAM · NOAA</Source>
          </div>
        </div>
      </div>

      <div className="panel-grid-2">
        <div className="card">
          <div className="card-head">
            <div>
              <h3>Qué tipo de alertas hay</h3>
              <div className="panel-sub">{totalAlertas.toLocaleString("es-CO")} alertas activas según el evento que las origina.</div>
            </div>
          </div>
          <div className="card-body">
            <div className="donut-row">
              <Donut size={190} segments={data.alertas_por_tipo.map((a, i) => ({ label: a.tipo, value: a.total, color: COLORS[i % COLORS.length] }))} />
              <div className="bars">
                {data.alertas_por_tipo.map((a, i) => (
                  <div className="bar-item" key={a.tipo}>
                    <div className="bar-meta"><span className="lbl">{a.tipo}</span><span className="val">{a.pct}%</span></div>
                    <div className="bar-track"><span className="bar-fill" style={{ width: `${a.pct}%`, background: COLORS[i % COLORS.length] }} /></div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>

        <div className="card">
          <div className="card-head">
            <div>
              <h3>Municipios con mayor rendimiento esperado</h3>
              <div className="panel-sub">Promedio de todas las predicciones del municipio, en toneladas por hectárea.</div>
            </div>
          </div>
          <div className="card-body">
            <HBars items={data.top_municipios.map((m) => ({ l: m.municipio, v: m.rendimiento }))} />
            <Source>Predicciones AgroIA</Source>
          </div>
        </div>
      </div>

      {Array.isArray(data.anomalias) && data.anomalias.length > 0 && (
        <div className="card" style={{ marginTop: 16 }}>
          <div className="card-head">
            <div>
              <h3>Casos atípicos para revisar</h3>
              <div className="panel-sub">Predicciones que se salen mucho del patrón habitual. Pueden indicar un error en los datos de origen o una situación excepcional en el municipio.</div>
            </div>
          </div>
          <div className="table-scroll">
            <table className="data-table">
              <thead><tr><th>Municipio</th><th>Cultivo</th><th>Rendimiento esperado</th><th>Grado de rareza</th></tr></thead>
              <tbody>
                {data.anomalias.map((a, i) => (
                  <tr key={i}>
                    <td><strong>{a.municipio}</strong></td>
                    <td>{a.cultivo}</td>
                    <td className="num">{a.rendimiento} <span className="muted">t/ha</span></td>
                    <td className="num neg">{a.score}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

/* ── Tablero Power BI con guía de lectura ────────────────────────────── */
function PowerBI({ tab }) {
  const [loaded, setLoaded] = useState(false);
  useEffect(() => { setLoaded(false); }, [tab.id]);

  return (
    <div className="pbi-layout fade-in">
      <aside className="pbi-guide">
        <div className="pbi-guide-title"><Icon.compass size={15} /> Qué encontrarás</div>
        <ul>
          {tab.guia.map((g) => <li key={g}>{g}</li>)}
        </ul>
        <p className="pbi-guide-tip">
          Pasa el cursor sobre los gráficos para ver el detalle y haz clic en un elemento para filtrar el resto del tablero.
        </p>
        <a href={pbiUrl(tab)} target="_blank" rel="noopener noreferrer" className="btn-outline">
          <Icon.maximize /> Abrir en pantalla completa
        </a>
      </aside>
      <div className="pbi-panel">
        <div className="pbi-frame-wrap">
          {!loaded && (
            <div className="pbi-loading">
              <div className="lupa spin"><Icon.refresh size={24} /></div>
              Cargando tablero de Power BI…
            </div>
          )}
          <iframe
            key={tab.id}
            title={`Tablero Power BI: ${tab.label}`}
            src={pbiUrl(tab)}
            allowFullScreen
            className="pbi-iframe"
            onLoad={() => setLoaded(true)}
          />
        </div>
      </div>
    </div>
  );
}

export default function PageDatos() {
  const [tabId, setTabId] = useState("resumen");
  const tab = TABS.find((t) => t.id === tabId);

  return (
    <section className="section page-top">
      <div className="container">
        <SectionHead eyebrow="Explorar datos" tone="blue" title="El agro colombiano en datos">
          Empieza por el resumen para ver lo esencial. Si quieres profundizar, los tres tableros interactivos
          permiten filtrar por año, departamento, municipio y cultivo.
        </SectionHead>

        <div className="tabs" role="tablist" aria-label="Vistas de datos">
          {TABS.map((t) => (
            <button
              key={t.id}
              role="tab"
              aria-selected={tabId === t.id}
              className={`tab ${tabId === t.id ? "active" : ""}`}
              onClick={() => setTabId(t.id)}
            >
              <t.icon size={15} /> {t.label}
            </button>
          ))}
        </div>

        {tabId === "resumen" ? <Resumen /> : <PowerBI tab={tab} />}
      </div>
    </section>
  );
}
