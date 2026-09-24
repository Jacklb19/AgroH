"use client";
import { useEffect, useState } from "react";
import SerieChart from "./charts/SerieChart";
import HBars from "./charts/HBars";
import PreciosInsumos from "./PreciosInsumos";
import { Icon } from "./icons";
import { SectionHead, Source } from "./ui";
import { fmtNum } from "@/lib/format";

const BASE_PBI = "https://app.powerbi.com/view?r=eyJrIjoiYTU5ODY5MmMtNzVmNy00MTQ0LWFhODItZTg0ODIzNDI0MTk5IiwidCI6IjhkMzY4MzZlLTZiNzUtNGRlNi1iYWI5LTVmNGIxNzc1NDI3ZiIsImMiOjR9";

const TABS = [
  { id: "resumen",    label: "Resumen",            icon: Icon.barChart },
  { id: "precios",    label: "Precios e insumos",  icon: Icon.trend },
  { id: "panorama",   label: "Panorama general",   icon: Icon.target, pageName: "56b0cd0922bc5dea7a03",
    guia: ["Municipios, cultivos y registros cubiertos.", "Mapa por municipio y evolución del rendimiento.", "Filtra por año, departamento, municipio o cultivo."] },
  { id: "produccion", label: "Producción",         icon: Icon.wheat, pageName: "d1e8cfb98623cb22537b",
    guia: ["Producción total y área sembrada por año.", "Municipios y cultivos con mayor producción.", "Comparación entre regiones."] },
  { id: "clima",      label: "Clima y alertas",    icon: Icon.cloudRain, pageName: "763fb90f5adea074007a",
    guia: ["Lluvia y temperatura mensual por estación.", "Fase de El Niño / La Niña en cada periodo.", "Distribución territorial del clima."] },
];

const pbiUrl = (t) => `${BASE_PBI}&pageName=${t.pageName}`;
const pct = (v, d = 0) => `${fmtNum(v * 100, d)} %`;

/* ── Calidad del modelo ──────────────────────────────────────────────── */
function CalidadModelo({ modelo }) {
  const m = modelo.metricas;
  const ref = m.referencias;
  const filas = [
    { l: "Modelo AgroIA",               mae: ref.modelo_mismas_filas.mae, rmse: ref.modelo_mismas_filas.rmse, main: true },
    { l: "Repetir el año anterior",     mae: ref.anio_anterior.mae,       rmse: ref.anio_anterior.rmse },
    { l: "Promedio histórico",          mae: ref.promedio_historico.mae,  rmse: ref.promedio_historico.rmse },
  ];
  const maxR = Math.max(...filas.map((f) => f.rmse));
  return (
    <div className="card">
      <div className="card-head">
        <div>
          <h3>¿Qué tan bien predice el modelo?</h3>
          <div className="panel-sub">
            Probado con 2022, 2023 y 2024 sin que el modelo hubiera visto esos años: {fmtNum(m.modelo.n, 0)} pronósticos comparados con lo que realmente pasó.
          </div>
        </div>
      </div>
      <div className="card-body">
        <div className="kpi-row">
          <div className="kpi"><div className="v">{fmtNum(m.modelo.r2, 2)}</div><div className="l">R²: explica el {fmtNum(m.modelo.r2 * 100, 0)} % de las diferencias de rendimiento</div></div>
          <div className="kpi"><div className="v">{pct(m.modelo.error_relativo_mediano, 1)}</div><div className="l">Error típico (mediana) frente al valor real</div></div>
          <div className="kpi"><div className="v">{pct(m.cobertura_rango_90)}</div><div className="l">Casos en que el valor real cayó dentro del rango probable</div></div>
        </div>
        <div className="compare-bars">
          <div className="compare-head"><span>Frente a métodos simples</span><span>Error grande (RMSE, t/ha)</span></div>
          {filas.map((f) => (
            <div key={f.l} className={`cmp ${f.main ? "main" : ""}`}>
              <span className="cmp-lbl">{f.l}</span>
              <div className="cmp-track"><span style={{ width: `${(f.rmse / maxR) * 100}%` }} /></div>
              <span className="cmp-val">{fmtNum(f.rmse, 2)}</span>
            </div>
          ))}
        </div>
        <p className="panel-foot">
          Muchos municipios reportan el mismo rendimiento varios años seguidos; por eso “repetir el año anterior” es difícil
          de superar en el error típico ({fmtNum(ref.anio_anterior.mae, 2)} frente a {fmtNum(ref.modelo_mismas_filas.mae, 2)} t/ha del modelo).
          El modelo reduce los errores grandes y además entrega un rango probable y escenarios.
        </p>
      </div>
    </div>
  );
}

/* ── Explorar un cultivo ─────────────────────────────────────────────── */
function ExplorarCultivo({ anio }) {
  const [lista, setLista] = useState([]);
  const [id, setId] = useState(null);
  const [data, setData] = useState(null);

  useEffect(() => {
    fetch("/api/cultivo").then((r) => r.json()).then((l) => { if (Array.isArray(l) && l.length) { setLista(l); setId(l[0].id); } });
  }, []);
  useEffect(() => {
    if (!id) return;
    setData(null);
    fetch(`/api/cultivo?id=${id}&anio=${anio}`).then((r) => r.json()).then(setData).catch(() => setData({ error: true }));
  }, [id, anio]);

  const nombre = lista.find((c) => c.id === id)?.nombre || "";
  const c = data?.cambios;
  const p = (v) => (c?.total ? Math.round((v / c.total) * 100) : 0);

  return (
    <div className="card" style={{ marginTop: 16 }}>
      <div className="card-head column">
        <div>
          <h3>Explora un cultivo</h3>
          <div className="panel-sub">Rendimiento típico en Colombia (mediana de los municipios), cómo le fue al modelo y su pronóstico.</div>
        </div>
        <div className="chip-row" role="tablist" aria-label="Cultivo">
          {lista.map((cu) => (
            <button key={cu.id} role="tab" aria-selected={cu.id === id} className={`chip ${cu.id === id ? "active" : ""}`} onClick={() => setId(cu.id)}>
              {cu.nombre}
            </button>
          ))}
        </div>
      </div>
      <div className="card-body">
        {!data && <div className="panel-empty">Cargando {nombre.toLowerCase()}…</div>}
        {data?.error && <div className="panel-empty">No fue posible cargar este cultivo.</div>}
        {data?.serie && (
          <div className="explore-grid fade-in">
            <div>
              <div className="mini-title">{nombre}: rendimiento típico por año (t/ha)</div>
              <SerieChart rows={data.serie} height={250} />
            </div>
            <div className="explore-side">
              <div className="mini-title">¿Qué se espera en {anio}?</div>
              <div className="stack-bar big" role="img" aria-label={`Sube ${p(c.sube)}%, estable ${p(c.estable)}%, baja ${p(c.baja)}%`}>
                <span className="sube" style={{ width: `${p(c.sube)}%` }} />
                <span className="estable" style={{ width: `${p(c.estable)}%` }} />
                <span className="baja" style={{ width: `${p(c.baja)}%` }} />
              </div>
              <div className="stack-legend">
                <span><i className="sube" /> Sube <strong>{fmtNum(c.sube, 0)}</strong></span>
                <span><i className="estable" /> Estable <strong>{fmtNum(c.estable, 0)}</strong></span>
                <span><i className="baja" /> Baja <strong>{fmtNum(c.baja, 0)}</strong></span>
              </div>
              <p className="panel-foot" style={{ marginTop: 6 }}>Municipios según el cambio esperado frente a su último año registrado (±5 %).</p>
              <div className="mini-title" style={{ marginTop: 18 }}>Mayor rendimiento esperado en {anio}</div>
              <HBars items={data.top.map((t) => ({ l: t.municipio, v: t.rendimiento }))} />
              <p className="panel-foot">Municipios con al menos 3 años de datos. Se excluyen cifras por encima del 95 % de lo registrado para el cultivo, que suelen ser errores de reporte.</p>
            </div>
          </div>
        )}
        <Source>Encuesta de Evaluaciones Agropecuarias (EVA) · pronósticos AgroIA</Source>
      </div>
    </div>
  );
}

function Resumen() {
  const [modelo, setModelo] = useState(null);
  useEffect(() => {
    fetch("/api/modelo").then((r) => r.json()).then(setModelo).catch(() => setModelo({ error: true }));
  }, []);
  if (!modelo) return <div className="panel-empty">Cargando datos…</div>;
  if (modelo.error || !modelo.metricas) return <div className="notice-bar"><Icon.alert size={15} /> Los datos no están disponibles en este momento.</div>;
  const anio = new Date().getFullYear();
  const e = modelo.enso;
  return (
    <div className="fade-in">
      <div className="panel-grid-2">
        <CalidadModelo modelo={modelo} />
        <div className="card enso-card">
          <div className="card-head"><div><h3>El Niño / La Niña hoy</h3><div className="panel-sub">Índice oceánico ONI de la NOAA</div></div></div>
          {e && (
            <div className="card-body">
              <div className={`enso-phase ${e.fase === "El Niño" ? "nino" : e.fase === "La Niña" ? "nina" : ""}`}>
                <Icon.sun size={22} />
                <div><strong>{e.fase === "Neutro" ? "Fase neutral" : e.fase}</strong><span>ONI {fmtNum(e.oni, 2)} · {e.nombre_mes?.toLowerCase()} de {e.anio}</span></div>
              </div>
              <p className="body-text" style={{ marginTop: 14 }}>
                {e.fase === "Neutro"
                  ? "Sin señal fuerte de El Niño ni de La Niña. Las lluvias tienden a seguir su patrón habitual de dos temporadas."
                  : e.fase === "El Niño"
                    ? "El Niño suele reducir las lluvias en las regiones Andina y Caribe."
                    : "La Niña suele aumentar las lluvias en buena parte del país."}
              </p>
              <p className="panel-foot">El índice se considera El Niño por encima de +0,5 y La Niña por debajo de −0,5.</p>
            </div>
          )}
        </div>
      </div>
      <ExplorarCultivo anio={anio} />
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
        <ul>{tab.guia.map((g) => <li key={g}>{g}</li>)}</ul>
        <p className="pbi-guide-tip">
          Pasa el cursor sobre los gráficos para ver el detalle y haz clic en un elemento para filtrar el resto del tablero.
        </p>
        <p className="pbi-guide-tip warn">
          Los tableros de Power BI se actualizan por separado. Si ves rendimientos muy altos, corresponden a la versión
          anterior de los datos; las cifras corregidas están en la pestaña Resumen.
        </p>
        <a href={pbiUrl(tab)} target="_blank" rel="noopener noreferrer" className="btn-outline">
          <Icon.maximize /> Abrir en pantalla completa
        </a>
      </aside>
      <div className="pbi-panel">
        <div className="pbi-frame-wrap">
          {!loaded && (
            <div className="pbi-loading"><div className="lupa spin"><Icon.refresh size={24} /></div>Cargando tablero de Power BI…</div>
          )}
          <iframe key={tab.id} title={`Tablero Power BI: ${tab.label}`} src={pbiUrl(tab)} allowFullScreen
            className="pbi-iframe" onLoad={() => setLoaded(true)} />
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
          Empieza por el resumen: qué tan confiable es el modelo y qué se espera para cada cultivo. Luego revisa precios
          e insumos, o profundiza en los tableros interactivos.
        </SectionHead>
        <div className="tabs" role="tablist" aria-label="Vistas de datos">
          {TABS.map((t) => (
            <button key={t.id} role="tab" aria-selected={tabId === t.id}
              className={`tab ${tabId === t.id ? "active" : ""}`} onClick={() => setTabId(t.id)}>
              <t.icon size={15} /> {t.label}
            </button>
          ))}
        </div>
        {tabId === "resumen" && <Resumen />}
        {tabId === "precios" && <PreciosInsumos />}
        {tab.pageName && <PowerBI tab={tab} />}
      </div>
    </section>
  );
}
