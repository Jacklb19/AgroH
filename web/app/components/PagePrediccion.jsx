"use client";
import { useState, useEffect, useRef, useMemo } from "react";
import ConfidenceBar from "./charts/ConfidenceBar";
import SerieChart from "./charts/SerieChart";
import GemeloDigital from "./GemeloDigital";
import TourOverlay from "./TourOverlay";
import { Icon } from "./icons";
import { SectionHead, RiskBadge } from "./ui";
import { fmtNum, signed, temporadasDisponibles } from "@/lib/format";
import { labelFeature } from "@/lib/labels";

const TOUR_STEPS = [
  { refKey: "muni",      placement: "right", title: "Dónde vas a sembrar",
    desc: "Elige el departamento y luego el municipio. Verás también el clima de hoy en ese lugar." },
  { refKey: "cultivo",   placement: "right", title: "Qué vas a sembrar",
    desc: "Solo aparecen los cultivos que tienen historia de producción en ese municipio; así el pronóstico siempre se basa en datos reales." },
  { refKey: "temporada", placement: "right", title: "Cuándo vas a sembrar",
    desc: "Solo se ofrecen temporadas cuya ventana de siembra sigue abierta. El pronóstico es del rendimiento de ese año; el semestre ajusta las recomendaciones." },
  { refKey: "submit",    placement: "top",   title: "Consultar", desc: "El resultado aparece en segundos." },
  { refKey: "result",    placement: "left",  title: "Tu resultado",
    desc: "Rendimiento esperado, rango probable, cómo ha variado en el pasado y qué tan preciso ha sido el modelo con este cultivo." },
];

const VARIAB = {
  BAJO:  { label: "Variabilidad baja",  texto: "El rendimiento ha sido estable" },
  MEDIO: { label: "Variabilidad media", texto: "El rendimiento cambia de un año a otro" },
  ALTO:  { label: "Variabilidad alta",  texto: "El rendimiento ha sido muy irregular" },
};
const VARIAB_RIESGO = { BAJO: "bajo", MEDIO: "medio", ALTO: "alto" };

/* ── Resultado ───────────────────────────────────────────────────────── */
function ResultPanel({ r, temporada }) {
  const v = r.variabilidad ? VARIAB[r.variabilidad.nivel] : null;
  const u = r.ultimo_real;
  const cambio = u ? ((r.yhat - u.valor) / u.valor) * 100 : null;
  const serie = [
    ...r.historia.map((h) => ({ anio: h.anio, real: h.real, backtest: h.backtest })),
    { anio: r.anio, pronostico: r.yhat, low: r.low, high: r.high },
  ];

  return (
    <div className="result-filled fade-in">
      <div className="result-head">
        <div>
          <h4>{r.cultivo.nombre}</h4>
          <div className="sub">{r.municipio.nombre}, {r.municipio.departamento} · cosecha {r.anio}</div>
        </div>
        {v && <RiskBadge nivel={VARIAB_RIESGO[r.variabilidad.nivel]} label={v.label} />}
      </div>

      <div className="result-main">
        <div>
          <div className="result-num">{fmtNum(r.yhat)}<small>t/ha</small></div>
          <div className="result-lbl">Rendimiento esperado en {r.anio}</div>
        </div>
        <div className="ci-block">
          <div className="ci-title">Rango probable (9 de cada 10 casos)</div>
          <ConfidenceBar low={r.low} mid={r.yhat} high={r.high} vmin={Math.max(0, r.low * 0.8)} vmax={r.high * 1.08} />
        </div>
      </div>

      <p className="result-summary">
        Para {r.anio} se esperan unas <strong>{fmtNum(r.yhat)} toneladas por hectárea</strong> cosechada.
        {u && (
          <> En {u.anio} se registraron {fmtNum(u.valor)} t/ha
            {Math.abs(cambio) < 3 ? ", así que se espera un rendimiento similar." :
              <>, así que se espera un cambio de <strong className={cambio >= 0 ? "pos" : "neg"}>{signed(cambio, 0)} %</strong>.</>}
          </>
        )}
        {" "}En casos parecidos, el resultado real quedó entre <strong>{fmtNum(r.low)}</strong> y <strong>{fmtNum(r.high)}</strong> t/ha
        nueve de cada diez veces.
      </p>

      <div className="metrics-3">
        <div className={`metric-mini risk-${v ? VARIAB_RIESGO[r.variabilidad.nivel] : ""}`}>
          <div className="v">{r.variabilidad ? `±${r.variabilidad.cv} %` : "—"}</div>
          <div className="l">{v ? `${v.texto} (${r.historia[0]?.anio}–${u?.anio})` : "Pocos años de datos"}</div>
        </div>
        <div className="metric-mini">
          <div className="v">{r.precision_cultivo ? `${r.precision_cultivo.error_relativo} %` : "—"}</div>
          <div className="l">Error típico del modelo en {r.cultivo.nombre.toLowerCase()}</div>
        </div>
        <div className="metric-mini">
          <div className="v">{u ? fmtNum(u.valor) : "—"}</div>
          <div className="l">Último dato real{u ? ` (${u.anio}), t/ha` : ""}</div>
        </div>
      </div>

      <div className="result-chart">
        <div className="mini-title">Historia y pronóstico en {r.municipio.nombre}</div>
        <SerieChart rows={serie} height={210} />
      </div>

      <Escenarios escenarios={r.escenarios} anio={r.anio} />
      <ShapPanel shap={r.shap} ultimo={u} />
      {temporada && (
        <p className="panel-foot">
          Pronóstico del rendimiento anual de {r.anio}. Siembra en el {temporada.semestre === "A" ? "primer" : "segundo"} semestre:
          revisa la pestaña “Qué hacer” para la ventana de siembra.
        </p>
      )}
    </div>
  );
}

function Escenarios({ escenarios, anio }) {
  if (!escenarios || escenarios.length < 2) return null;
  const orden = ["La Niña", "Neutral", "El Niño"];
  const lista = orden.map((e) => escenarios.find((x) => x.escenario === e)).filter(Boolean);
  const neutral = escenarios.find((e) => e.escenario === "Neutral")?.yhat;
  const maxDif = Math.max(...lista.map((e) => Math.abs(e.yhat / neutral - 1)));
  return (
    <div className="scen-panel">
      <div className="mini-title">¿Y si hay El Niño o La Niña en {anio}?</div>
      <div className="scen-row">
        {lista.map((e) => (
          <div key={e.escenario} className={`scen ${e.escenario === "Neutral" ? "base" : ""}`}>
            <span>{e.escenario === "Neutral" ? "Año normal" : e.escenario}</span>
            <strong>{fmtNum(e.yhat)} <small>t/ha</small></strong>
          </div>
        ))}
      </div>
      <p className="scen-note">
        {maxDif < 0.03
          ? "Con los datos de 2019 a 2024, el modelo no encuentra una diferencia importante entre escenarios para este cultivo en este municipio."
          : "Diferencias estimadas por el modelo a partir de lo ocurrido en los años con El Niño y La Niña entre 2019 y 2024."}
      </p>
    </div>
  );
}

function ShapPanel({ shap, ultimo }) {
  if (!Array.isArray(shap) || shap.length === 0) return null;
  const maxAbs = Math.max(...shap.map((s) => Math.abs(s.shap || 0)), 0.001);
  return (
    <div className="shap-panel">
      <div className="shap-title">Por qué este resultado</div>
      <p className="shap-sub">
        El modelo parte de los rendimientos recientes del municipio{ultimo ? ` (el último real: ${fmtNum(ultimo.valor)} t/ha en ${ultimo.anio})` : ""} y
        los ajusta. Estos factores fueron los que más movieron el resultado:
      </p>
      {shap.map((s, i) => {
        const val = Number(s.shap || 0);
        const pct = (Math.abs(val) / maxAbs) * 100;
        const up = val >= 0;
        return (
          <div key={i} className="shap-row">
            <div className="shap-meta">
              <span>{labelFeature(s.feature)}</span>
              <span className={up ? "pos" : "neg"}>{up ? "Sube" : "Baja"} {fmtNum(Math.abs(val), 2)} t/ha</span>
            </div>
            <div className="diverge-track">
              <span className={up ? "pos" : "neg"} style={up ? { left: "50%", width: `${pct / 2}%` } : { left: `${50 - pct / 2}%`, width: `${pct / 2}%` }} />
              <i />
            </div>
          </div>
        );
      })}
    </div>
  );
}

/* ── Qué hacer ───────────────────────────────────────────────────────── */
const REC_ICON = { calendario: Icon.calendar, suelo: Icon.layers, clima: Icon.cloudRain };

function Recomendacion({ muni, cultivo, semestre }) {
  const [data, setData] = useState(null);
  useEffect(() => {
    setData(null);
    fetch("/api/recomendacion", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id_municipio: muni, id_cultivo: cultivo, semestre }),
    }).then((r) => r.json()).then(setData).catch(() => setData({ error: true }));
  }, [muni, cultivo, semestre]);

  if (!data) return <div className="panel-empty">Preparando recomendaciones…</div>;
  if (!data.recomendaciones) return <div className="panel-empty">No fue posible generar recomendaciones en este momento.</div>;
  return (
    <>
      <div className="rec-grid three">
        {data.recomendaciones.map((r) => {
          const I = r.alerta ? Icon.alert : REC_ICON[r.tipo] || Icon.checkCircle;
          return (
            <div key={r.tipo} className={`rec-card ${r.alerta ? "warn" : ""}`}>
              <span className="icon-chip"><I size={18} /></span>
              <h4>{r.titulo}</h4>
              <p>{r.detalle}</p>
              <p className="rec-src">{r.fuente}</p>
            </div>
          );
        })}
      </div>
      <p className="panel-foot">Orientación general basada en datos públicos. Consulta siempre con un asistente técnico local antes de sembrar.</p>
    </>
  );
}

/* ── Comparar con la región ──────────────────────────────────────────── */
function Comparativo({ r }) {
  const [data, setData] = useState(null);
  useEffect(() => {
    setData(null);
    fetch(`/api/comparativo?muni=${r.municipio.id}&cultivo=${r.cultivo.id}&anio=${r.anio}`)
      .then((x) => x.json()).then(setData).catch(() => setData({ error: true }));
  }, [r.municipio.id, r.cultivo.id, r.anio]);

  if (!data) return <div className="panel-empty">Buscando municipios para comparar…</div>;
  if (data.error) return <div className="panel-empty">La comparación no está disponible en este momento.</div>;
  if (!data.filas || data.filas.length < 2) {
    return <div className="panel-empty">No hay otros municipios de {data.departamento || "este departamento"} con pronóstico de {r.cultivo.nombre.toLowerCase()}.</div>;
  }
  return (
    <>
      <p className="panel-lead">
        En {data.departamento}, <strong>{r.municipio.nombre}</strong> ocupa el puesto <strong>{data.posicion} de {data.total}</strong> municipios
        por rendimiento esperado de {r.cultivo.nombre.toLowerCase()} en {r.anio}.
      </p>
      <div className="table-scroll">
        <table className="data-table">
          <thead><tr><th>Municipio</th><th>Rendimiento esperado {r.anio}</th><th>Frente a su último año</th></tr></thead>
          <tbody>
            {data.filas.map((f) => (
              <tr key={f.id} className={f.actual ? "current" : ""}>
                <td><strong>{f.municipio}</strong>{f.actual && <span className="tag-current">Tu consulta</span>}</td>
                <td className="num">{fmtNum(f.rendimiento)} <span className="muted">t/ha</span></td>
                <td className={`num ${f.cambio_pct == null ? "" : f.cambio_pct >= 0 ? "pos" : "neg"}`}>
                  {f.cambio_pct == null ? "—" : `${signed(f.cambio_pct)} %`}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="panel-foot">Las diferencias entre municipios reflejan lo que cada uno ha reportado a la Encuesta Agropecuaria (EVA).</p>
    </>
  );
}

/* ── Clima en vivo (Open-Meteo) ──────────────────────────────────────── */
function ClimaActual({ id }) {
  const [data, setData] = useState(null);
  useEffect(() => {
    if (!id) return;
    setData(null);
    fetch(`/api/clima/actual?id=${id}`)
      .then((r) => r.json()).then(setData).catch(() => setData(null));
  }, [id]);
  if (!data || data.error || !data.actual) return null;
  const a = data.actual;
  const Sky = a.es_de_dia ? Icon.sun : Icon.moon;
  return (
    <div className="weather-now">
      <Sky size={20} />
      <div className="weather-place">
        <strong>Clima ahora</strong>
        <span>Open-Meteo · en vivo</span>
      </div>
      <div className="weather-vals">
        <span><Icon.thermo size={13} /> {a.temperatura_c}°C</span>
        <span><Icon.drop size={13} /> {a.humedad_pct}%</span>
        <span><Icon.cloudRain size={13} /> {a.precipitacion_mm} mm</span>
        <span><Icon.wind size={13} /> {a.viento_kmh} km/h</span>
      </div>
    </div>
  );
}

/* ── Siguientes pasos ────────────────────────────────────────────────── */
const FOLLOW_TABS = [
  { id: "hacer",    label: "Qué hacer",              icon: Icon.checkCircle },
  { id: "comparar", label: "Comparar con la región", icon: Icon.barChart },
  { id: "simular",  label: "¿Y si cambia el clima?", icon: Icon.sliders },
];

function FollowUp({ r, temporada }) {
  const [tab, setTab] = useState("hacer");
  return (
    <div className="card followup fade-in">
      <div className="followup-head">
        <h3>Siguientes pasos</h3>
        <div className="tabs" role="tablist">
          {FOLLOW_TABS.map((t) => (
            <button key={t.id} role="tab" aria-selected={tab === t.id}
              className={`tab ${tab === t.id ? "active" : ""}`} onClick={() => setTab(t.id)}>
              <t.icon size={15} /> {t.label}
            </button>
          ))}
        </div>
      </div>
      <div className="card-body">
        <div hidden={tab !== "hacer"}><Recomendacion muni={r.municipio.id} cultivo={r.cultivo.id} semestre={temporada?.semestre || "A"} /></div>
        <div hidden={tab !== "comparar"}><Comparativo r={r} /></div>
        <div hidden={tab !== "simular"}><GemeloDigital muni={r.municipio.nombre} cultivo={r.cultivo.nombre} baseline={r.yhat} /></div>
      </div>
    </div>
  );
}

/* ── Página ──────────────────────────────────────────────────────────── */
export default function PagePrediccion() {
  const [municipios, setMunicipios] = useState([]);
  const [depto, setDepto]           = useState("");
  const [muni, setMuni]             = useState("");
  const [cultivos, setCultivos]     = useState(null);
  const [cultivo, setCultivo]       = useState("");
  const [temporadas, setTemporadas] = useState([]);
  const [tempId, setTempId]         = useState("");
  const [result, setResult]         = useState(null);
  const [loading, setLoading]       = useState(false);
  const [errorCarga, setErrorCarga] = useState(false);
  const [tourActive, setTourActive] = useState(false);

  const tourRefs = {
    muni: useRef(null), cultivo: useRef(null), temporada: useRef(null),
    submit: useRef(null), result: useRef(null),
  };

  useEffect(() => {
    Promise.all([fetch("/api/municipios").then((r) => r.json()), fetch("/api/modelo").then((r) => r.json())])
      .then(([lista, modelo]) => {
        if (!Array.isArray(lista) || !modelo.anios) throw new Error("sin datos");
        setMunicipios(lista);
        const ibague = lista.find((m) => m.id === "73001") || lista[0];
        setDepto(ibague.departamento);
        setMuni(ibague.id);
        const t = temporadasDisponibles(modelo.anios.filter((a) => a.escenarios.length > 1).map((a) => a.anio));
        setTemporadas(t);
        setTempId(t[0]?.id || "");
      })
      .catch(() => setErrorCarga(true));
  }, []);

  const deptos = useMemo(() => [...new Set(municipios.map((m) => m.departamento))].sort((a, b) => a.localeCompare(b, "es")), [municipios]);
  const munisDepto = useMemo(() => municipios.filter((m) => m.departamento === depto), [municipios, depto]);
  const muniSel = municipios.find((m) => m.id === muni);
  const temporada = temporadas.find((t) => t.id === tempId);

  useEffect(() => {
    if (!muni) return;
    setCultivos(null);
    fetch(`/api/cultivos?muni=${muni}`).then((r) => r.json())
      .then((lista) => {
        const l = Array.isArray(lista) ? lista : [];
        setCultivos(l);
        setCultivo((prev) => (l.some((c) => String(c.id) === String(prev)) ? prev : String((l.find((c) => c.nombre === "Arroz") || l[0])?.id || "")));
      })
      .catch(() => setCultivos([]));
  }, [muni]);

  const onDepto = (d) => {
    setDepto(d);
    const primero = municipios.find((m) => m.departamento === d);
    if (primero) setMuni(primero.id);
  };

  const onSubmit = async (e) => {
    e.preventDefault();
    setLoading(true); setResult(null);
    try {
      const res = await fetch("/api/prediccion", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ id_municipio: muni, id_cultivo: cultivo, anio: temporada.anio }),
      });
      const data = await res.json();
      setResult(res.ok ? data : { error: true });
      if (window.innerWidth < 900) setTimeout(() => tourRefs.result.current?.scrollIntoView({ behavior: "smooth", block: "start" }), 50);
    } catch {
      setResult({ error: true });
    } finally {
      setLoading(false);
    }
  };

  const listo = muni && cultivo && temporada && !loading;

  return (
    <>
      {tourActive && <TourOverlay steps={TOUR_STEPS} refs={tourRefs} onClose={() => setTourActive(false)} />}
      <section className="section page-top">
        <div className="container">
          <SectionHead eyebrow="Predicción" title="¿Cuánto rendirá tu cultivo?">
            Elige dónde, qué y cuándo vas a sembrar. Te mostramos cuánto se espera cosechar, qué tan confiable es la
            estimación y qué puedes hacer al respecto. Todo sale de datos oficiales.
          </SectionHead>

          {errorCarga && (
            <div className="notice-bar"><Icon.alert size={15} /> El servicio de predicción no está disponible en este momento. Intenta de nuevo en unos minutos.</div>
          )}

          <div className="predict-grid">
            <form className="form-card" onSubmit={onSubmit}>
              <h3><span className="icon-chip sm"><Icon.filter size={15} /></span> Tu consulta</h3>

              <div className="form-row" ref={tourRefs.muni}>
                <div className="form-row cols2" style={{ marginBottom: 0 }}>
                  <div className="field">
                    <label htmlFor="f-depto">Departamento</label>
                    <select id="f-depto" value={depto} onChange={(e) => onDepto(e.target.value)} disabled={!deptos.length}>
                      {!deptos.length && <option>Cargando…</option>}
                      {deptos.map((d) => <option key={d}>{d}</option>)}
                    </select>
                  </div>
                  <div className="field">
                    <label htmlFor="f-muni">Municipio</label>
                    <select id="f-muni" value={muni} onChange={(e) => setMuni(e.target.value)} disabled={!munisDepto.length}>
                      {munisDepto.map((m) => <option key={m.id} value={m.id}>{m.nombre}</option>)}
                    </select>
                  </div>
                </div>
                <ClimaActual id={muni} />
              </div>

              <div className="form-row" ref={tourRefs.cultivo}>
                <div className="field">
                  <label htmlFor="f-cultivo">Cultivo</label>
                  <select id="f-cultivo" value={cultivo} onChange={(e) => setCultivo(e.target.value)} disabled={!cultivos?.length}>
                    {cultivos === null && <option>Cargando cultivos…</option>}
                    {cultivos?.length === 0 && <option>Sin cultivos con datos</option>}
                    {cultivos?.map((c) => <option key={c.id} value={c.id}>{c.nombre}</option>)}
                  </select>
                  {cultivos?.length > 0 && <p className="field-hint">{cultivos.length} cultivos con historia de producción en {muniSel?.nombre}.</p>}
                </div>
              </div>

              <div className="form-row" ref={tourRefs.temporada}>
                <div className="field">
                  <label htmlFor="f-temp">¿Cuándo vas a sembrar?</label>
                  <select id="f-temp" value={tempId} onChange={(e) => setTempId(e.target.value)} disabled={!temporadas.length}>
                    {temporadas.map((t) => <option key={t.id} value={t.id}>{t.label}</option>)}
                  </select>
                  <p className="field-hint">Solo aparecen temporadas cuya ventana de siembra sigue abierta.</p>
                </div>
              </div>

              <button ref={tourRefs.submit} type="submit" className="btn-block" disabled={!listo}>
                {loading ? "Consultando…" : <>Ver pronóstico <Icon.arrow className="arrow" /></>}
              </button>
            </form>

            <div className="result-card" ref={tourRefs.result} aria-live="polite">
              {!result && !loading && (
                <div className="result-empty">
                  <div className="lupa"><Icon.search size={30} strokeWidth={1.6} /></div>
                  <div className="head">Tu resultado aparecerá aquí</div>
                  <ul className="empty-list">
                    <li><Icon.check size={14} /> Toneladas por hectárea que se esperan cosechar</li>
                    <li><Icon.check size={14} /> Rango probable y qué tan estable ha sido el cultivo</li>
                    <li><Icon.check size={14} /> Los factores que más influyen y qué hacer</li>
                  </ul>
                  <button type="button" className="btn-tour-start" onClick={() => setTourActive(true)}>
                    <Icon.play /> Ver guía paso a paso
                  </button>
                </div>
              )}
              {loading && (
                <div className="result-empty">
                  <div className="lupa spin"><Icon.refresh size={28} /></div>
                  <div className="head">Consultando el pronóstico…</div>
                </div>
              )}
              {result?.error && !loading && (
                <div className="result-empty">
                  <div className="lupa"><Icon.alert size={28} /></div>
                  <div className="head">No pudimos obtener el pronóstico</div>
                  <div className="sub">El servicio no respondió. Inténtalo de nuevo en unos segundos.</div>
                </div>
              )}
              {result?.sin_datos && !loading && (
                <div className="result-empty">
                  <div className="lupa"><Icon.info size={28} /></div>
                  <div className="head">Sin pronóstico para esta combinación</div>
                  <div className="sub">{result.motivo} Prueba con otro cultivo o temporada.</div>
                </div>
              )}
              {result && !result.error && !result.sin_datos && !loading && <ResultPanel r={result} temporada={temporada} />}
            </div>
          </div>

          {result && !result.error && !result.sin_datos && !loading && <FollowUp r={result} temporada={temporada} />}
        </div>
      </section>
    </>
  );
}
