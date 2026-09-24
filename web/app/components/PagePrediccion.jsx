"use client";
import { useState, useEffect, useRef } from "react";
import ConfidenceBar from "./charts/ConfidenceBar";
import GemeloDigital from "./GemeloDigital";
import { Icon } from "./icons";
import { SectionHead, DemoNotice, RiskBadge } from "./ui";
import { fmtNum, signed, riesgoInfo, SEMESTRE } from "@/lib/format";
import { labelFeature } from "@/lib/labels";

/* ── Pasos del tour ─────────────────────────────────────────────────── */
const TOUR_STEPS = [
  { refKey: "muni",       placement: "right", title: "Municipio",
    desc: "Elige la zona donde se va a sembrar. Verás también el clima de hoy en ese municipio." },
  { refKey: "cultivo",    placement: "right", title: "Cultivo",
    desc: "Selecciona qué vas a sembrar: arroz, papa, maíz, café… Cada cultivo responde distinto al clima de cada región." },
  { refKey: "periodo",    placement: "right", title: "Año y semestre",
    desc: "Semestre A es de enero a junio y semestre B de julio a diciembre." },
  { refKey: "escenarios", placement: "right", title: "Escenario climático (opcional)",
    desc: "Si hay pronóstico de El Niño o La Niña, o esperas más o menos lluvia de lo normal, indícalo aquí. Si no sabes, deja los valores por defecto." },
  { refKey: "submit",     placement: "top",   title: "Consultar",
    desc: "El resultado aparece en segundos." },
  { refKey: "result",     placement: "left",  title: "Tu resultado",
    desc: "Cuántas toneladas por hectárea se esperan, el rango probable, el nivel de riesgo y los factores que más influyen." },
];

/* ── Overlay del tour ────────────────────────────────────────────────── */
function TourOverlay({ steps, refs, onClose }) {
  const [step, setStep] = useState(0);
  const [rect, setRect] = useState(null);
  const current = steps[step];

  useEffect(() => {
    const el = refs[current.refKey]?.current;
    if (!el) return;
    el.scrollIntoView({ block: "center", behavior: "smooth" });
    const update = () => setRect(el.getBoundingClientRect());
    update();
    window.addEventListener("resize", update);
    window.addEventListener("scroll", update, true);
    return () => {
      window.removeEventListener("resize", update);
      window.removeEventListener("scroll", update, true);
    };
  }, [step, current.refKey, refs]);

  useEffect(() => {
    const onKey = (e) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const PAD = 10, GAP = 22;
  const narrow = typeof window !== "undefined" && window.innerWidth < 900;

  const spotlightStyle = rect && {
    top: rect.top - PAD, left: rect.left - PAD,
    width: rect.width + PAD * 2, height: rect.height + PAD * 2,
  };

  let tStyle = {};
  if (rect) {
    const placement = narrow ? (rect.top > window.innerHeight / 2 ? "top" : "bottom") : current.placement;
    if (placement === "right") {
      tStyle = { top: rect.top + rect.height / 2, left: rect.right + PAD + GAP, transform: "translateY(-50%)" };
    } else if (placement === "left") {
      tStyle = { top: rect.top + rect.height / 2, right: window.innerWidth - rect.left + PAD + GAP, transform: "translateY(-50%)" };
    } else if (placement === "top") {
      tStyle = { bottom: window.innerHeight - rect.top + PAD + GAP, left: Math.max(16, Math.min(rect.left, window.innerWidth - 312)) };
    } else {
      tStyle = { top: rect.bottom + PAD + GAP, left: Math.max(16, Math.min(rect.left, window.innerWidth - 312)) };
    }
  }

  const isLast = step === steps.length - 1;

  return (
    <>
      <div className="tour-backdrop" onClick={onClose} />
      {spotlightStyle && <div className="tour-spotlight" style={spotlightStyle} />}
      {rect && (
        <div className="tour-tooltip" style={tStyle} role="dialog" aria-label={current.title}>
          <div className="tour-dots">
            {steps.map((_, i) => (
              <button
                key={i}
                aria-label={`Paso ${i + 1}`}
                className={`tour-dot ${i === step ? "active" : i < step ? "done" : ""}`}
                onClick={(e) => { e.stopPropagation(); setStep(i); }}
              />
            ))}
          </div>
          <div className="tour-tt-title">{current.title}</div>
          <div className="tour-tt-desc">{current.desc}</div>
          <div className="tour-tt-actions">
            <button className="tour-skip" onClick={onClose}>Saltar</button>
            <div style={{ display: "flex", gap: 8 }}>
              {step > 0 && (
                <button className="tour-prev" onClick={(e) => { e.stopPropagation(); setStep((s) => s - 1); }}>Atrás</button>
              )}
              <button className="tour-next" onClick={(e) => { e.stopPropagation(); isLast ? onClose() : setStep((s) => s + 1); }}>
                {isLast ? "Entendido" : "Siguiente"}
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}

/* ── Panel de resultado ──────────────────────────────────────────────── */
function ResultPanel({ r }) {
  const risk     = riesgoInfo(r.risk);
  const muniName = r.muni.split(",")[0];
  const diff     = r.hist != null ? r.yhat - r.hist : null;
  const otroAnio = r.anio_dato && String(r.anio_dato) !== String(r.year);

  return (
    <div className="result-filled fade-in">
      <div className="result-head">
        <div>
          <h4>{r.cultivo}</h4>
          <div className="sub">{r.muni} · {r.year}, semestre {r.semester} ({SEMESTRE[r.semester]})</div>
        </div>
        <RiskBadge nivel={r.risk} label={`Riesgo ${risk.label.toLowerCase()}`} />
      </div>

      <div className="result-main">
        <div>
          <div className="result-num">{fmtNum(r.yhat)}<small>t/ha</small></div>
          <div className="result-lbl">Rendimiento esperado</div>
        </div>
        <div className="ci-block">
          <div className="ci-title">Rango probable (95 %)</div>
          <ConfidenceBar low={r.low} mid={r.yhat} high={r.high} vmin={Math.max(0, r.low - 0.5)} vmax={r.high + 0.5} />
        </div>
      </div>

      <p className="result-summary">
        Se espera cosechar unas <strong>{fmtNum(r.yhat)} toneladas por hectárea</strong> sembrada; lo más probable es que
        el resultado quede entre {fmtNum(r.low)} y {fmtNum(r.high)}.
        {diff != null && (
          <> Eso es <strong className={diff >= 0 ? "pos" : "neg"}>{signed(diff, 2)} t/ha</strong> frente al promedio
          histórico de {muniName} ({fmtNum(r.hist, 2)}).</>
        )}
      </p>

      <div className="metrics-3">
        <div className={`metric-mini risk-${risk.cls}`}><div className="v">{risk.label}</div><div className="l">{risk.texto}</div></div>
        <div className="metric-mini"><div className="v">{r.confidence}%</div><div className="l">Confianza del modelo</div></div>
        <div className="metric-mini"><div className="v">{r.hist != null ? fmtNum(r.hist, 2) : "—"}</div><div className="l">Promedio histórico (t/ha)</div></div>
      </div>

      {(otroAnio || !r.fromDB) && (
        <div className="result-notes">
          {otroAnio && (
            <span><Icon.info size={13} /> Aún no hay predicción para {r.year}; se muestra la más reciente disponible ({r.anio_dato}).</span>
          )}
          <DemoNotice show={!r.fromDB} />
        </div>
      )}

      <ShapPanel shap={r.shap} />
    </div>
  );
}

/* ── Por qué este resultado (SHAP) ───────────────────────────────────── */
function ShapPanel({ shap }) {
  if (!Array.isArray(shap) || shap.length === 0) return null;
  const maxAbs = Math.max(...shap.map((s) => Math.abs(s.shap || 0)), 0.001);

  return (
    <div className="shap-panel">
      <div className="shap-title">Por qué este resultado</div>
      <p className="shap-sub">Los factores que más movieron la predicción para este caso:</p>
      {shap.map((s, i) => {
        const v = Number(s.shap || 0);
        const pct = (Math.abs(v) / maxAbs) * 100;
        const up = v >= 0;
        return (
          <div key={i} className="shap-row">
            <div className="shap-meta">
              <span>{labelFeature(s.feature)}</span>
              <span className={up ? "pos" : "neg"}>{up ? "Sube" : "Baja"} {fmtNum(Math.abs(v), 2)} t/ha</span>
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

/* ── Qué hacer (recomendación) ───────────────────────────────────────── */
const REC_ICONS = [Icon.calendar, Icon.sprout, null, Icon.drop];

function Recomendacion({ muni, cultivo, enso, lluvia }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    fetch("/api/recomendacion", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ muni, cultivo, enso, lluvia }),
    })
      .then((r) => r.json()).then(setData).catch(() => setData(null))
      .finally(() => setLoading(false));
  }, [muni, cultivo, enso, lluvia]);

  if (loading) return <div className="panel-empty">Preparando recomendaciones…</div>;
  if (!data?.recomendaciones) return <div className="panel-empty">No fue posible generar recomendaciones para esta consulta.</div>;

  return (
    <>
      <div className="rec-grid">
        {data.recomendaciones.map((r, i) => {
          const alerta = r.icono === "⚠️";
          const I = REC_ICONS[i] || (alerta ? Icon.alert : Icon.checkCircle);
          return (
            <div key={i} className={`rec-card ${alerta ? "warn" : ""}`}>
              <span className="icon-chip"><I size={18} /></span>
              <h4>{r.titulo}</h4>
              <p>{r.detalle}</p>
              <p className="rec-tip">{r.ajuste}</p>
            </div>
          );
        })}
      </div>
      <p className="panel-foot">
        Recomendaciones orientativas según el calendario típico del cultivo, la aptitud del suelo (UPRA) y el escenario El Niño / La Niña.
        {data.aptitud_sipra && <> Aptitud del suelo en la zona: <strong>{data.aptitud_sipra}</strong>.</>}
        {" "}Consulta siempre con un asistente técnico local.
      </p>
    </>
  );
}

/* ── Comparar con la región ──────────────────────────────────────────── */
function Comparativo({ muni, cultivo }) {
  const [data, setData] = useState(null);

  useEffect(() => {
    setData(null);
    fetch(`/api/comparativo?muni=${encodeURIComponent(muni)}&cultivo=${encodeURIComponent(cultivo)}`)
      .then((r) => r.json()).then(setData).catch(() => setData({ filas: [] }));
  }, [muni, cultivo]);

  if (!data) return <div className="panel-empty">Buscando municipios para comparar…</div>;
  if (data.fromDB === false) {
    return <div className="panel-empty">La comparación regional no está disponible en este momento porque la base de datos no responde.</div>;
  }
  if (!data.filas || data.filas.length < 2) {
    return (
      <div className="panel-empty">
        No hay suficientes predicciones de {cultivo.toLowerCase()} en otros municipios
        {data.departamento ? ` de ${data.departamento}` : ""} para hacer una comparación.
      </div>
    );
  }

  return (
    <>
      {data.posicion && (
        <p className="panel-lead">
          En {data.departamento}, <strong>{muni.split(",")[0]}</strong> ocupa el puesto <strong>{data.posicion} de {data.total}</strong> municipios
          por rendimiento esperado de {cultivo.toLowerCase()}.
        </p>
      )}
      <div className="table-scroll">
        <table className="data-table">
          <thead>
            <tr><th>Municipio</th><th>Rendimiento esperado</th><th>Riesgo climático</th><th>Frente a su historia</th></tr>
          </thead>
          <tbody>
            {data.filas.map((f) => (
              <tr key={f.municipio} className={f.actual ? "current" : ""}>
                <td><strong>{f.municipio}</strong>{f.actual && <span className="tag-current">Tu consulta</span>}</td>
                <td className="num">{fmtNum(f.rendimiento)} <span className="muted">t/ha</span></td>
                <td>{f.riesgo ? <RiskBadge nivel={f.riesgo} label={riesgoInfo(f.riesgo).label} /> : <span className="muted">Sin alertas</span>}</td>
                <td className={`num ${f.vs_hist_pct == null ? "" : f.vs_hist_pct >= 0 ? "pos" : "neg"}`}>
                  {f.vs_hist_pct == null ? "—" : `${signed(f.vs_hist_pct)} %`}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

/* ── Clima en vivo (Open-Meteo) ──────────────────────────────────────── */
function ClimaActual({ muni }) {
  const [data, setData] = useState(null);
  useEffect(() => {
    if (!muni) return;
    setData(null);
    const nombre = muni.split(",")[0].trim();
    fetch(`/api/clima/actual?municipio=${encodeURIComponent(nombre)}`)
      .then((r) => r.json()).then(setData).catch(() => setData(null));
  }, [muni]);

  if (!data || data.error || !data.actual) return null;
  const a = data.actual;
  const Sky = a.es_de_dia ? Icon.sun : Icon.moon;
  return (
    <div className="weather-now">
      <Sky size={20} />
      <div className="weather-place">
        <strong>Clima ahora en {data.municipio}</strong>
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

/* ── Pestañas posteriores al resultado ───────────────────────────────── */
const FOLLOW_TABS = [
  { id: "hacer",    label: "Qué hacer",               icon: Icon.checkCircle },
  { id: "comparar", label: "Comparar con la región",  icon: Icon.barChart },
  { id: "simular",  label: "¿Y si cambia el clima?",  icon: Icon.sliders },
];

function FollowUp({ r }) {
  const [tab, setTab] = useState("hacer");
  return (
    <div className="card followup fade-in">
      <div className="followup-head">
        <h3>Siguientes pasos</h3>
        <div className="tabs" role="tablist">
          {FOLLOW_TABS.map((t) => (
            <button
              key={t.id}
              role="tab"
              aria-selected={tab === t.id}
              className={`tab ${tab === t.id ? "active" : ""}`}
              onClick={() => setTab(t.id)}
            >
              <t.icon size={15} /> {t.label}
            </button>
          ))}
        </div>
      </div>
      <div className="card-body">
        <div hidden={tab !== "hacer"}>
          <Recomendacion muni={r.muni} cultivo={r.cultivo} enso={r.escenario_enso} lluvia={r.escenario_lluvia} />
        </div>
        <div hidden={tab !== "comparar"}>
          <Comparativo muni={r.muni} cultivo={r.cultivo} />
        </div>
        <div hidden={tab !== "simular"}>
          <GemeloDigital muni={r.muni} cultivo={r.cultivo} baseline={r.yhat} />
        </div>
      </div>
    </div>
  );
}

/* ── Página ──────────────────────────────────────────────────────────── */
export default function PagePrediccion() {
  const [municipios, setMunicipios] = useState([]);
  const [cultivos,   setCultivos]   = useState([]);
  const [muni,       setMuni]       = useState("");
  const [cultivo,    setCultivo]    = useState("");
  const [year,       setYear]       = useState("2026");
  const [semester,   setSemester]   = useState("A");
  const [enso,       setEnso]       = useState("Neutral");
  const [lluvia,     setLluvia]     = useState("Normal");
  const [result,     setResult]     = useState(null);
  const [loading,    setLoading]    = useState(false);
  const [tourActive, setTourActive] = useState(false);

  const tourRefs = {
    muni: useRef(null), cultivo: useRef(null), periodo: useRef(null),
    escenarios: useRef(null), submit: useRef(null), result: useRef(null),
  };

  useEffect(() => {
    const load = (url, fallback, setList, setSel) =>
      fetch(url).then((r) => r.json())
        .then((d) => { const l = Array.isArray(d) && d.length ? d : fallback; setList(l); setSel(l[0]); })
        .catch(() => { setList(fallback); setSel(fallback[0]); });
    load("/api/municipios", ["Ibagué, Tolima", "Espinal, Tolima", "Villavicencio, Meta", "Pasto, Nariño", "Manizales, Caldas", "Montería, Córdoba"], setMunicipios, setMuni);
    load("/api/cultivos", ["Maíz tecnificado", "Arroz riego", "Café arábica", "Caña panelera", "Plátano", "Papa Diacol"], setCultivos, setCultivo);
  }, []);

  const onSubmit = async (e) => {
    e.preventDefault();
    setLoading(true); setResult(null);
    try {
      const res  = await fetch("/api/prediccion", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ muni, cultivo, year, semester, enso, lluvia }),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setResult({ ...data, escenario_enso: enso, escenario_lluvia: lluvia });
      if (window.innerWidth < 900) {
        setTimeout(() => tourRefs.result.current?.scrollIntoView({ behavior: "smooth", block: "start" }), 50);
      }
    } catch {
      setResult({ error: true });
    } finally {
      setLoading(false);
    }
  };

  return (
    <>
      {tourActive && <TourOverlay steps={TOUR_STEPS} refs={tourRefs} onClose={() => setTourActive(false)} />}

      <section className="section page-top">
        <div className="container">
          <SectionHead eyebrow="Predicción" title="¿Cuánto rendirá tu cultivo?">
            Elige un municipio, un cultivo y un período. Te mostramos cuánto se espera cosechar, qué tan
            riesgosa es la temporada y qué puedes hacer al respecto.
          </SectionHead>

          <div className="predict-grid">
            <form className="form-card" onSubmit={onSubmit}>
              <h3><span className="icon-chip sm"><Icon.filter size={15} /></span> Tu consulta</h3>

              <div className="form-row" ref={tourRefs.muni}>
                <div className="field">
                  <label htmlFor="f-muni">Municipio</label>
                  <select id="f-muni" value={muni} onChange={(e) => setMuni(e.target.value)}>
                    {municipios.map((m) => <option key={m}>{m}</option>)}
                  </select>
                  <ClimaActual muni={muni} />
                </div>
              </div>

              <div className="form-row" ref={tourRefs.cultivo}>
                <div className="field">
                  <label htmlFor="f-cultivo">Cultivo</label>
                  <select id="f-cultivo" value={cultivo} onChange={(e) => setCultivo(e.target.value)}>
                    {cultivos.map((c) => <option key={c}>{c}</option>)}
                  </select>
                </div>
              </div>

              <div className="form-row cols2" ref={tourRefs.periodo}>
                <div className="field">
                  <label htmlFor="f-year">Año</label>
                  <select id="f-year" value={year} onChange={(e) => setYear(e.target.value)}>
                    <option>2026</option><option>2027</option><option>2028</option>
                  </select>
                </div>
                <div className="field">
                  <label htmlFor="f-sem">Semestre</label>
                  <select id="f-sem" value={semester} onChange={(e) => setSemester(e.target.value)}>
                    <option value="A">A · enero – junio</option>
                    <option value="B">B · julio – diciembre</option>
                  </select>
                </div>
              </div>

              <div className="adv" ref={tourRefs.escenarios}>
                <div className="adv-title">Escenario climático <span>opcional</span></div>
                <div className="form-row cols2" style={{ marginBottom: 0 }}>
                  <div className="field">
                    <label htmlFor="f-enso">El Niño / La Niña</label>
                    <select id="f-enso" value={enso} onChange={(e) => setEnso(e.target.value)}>
                      <option value="Neutral">Año normal</option>
                      <option value="El Niño">El Niño (más seco)</option>
                      <option value="La Niña">La Niña (más lluvioso)</option>
                    </select>
                  </div>
                  <div className="field">
                    <label htmlFor="f-lluvia">Lluvia esperada</label>
                    <select id="f-lluvia" value={lluvia} onChange={(e) => setLluvia(e.target.value)}>
                      <option value="Normal">Normal</option>
                      <option value="Déficit">Menos de lo normal</option>
                      <option value="Exceso">Más de lo normal</option>
                    </select>
                  </div>
                </div>
              </div>

              <button ref={tourRefs.submit} type="submit" className="btn-block" disabled={loading || !muni || !cultivo}>
                {loading ? "Consultando…" : <>Ver predicción <Icon.arrow className="arrow" /></>}
              </button>
            </form>

            <div className="result-card" ref={tourRefs.result} aria-live="polite">
              {!result && !loading && (
                <div className="result-empty">
                  <div className="lupa"><Icon.search size={30} strokeWidth={1.6} /></div>
                  <div className="head">Tu resultado aparecerá aquí</div>
                  <ul className="empty-list">
                    <li><Icon.check size={14} /> Toneladas por hectárea que se esperan cosechar</li>
                    <li><Icon.check size={14} /> Rango probable y nivel de riesgo climático</li>
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
                  <div className="head">Consultando el modelo…</div>
                  <div className="sub">Buscamos la predicción para este municipio y cultivo.</div>
                </div>
              )}
              {result?.error && !loading && (
                <div className="result-empty">
                  <div className="lupa"><Icon.alert size={28} /></div>
                  <div className="head">No pudimos obtener la predicción</div>
                  <div className="sub">Revisa tu conexión e inténtalo de nuevo en unos segundos.</div>
                </div>
              )}
              {result && !result.error && !loading && <ResultPanel r={result} />}
            </div>
          </div>

          {result && !result.error && !loading && <FollowUp r={result} />}
        </div>
      </section>
    </>
  );
}
