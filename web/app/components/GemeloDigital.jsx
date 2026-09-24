"use client";
import { useEffect, useState } from "react";
import { Icon } from "./icons";
import { fmtNum, signed } from "@/lib/format";

const DEBOUNCE = 250;

const FACTOR_LABEL = {
  "Lluvia (Δ%)":        "Lluvia",
  "Temperatura (Δ°C)":  "Temperatura",
  "ENSO":               "El Niño / La Niña",
  "Fertilización (Δ%)": "Fertilización",
  "Aptitud SIPRA":      "Aptitud del suelo",
};

function Slider({ id, icon: I, label, value, display, tone, ...props }) {
  return (
    <div className="sim-control">
      <label htmlFor={id}>
        <I size={14} /> {label} <span className={`sim-val ${tone}`}>{display}</span>
      </label>
      <input id={id} type="range" value={value} {...props} />
    </div>
  );
}

export default function GemeloDigital({ muni, cultivo, baseline = null }) {
  const [lluvia, setLluvia] = useState(0);
  const [temp,   setTemp]   = useState(0);
  const [enso,   setEnso]   = useState("Neutral");
  const [fert,   setFert]   = useState(0);
  const [data,   setData]   = useState(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!muni || !cultivo) return;
    setLoading(true);
    const t = setTimeout(() => {
      fetch("/api/simular", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ muni, cultivo, baseline, lluvia_pct: lluvia, temp_delta_c: temp, enso, fertilizante_pct: fert }),
      })
        .then((r) => r.json()).then(setData).catch(() => setData(null))
        .finally(() => setLoading(false));
    }, DEBOUNCE);
    return () => clearTimeout(t);
  }, [muni, cultivo, baseline, lluvia, temp, enso, fert]);

  const reset = () => { setLluvia(0); setTemp(0); setEnso("Neutral"); setFert(0); };
  const toneOf = (v, invert = false) => (v === 0 ? "" : (v > 0) !== invert ? "pos" : "neg");

  return (
    <div className="sim">
      <p className="panel-lead">
        Mueve los controles para ver cómo cambiaría la cosecha frente a tu predicción. Aplicamos efectos agronómicos
        típicos sobre ese resultado; es una aproximación orientativa, no una nueva predicción del modelo.
      </p>

      <div className="sim-controls">
        <Slider id="s-lluvia" icon={Icon.cloudRain} label="Lluvia" value={lluvia}
          display={`${lluvia > 0 ? "+" : ""}${lluvia} %`} tone={toneOf(lluvia)}
          min={-30} max={30} step={1} onChange={(e) => setLluvia(parseFloat(e.target.value))} />
        <Slider id="s-temp" icon={Icon.thermo} label="Temperatura" value={temp}
          display={`${temp > 0 ? "+" : ""}${temp} °C`} tone={toneOf(temp, true)}
          min={-3} max={3} step={0.5} onChange={(e) => setTemp(parseFloat(e.target.value))} />
        <div className="sim-control">
          <label htmlFor="s-enso"><Icon.sun size={14} /> El Niño / La Niña</label>
          <select id="s-enso" value={enso} onChange={(e) => setEnso(e.target.value)}>
            <option value="Neutral">Año normal</option>
            <option value="El Niño">El Niño</option>
            <option value="La Niña">La Niña</option>
          </select>
        </div>
        <Slider id="s-fert" icon={Icon.sprout} label="Fertilización" value={fert}
          display={`+${fert} %`} tone={toneOf(fert)}
          min={0} max={50} step={5} onChange={(e) => setFert(parseFloat(e.target.value))} />
      </div>

      {data && (
        <div className="sim-result">
          <div className="sim-numbers">
            <div>
              <div className="sim-lbl">Tu predicción</div>
              <div className="sim-big">{fmtNum(data.baseline, 2)} <small>t/ha</small></div>
            </div>
            <Icon.arrow size={20} className="sim-arrow" />
            <div>
              <div className="sim-lbl">Con este escenario</div>
              <div className={`sim-big ${data.delta_t_ha >= 0 ? "pos" : "neg"}`}>{fmtNum(data.proyectado, 2)} <small>t/ha</small></div>
            </div>
            <div className="sim-delta">
              <div className="sim-lbl">Cambio</div>
              <div className={data.delta_t_ha >= 0 ? "pos" : "neg"}>
                {signed(data.delta_t_ha, 2)} t/ha · {signed(data.delta_pct)} %
              </div>
            </div>
          </div>

          <div className="sim-factors">
            {data.contribuciones.map((c) => {
              const max = Math.max(...data.contribuciones.map((x) => Math.abs(x.valor)), 0.001);
              const pct = (Math.abs(c.valor) / max) * 100;
              const up = c.valor >= 0;
              return (
                <div key={c.factor} className="shap-row">
                  <div className="shap-meta">
                    <span>{FACTOR_LABEL[c.factor] || c.factor}</span>
                    <span className={c.valor === 0 ? "muted" : up ? "pos" : "neg"}>{signed(c.valor, 2)} t/ha</span>
                  </div>
                  <div className="diverge-track">
                    {c.valor !== 0 && (
                      <span className={up ? "pos" : "neg"} style={up ? { left: "50%", width: `${pct / 2}%` } : { left: `${50 - pct / 2}%`, width: `${pct / 2}%` }} />
                    )}
                    <i />
                  </div>
                </div>
              );
            })}
          </div>

          <div className="sim-foot">
            <span>{loading ? "Recalculando…" : data.interpretacion}</span>
            <button type="button" className="btn-ghost" onClick={reset}><Icon.refresh /> Restablecer</button>
          </div>
        </div>
      )}
    </div>
  );
}
