"use client";
import useAncho from "./useAncho";

/* Marcas "redondas" del eje Y (1, 2, 2,5 o 5 × 10^n). */
function escala(min, max, n = 4) {
  const bruto = (max - min) / n || 1;
  const mag = 10 ** Math.floor(Math.log10(bruto));
  const paso = [1, 2, 2.5, 5, 10].map((k) => k * mag).find((k) => k >= bruto);
  const ini = Math.floor(min / paso) * paso;
  const out = [];
  for (let v = ini; v <= max + paso * 0.999; v += paso) out.push(+v.toFixed(6));
  return out;
}

/* Serie anual de rendimiento: real (línea continua), lo que el modelo habría
   predicho sin conocer ese año (puntos huecos) y pronóstico con su rango (banda).
   rows: [{ anio, real?, backtest?, pronostico?, low?, high? }] */
export default function SerieChart({ rows = [], height = 260, unidad = "t/ha" }) {
  const [ref, ancho] = useAncho();
  const data = rows.filter((r) => r.real != null || r.backtest != null || r.pronostico != null);
  if (data.length < 2) return <div className="chart-empty">No hay suficientes años para graficar.</div>;

  const w = ancho, h = height, pad = { l: 44, r: 16, t: 30, b: 30 };
  const vals = data.flatMap((d) => [d.real, d.backtest, d.pronostico, d.low, d.high]).filter((v) => v != null);
  const ticks = escala(Math.max(0, Math.min(...vals) * 0.9), Math.max(...vals) * 1.05);
  const minY = ticks[0], maxY = ticks[ticks.length - 1];
  const x = (i) => pad.l + (i * (w - pad.l - pad.r)) / (data.length - 1);
  const y = (v) => pad.t + (1 - (v - minY) / (maxY - minY || 1)) * (h - pad.t - pad.b);
  const linea = (key) => {
    let d = "", pen = false;
    data.forEach((r, i) => {
      if (r[key] == null) { pen = false; return; }
      d += `${pen ? "L" : "M"}${x(i).toFixed(1)} ${y(r[key]).toFixed(1)} `;
      pen = true;
    });
    return d.trim();
  };

  const idxF = data.map((r, i) => (r.pronostico != null ? i : -1)).filter((i) => i >= 0);
  const lastReal = data.reduce((a, r, i) => (r.real != null ? i : a), -1);
  const banda = idxF.length
    ? idxF.map((i) => `${x(i)},${y(data[i].high)}`).join(" ") + " " +
      [...idxF].reverse().map((i) => `${x(i)},${y(data[i].low)}`).join(" ")
    : null;
  const unir = lastReal >= 0 && idxF.length ? `M${x(lastReal)} ${y(data[lastReal].real)} L${x(idxF[0])} ${y(data[idxF[0]].pronostico)}` : null;
  const fmt = (v) => v.toLocaleString("es-CO", { maximumFractionDigits: v < 10 ? 1 : 0 });

  return (
    <div className="serie-chart" ref={ref}>
      <svg viewBox={`0 0 ${w} ${h}`} width={w} height={h} role="img" aria-label="Rendimiento real, validación del modelo y pronóstico por año">
        {ticks.map((v) => (
          <g key={v}>
            <line x1={pad.l} x2={w - pad.r} y1={y(v)} y2={y(v)} stroke="#e5e7eb" strokeDasharray="2 4" />
            <text x={pad.l - 8} y={y(v) + 3} fontSize="10.5" fill="#6b7280" textAnchor="end">{fmt(v)}</text>
          </g>
        ))}
        {data.map((r, i) => (w > 420 || i % 2 === 0 || i === data.length - 1) && (
          <text key={r.anio} x={x(i)} y={h - 9} fontSize="10.5" fill={r.pronostico != null ? "#1e4d7b" : "#6b7280"} textAnchor="middle" fontWeight={r.pronostico != null ? 600 : 400}>{r.anio}</text>
        ))}
        {banda && <polygon points={banda} fill="#1e4d7b" opacity="0.08" stroke="#1e4d7b" strokeOpacity="0.25" strokeDasharray="3 3" />}
        {unir && <path d={unir} stroke="#1e4d7b" strokeWidth="2" strokeDasharray="5 4" fill="none" />}
        <path d={linea("pronostico")} stroke="#1e4d7b" strokeWidth="2.2" strokeDasharray="5 4" fill="none" />
        <path d={linea("real")} stroke="#1a7a4a" strokeWidth="2.5" fill="none" strokeLinejoin="round" />
        {data.map((r, i) => (
          <g key={i}>
            {r.real != null && <circle cx={x(i)} cy={y(r.real)} r="3.6" fill="#1a7a4a" stroke="white" strokeWidth="1.2"><title>{`${r.anio}: real ${fmt(r.real)} ${unidad}`}</title></circle>}
            {r.backtest != null && <circle cx={x(i)} cy={y(r.backtest)} r="4.2" fill="white" stroke="#d97706" strokeWidth="2"><title>{`${r.anio}: el modelo habría predicho ${fmt(r.backtest)} ${unidad}`}</title></circle>}
            {r.pronostico != null && <circle cx={x(i)} cy={y(r.pronostico)} r="4" fill="#1e4d7b" stroke="white" strokeWidth="1.2"><title>{`${r.anio}: pronóstico ${fmt(r.pronostico)} ${unidad} (entre ${fmt(r.low)} y ${fmt(r.high)})`}</title></circle>}
          </g>
        ))}
      </svg>
      <div className="chart-legend">
        <span><i className="lg-real" /> Real</span>
        {data.some((r) => r.backtest != null) && <span><i className="lg-back" /> Lo que el modelo habría predicho</span>}
        {idxF.length > 0 && <span><i className="lg-pron" /> Pronóstico y rango probable</span>}
      </div>
    </div>
  );
}
