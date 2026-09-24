"use client";

/* Rendimiento real (línea continua) vs. predicho (discontinua) por año.
   `data`: [{ anio, real|null, predicho|null }]. Los años sin valor se saltan
   sin desalinear el eje X. */
export default function DualLineChart({ height = 280, data = [] }) {
  const rows = (Array.isArray(data) ? data : []).filter((d) => d.real != null || d.predicho != null);
  if (rows.length < 2) return <div className="chart-empty">Sin serie suficiente para graficar.</div>;

  const w = 620, h = height, pad = { l: 40, r: 20, t: 34, b: 30 };
  const vals = rows.flatMap((d) => [d.real, d.predicho]).filter((v) => v != null);
  const minY = Math.floor((Math.min(...vals) - 0.2) * 2) / 2;
  const maxY = Math.ceil((Math.max(...vals) + 0.2) * 2) / 2;
  const x = (i) => pad.l + (i * (w - pad.l - pad.r)) / (rows.length - 1);
  const y = (v) => pad.t + (1 - (v - minY) / (maxY - minY)) * (h - pad.t - pad.b);

  const path = (key) => {
    let d = "", pen = false;
    rows.forEach((r, i) => {
      if (r[key] == null) { pen = false; return; }
      d += `${pen ? "L" : "M"}${x(i).toFixed(1)} ${y(r[key]).toFixed(1)} `;
      pen = true;
    });
    return d.trim();
  };

  const lastReal = rows.reduce((acc, r, i) => (r.real != null ? i : acc), -1);
  const forecastX = lastReal >= 0 && lastReal < rows.length - 1 ? x(lastReal) : null;
  const step = (maxY - minY) / 4;
  const ticks = [0, 1, 2, 3, 4].map((k) => minY + k * step);
  const labelEvery = Math.ceil(rows.length / 6);

  return (
    <svg viewBox={`0 0 ${w} ${h}`} width="100%" height={h} style={{ display: "block" }} role="img"
      aria-label="Rendimiento real frente a predicho por año">
      {forecastX != null && (
        <>
          <rect x={forecastX} y={pad.t} width={w - pad.r - forecastX} height={h - pad.t - pad.b} fill="#1e4d7b" opacity="0.06" />
          <text x={forecastX + 6} y={pad.t + 12} fontSize="10" fill="#1e4d7b" fontFamily="ui-monospace, monospace">PRONÓSTICO</text>
        </>
      )}
      {ticks.map((v) => (
        <g key={v}>
          <line x1={pad.l} x2={w - pad.r} y1={y(v)} y2={y(v)} stroke="#e5e7eb" strokeDasharray="2 4" />
          <text x={pad.l - 8} y={y(v) + 3} fontSize="10" fill="#6b7280" textAnchor="end" fontFamily="ui-monospace, monospace">{v.toFixed(1)}</text>
        </g>
      ))}
      {rows.map((r, i) => (i % labelEvery === 0 || i === rows.length - 1) && (
        <text key={r.anio} x={x(i)} y={h - 10} fontSize="10" fill="#6b7280" textAnchor="middle" fontFamily="ui-monospace, monospace">{r.anio}</text>
      ))}
      <path d={path("predicho")} fill="none" stroke="#1e4d7b" strokeWidth="2" strokeDasharray="5 4" strokeLinejoin="round" />
      <path d={path("real")} fill="none" stroke="#1a7a4a" strokeWidth="2.5" strokeLinejoin="round" />
      {rows.map((r, i) => r.real != null && (
        <circle key={i} cx={x(i)} cy={y(r.real)} r="3" fill="#1a7a4a" stroke="white" strokeWidth="1">
          <title>{`${r.anio}: real ${r.real} t/ha${r.predicho != null ? ` · predicho ${r.predicho}` : ""}`}</title>
        </circle>
      ))}
      <g transform={`translate(${pad.l}, 14)`}>
        <line x1="0" x2="16" y1="0" y2="0" stroke="#1a7a4a" strokeWidth="2.5" />
        <text x="22" y="4" fontSize="11" fill="#374151">Real</text>
        <line x1="66" x2="82" y1="0" y2="0" stroke="#1e4d7b" strokeWidth="2" strokeDasharray="5 4" />
        <text x="88" y="4" fontSize="11" fill="#374151">Predicho</text>
      </g>
    </svg>
  );
}
