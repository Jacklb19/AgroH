"use client";

const fmt = (v) => Number(v).toLocaleString("es-CO", { maximumFractionDigits: v < 10 ? 1 : 0 });

/* Barra de rango probable: extremos (low/high) y valor central (mid). */
export default function ConfidenceBar({ low, mid, high, vmin, vmax }) {
  const w = 360, h = 64, pad = 18;
  const lo = vmin ?? low * 0.8, hi = vmax ?? high * 1.08;
  const scale = (v) => pad + ((v - lo) / (hi - lo || 1)) * (w - pad * 2);
  const xLow = scale(low), xMid = scale(mid), xHigh = scale(high);
  return (
    <svg viewBox={`0 0 ${w} ${h}`} width="100%" height={h} style={{ display: "block" }}
      role="img" aria-label={`Rango probable entre ${fmt(low)} y ${fmt(high)}, valor esperado ${fmt(mid)}`}>
      <line x1={pad} x2={w - pad} y1={h / 2} y2={h / 2} stroke="#d1d5db" strokeWidth="1.5" />
      <rect x={xLow} y={h / 2 - 9} width={Math.max(2, xHigh - xLow)} height="18" fill="#22a35f" fillOpacity="0.22" rx="4" stroke="#1a7a4a" strokeOpacity="0.4" />
      <line x1={xLow} x2={xLow} y1={h / 2 - 12} y2={h / 2 + 12} stroke="#1a7a4a" strokeWidth="2" />
      <line x1={xHigh} x2={xHigh} y1={h / 2 - 12} y2={h / 2 + 12} stroke="#1a7a4a" strokeWidth="2" />
      <circle cx={xMid} cy={h / 2} r="7" fill="#1a7a4a" stroke="white" strokeWidth="2.5" />
      <text x={xLow} y={h / 2 - 17} textAnchor="middle" fontSize="11" fill="#4b5563">{fmt(low)}</text>
      <text x={xHigh} y={h / 2 - 17} textAnchor="middle" fontSize="11" fill="#4b5563">{fmt(high)}</text>
      <text x={xMid} y={h / 2 + 27} textAnchor="middle" fontSize="12" fontWeight="700" fill="#0f3d24">{fmt(mid)}</text>
    </svg>
  );
}
