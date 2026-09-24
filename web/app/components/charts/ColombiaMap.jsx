"use client";

const RIESGO_COLOR = { ALTO: "#dc2626", MEDIO: "#d97706", BAJO: "#1a7a4a" };

/* Contorno simplificado de Colombia continental (lon, lat), sentido horario
   desde Sapzurro. Suficiente para ubicar municipios a esta escala. */
const BORDE = [
  [-77.36, 8.67], [-76.9, 8.1], [-76.3, 8.9], [-75.6, 9.4], [-75.5, 10.4], [-74.85, 11.1],
  [-74.2, 11.3], [-73.3, 11.3], [-72.2, 11.9], [-71.95, 12.3], [-71.66, 12.46], [-71.3, 11.95],
  [-72.25, 11.15], [-72.9, 10.4], [-73.05, 9.3], [-72.6, 8.6], [-72.45, 7.5], [-71.9, 7.05],
  [-70.9, 7.1], [-70.1, 6.95], [-69.3, 6.1], [-67.85, 6.3], [-67.45, 5.9], [-67.8, 5.0],
  [-67.85, 4.1], [-67.3, 3.2], [-67.85, 2.8], [-67.2, 2.2], [-66.87, 1.22], [-67.1, 1.1],
  [-68.2, 1.7], [-69.8, 1.7], [-69.9, 1.1], [-70.05, 0.6], [-70.05, -0.15], [-69.6, -0.6],
  [-69.4, -1.3], [-69.95, -4.22], [-70.3, -3.8], [-70.7, -3.8], [-70.1, -2.8], [-70.9, -2.2],
  [-72.0, -2.45], [-73.1, -1.9], [-73.6, -1.25], [-74.8, -0.2], [-75.3, -0.1], [-76.4, 0.4],
  [-77.4, 0.8], [-78.2, 1.2], [-78.85, 1.45], [-78.6, 1.8], [-78.0, 2.6], [-77.4, 3.5],
  [-77.3, 4.3], [-77.4, 5.5], [-77.35, 6.6], [-77.9, 7.2], [-77.7, 7.7],
];

/* Proyección equirectangular con la misma escala en ambos ejes. */
const LON0 = -79.3, LAT1 = 12.8, S = 14, PAD = 10;
const W = Math.round((-66.6 - LON0) * S + PAD * 2);
const H = Math.round((LAT1 + 4.5) * S + PAD * 2);
const proj = (lon, lat) => [PAD + (lon - LON0) * S, PAD + (LAT1 - lat) * S];

const PATH = BORDE.map(([lon, lat], i) => {
  const [x, y] = proj(lon, lat);
  return `${i ? "L" : "M"}${x.toFixed(1)} ${y.toFixed(1)}`;
}).join(" ") + " Z";

export default function ColombiaMap({ puntos = [], height = 280 }) {
  const pins = (Array.isArray(puntos) ? puntos : [])
    .filter((p) => Number.isFinite(p.lat) && Number.isFinite(p.lon))
    .map((p) => {
      const [x, y] = proj(p.lon, p.lat);
      return {
        x, y,
        color: RIESGO_COLOR[p.riesgo] || "#6b7280",
        label: [p.municipio, p.departamento].filter(Boolean).join(", "),
        riesgo: p.riesgo,
      };
    });

  return (
    <svg viewBox={`0 0 ${W} ${H}`} width="100%" height={height} style={{ display: "block" }}
      role="img" aria-label={`Mapa de Colombia con ${pins.length} municipios según su nivel de riesgo climático`}>
      <defs>
        <linearGradient id="colFill" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="#22a35f" stopOpacity="0.14" />
          <stop offset="100%" stopColor="#155436" stopOpacity="0.30" />
        </linearGradient>
      </defs>
      <path d={PATH} fill="url(#colFill)" stroke="#155436" strokeWidth="1.2" strokeOpacity="0.5" strokeLinejoin="round" />
      {pins.map((p, i) => (
        <g key={i}>
          <title>{`${p.label || "Municipio"} · riesgo ${String(p.riesgo || "").toLowerCase()}`}</title>
          <circle cx={p.x} cy={p.y} r="7" fill={p.color} fillOpacity="0.16" />
          <circle cx={p.x} cy={p.y} r="3.6" fill={p.color} stroke="white" strokeWidth="1.2" />
        </g>
      ))}
    </svg>
  );
}
