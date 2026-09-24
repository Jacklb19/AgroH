"use client";
import useAncho from "./charts/useAncho";
import { useEffect, useMemo, useState } from "react";
import { Icon } from "./icons";
import { Source } from "./ui";
import { fmtNum, signed } from "@/lib/format";

const COP = (v) => (v == null ? "—" : `$${Math.round(v).toLocaleString("es-CO")}`);

/* Índice mensual de precios de insumos (fertilizantes vs. agroquímicos). */
function IndiceChart({ serie }) {
  const [ref, ancho] = useAncho(900);
  const w = ancho, h = ancho < 500 ? 220 : 280, pad = { l: 40, r: 16, t: 26, b: 28 };
  const vals = serie.flatMap((d) => [d.fertilizantes, d.agroquimicos]).filter((v) => v != null);
  const maxY = Math.ceil(Math.max(...vals) / 20) * 20;
  const minY = Math.floor(Math.min(...vals) / 20) * 20;
  const x = (i) => pad.l + (i * (w - pad.l - pad.r)) / (serie.length - 1);
  const y = (v) => pad.t + (1 - (v - minY) / (maxY - minY)) * (h - pad.t - pad.b);
  const path = (k) => serie
    .map((d, i) => (d[k] == null ? "" : `${i && serie[i - 1][k] != null ? "L" : "M"}${x(i).toFixed(1)} ${y(d[k]).toFixed(1)}`))
    .join(" ");
  const pico = serie.reduce((a, d, i) => ((d.fertilizantes ?? -1) > (serie[a].fertilizantes ?? -1) ? i : a), 0);
  const ticks = [];
  for (let v = minY; v <= maxY; v += 20) ticks.push(v);
  return (
    <div className="serie-chart" ref={ref}>
      <svg viewBox={`0 0 ${w} ${h}`} width={w} height={h} role="img" aria-label="Índice de precios de fertilizantes y agroquímicos por mes">
        {ticks.map((v) => (
          <g key={v}>
            <line x1={pad.l} x2={w - pad.r} y1={y(v)} y2={y(v)} stroke="#e5e7eb" strokeDasharray="2 4" />
            <text x={pad.l - 8} y={y(v) + 3} fontSize="10.5" fill="#6b7280" textAnchor="end">{v}</text>
          </g>
        ))}
        {serie.map((d, i) => d.mes === 1 && (w > 500 || d.anio % 2 === 0) && (
          <text key={d.periodo} x={x(i)} y={h - 8} fontSize="10.5" fill="#6b7280" textAnchor="middle">{d.anio}</text>
        ))}
        <path d={path("agroquimicos")} stroke="#1e4d7b" strokeWidth="2" fill="none" />
        <path d={path("fertilizantes")} stroke="#d97706" strokeWidth="2.5" fill="none" />
        <circle cx={x(pico)} cy={y(serie[pico].fertilizantes)} r="4.5" fill="#d97706" stroke="white" strokeWidth="1.5" />
        <text x={x(pico)} y={y(serie[pico].fertilizantes) - 10} fontSize="11" fontWeight="600" fill="#b45309" textAnchor="middle">
          Máximo: {fmtNum(serie[pico].fertilizantes, 0)} ({serie[pico].periodo})
        </text>
      </svg>
      <div className="chart-legend">
        <span><i style={{ background: "#d97706" }} /> Fertilizantes</span>
        <span><i style={{ background: "#1e4d7b" }} /> Agroquímicos</span>
      </div>
    </div>
  );
}

export default function PreciosInsumos() {
  const [data, setData] = useState(null);
  const [grupo, setGrupo] = useState("Fertilizantes");

  useEffect(() => {
    fetch("/api/economia").then((r) => r.json()).then(setData).catch(() => setData({ error: true }));
  }, []);

  const resumen = useMemo(() => {
    if (!data?.indice?.length) return null;
    const anios = [...new Set(data.indice.map((d) => d.anio))];
    const prom = (a) => {
      const v = data.indice.filter((d) => d.anio === a && d.fertilizantes != null).map((d) => d.fertilizantes);
      return v.length ? v.reduce((s, x) => s + x, 0) / v.length : null;
    };
    const promedios = anios.map((a) => ({ anio: a, v: prom(a) })).filter((p) => p.v != null);
    const base = promedios[0];
    const pico = promedios.reduce((a, p) => (p.v > a.v ? p : a), promedios[0]);
    const ultimo = data.indice.filter((d) => d.fertilizantes != null).at(-1);
    return { base, pico, ultimo };
  }, [data]);

  if (!data) return <div className="panel-empty">Cargando precios…</div>;
  if (data.error || !data.fromDB) {
    return <div className="notice-bar"><Icon.alert size={15} /> Los datos de precios no están disponibles en este momento.</div>;
  }

  const insumos = data.insumos.filter((i) => i.grupo === grupo);
  const my = data.mayoristas;
  const cambio = (a, b) => ((a - b) / b) * 100;

  return (
    <div className="fade-in">
      <div className="card">
        <div className="card-head">
          <div>
            <h3>¿Cuánto han subido los insumos?</h3>
            <div className="panel-sub">
              Índice de precios de insumos agropecuarios: muestra cómo cambia el precio frente a un periodo base (100), no el precio en pesos.
            </div>
          </div>
        </div>
        <div className="card-body">
          {resumen && (
            <div className="kpi-row">
              <div className="kpi">
                <div className="v">{signed(cambio(resumen.pico.v, resumen.base.v), 0)} %</div>
                <div className="l">Subida de los fertilizantes entre {resumen.base.anio} y {resumen.pico.anio} (promedio anual)</div>
              </div>
              <div className="kpi">
                <div className="v">{fmtNum(resumen.ultimo.fertilizantes, 0)}</div>
                <div className="l">Índice de fertilizantes en {resumen.ultimo.periodo}</div>
              </div>
              <div className="kpi">
                <div className="v">{signed(cambio(resumen.ultimo.fertilizantes, resumen.base.v), 0)} %</div>
                <div className="l">Frente al promedio de {resumen.base.anio}</div>
              </div>
            </div>
          )}
          <IndiceChart serie={data.indice} />
          <div className="table-tools">
            <div className="chip-row">
              {["Fertilizantes", "Agroquímicos"].map((g) => (
                <button key={g} className={`chip ${g === grupo ? "active" : ""}`} onClick={() => setGrupo(g)}>{g}</button>
              ))}
            </div>
          </div>
          <div className="table-scroll">
            <table className="data-table compact">
              <thead><tr><th>Insumo</th><th>Índice actual</th><th>Cambio en 12 meses</th><th>Frente a su máximo</th></tr></thead>
              <tbody>
                {insumos.map((i) => (
                  <tr key={i.clave}>
                    <td><strong>{i.nombre}</strong></td>
                    <td className="num">{fmtNum(i.actual, 1)}</td>
                    <td className={`num ${i.cambio_12m > 0 ? "neg" : i.cambio_12m < 0 ? "pos" : ""}`}>
                      {i.cambio_12m == null ? "—" : `${signed(i.cambio_12m)} %`}
                    </td>
                    <td className="num">{i.desde_maximo == null ? "—" : `${signed(i.desde_maximo)} %`}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <Source>Índice de precios de insumos agrícolas (DANE / UPRA) · datos.gov.co</Source>
        </div>
      </div>

      {my?.productos?.length > 0 && (
        <div className="card" style={{ marginTop: 16 }}>
          <div className="card-head">
            <div>
              <h3>Precios en centrales de abastos</h3>
              <div className="panel-sub">
                Precio mayorista por kilo en {my.periodo} ({my.ciudades.join(", ")}). Es el único mes disponible por ahora:
                sirve como referencia, no como tendencia.
              </div>
            </div>
          </div>
          <div className="table-scroll">
            <table className="data-table compact">
              <thead><tr><th>Producto</th><th>Precio promedio (COP/kg)</th><th>Mínimo – máximo entre ciudades</th><th>Centrales</th></tr></thead>
              <tbody>
                {my.productos.map((p) => (
                  <tr key={p.producto}>
                    <td><strong>{p.producto}</strong></td>
                    <td className="num"><strong>{COP(p.promedio)}</strong></td>
                    <td className="num">{p.centrales > 1 ? `${COP(p.minimo)} – ${COP(p.maximo)}` : "—"}</td>
                    <td className="num">{p.centrales}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="card-body" style={{ paddingTop: 0 }}><Source>SIPSA (DANE)</Source></div>
        </div>
      )}
    </div>
  );
}
