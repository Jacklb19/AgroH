"use client";
import { useEffect, useMemo, useState } from "react";
import { Icon } from "./icons";

const COLORS = ["#d97706", "#1e4d7b", "#dc2626", "#1a7a4a", "#7c3aed", "#0891b2"];

const fmtCOP = (v) =>
  v != null
    ? new Intl.NumberFormat("es-CO", { style: "currency", currency: "COP", maximumFractionDigits: 0 }).format(v)
    : "—";

const fmtNum = (v) =>
  v != null ? new Intl.NumberFormat("es-CO").format(v) : "—";

/* ── KPIs superiores ─────────────────────────────────────────────────── */
function KpiRow({ kpis }) {
  const items = [
    { v: fmtNum(kpis.productos_monitoreados), l: "Productos con precio mayorista" },
    { v: fmtNum(kpis.centrales_abastos),      l: "Centrales de abastos" },
    { v: fmtNum(kpis.insumos_monitoreados),   l: "Insumos monitoreados" },
    { v: fmtNum(kpis.registros_insumos),      l: "Registros de precios IPIA" },
  ];
  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 12, marginBottom: 18 }}>
      {items.map((k) => (
        <div className="metric-mini" key={k.l}>
          <div className="v">{k.v}</div>
          <div className="l">{k.l}</div>
        </div>
      ))}
    </div>
  );
}

/* ── Precios mayoristas (SIPSA) ──────────────────────────────────────── */
function MayoristasCard({ mayoristas }) {
  if (mayoristas.length === 0) return null;
  return (
    <div className="card" style={{ marginTop: 16 }}>
      <div className="card-head">
        <div>
          <h3>Precios mayoristas · centrales de abastos</h3>
          <div className="panel-sub">Precio por kilogramo en pesos colombianos · agregado por producto</div>
        </div>
        <span className="src-badge">fact_precios_mayoristas · SIPSA</span>
      </div>
      <div className="card-body">
        <table className="compare-table">
          <thead>
            <tr>
              <th>Producto</th>
              <th>Precio mín.</th>
              <th>Precio promedio</th>
              <th>Precio máx.</th>
              <th>Volumen</th>
              <th>Centrales</th>
            </tr>
          </thead>
          <tbody>
            {mayoristas.map((m) => (
              <tr key={m.producto}>
                <td><strong>{m.producto}</strong></td>
                <td className="num">{fmtCOP(m.precio_min)}</td>
                <td className="num"><strong>{fmtCOP(m.precio_promedio)}</strong></td>
                <td className="num">{fmtCOP(m.precio_max)}</td>
                <td className="num">{m.volumen_ton != null ? `${fmtNum(m.volumen_ton)} t` : "—"}</td>
                <td className="num">{m.num_centrales}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <div className="summary-strip">
          <span className="ic"><Icon.trend /></span>
          <span>
            <strong>¿Cómo leer esta tabla?</strong> El precio promedio es lo que pagan las centrales
            de abastos por cada kilogramo de tu producto. Si tu costo de producción por kilo está por
            debajo de ese valor, tu cultivo es rentable en el mercado mayorista.
          </span>
        </div>
      </div>
    </div>
  );
}

/* ── Precios de insumos (IPIA) ───────────────────────────────────────── */
function InsumosCard({ insumos, tipos }) {
  const [tipoFiltro, setTipoFiltro] = useState("todos");

  const filtrados = useMemo(
    () => (tipoFiltro === "todos" ? insumos : insumos.filter((i) => i.tipo === tipoFiltro)),
    [insumos, tipoFiltro],
  );

  if (insumos.length === 0) return null;

  const totalTipos = tipos.reduce((s, t) => s + t.total, 0);

  return (
    <div className="card" style={{ marginTop: 16 }}>
      <div className="card-head">
        <div>
          <h3>Precios de insumos agrícolas</h3>
          <div className="panel-sub">Último precio registrado por insumo · fertilizantes, plaguicidas y más</div>
        </div>
        <span className="src-badge">fact_precios_insumos · IPIA</span>
      </div>
      <div className="card-body">

        {/* Distribución por tipo de insumo */}
        {tipos.length > 0 && (
          <div className="bars" style={{ marginBottom: 18 }}>
            {tipos.map((t, i) => {
              const pct = totalTipos > 0 ? Math.round((t.total / totalTipos) * 100) : 0;
              return (
                <div className="bar-item" key={t.tipo}>
                  <div className="bar-meta"><span className="lbl">{t.tipo}</span><span className="val">{pct}%</span></div>
                  <div className="bar-track"><span className="bar-fill" style={{ width: `${pct}%`, background: COLORS[i % COLORS.length] }}></span></div>
                </div>
              );
            })}
          </div>
        )}

        {/* Filtro por tipo */}
        <div className="field" style={{ maxWidth: 280, marginBottom: 14 }}>
          <label>Filtrar por tipo de insumo</label>
          <select value={tipoFiltro} onChange={(e) => setTipoFiltro(e.target.value)}>
            <option value="todos">Todos los tipos</option>
            {tipos.map((t) => <option key={t.tipo} value={t.tipo}>{t.tipo}</option>)}
          </select>
        </div>

        <table className="compare-table">
          <thead>
            <tr>
              <th>Insumo</th>
              <th>Tipo</th>
              <th>Precio</th>
              <th>Unidad</th>
              <th>Región</th>
              <th>Período</th>
            </tr>
          </thead>
          <tbody>
            {filtrados.map((i) => (
              <tr key={i.nombre}>
                <td><strong>{i.nombre}</strong></td>
                <td>{i.tipo}</td>
                <td className="num"><strong>{fmtCOP(i.precio)}</strong></td>
                <td>{i.unidad || "—"}</td>
                <td>{i.region || "—"}</td>
                <td className="num">{i.periodo || "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>

        <div className="summary-strip">
          <span className="ic"><Icon.check /></span>
          <span>
            <strong>Planea tus compras.</strong> Conocer el precio de fertilizantes y plaguicidas antes
            de la temporada de siembra te permite presupuestar mejor y comparar con los precios de tu
            proveedor local.
          </span>
        </div>
      </div>
    </div>
  );
}

/* ── Página principal ────────────────────────────────────────────────── */
export default function PageEconomia() {
  const [data, setData] = useState(null);
  const [err,  setErr]  = useState(false);

  useEffect(() => {
    fetch("/api/economia")
      .then((r) => r.json())
      .then(setData)
      .catch(() => setErr(true));
  }, []);

  return (
    <section className="section">
      <div className="container">

        <div className="section-head">
          <span className="eyebrow">Economía agrícola</span>
          <h2>Precios de mercado e insumos para decidir mejor</h2>
          <p>
            Cuánto pagan las centrales de abastos por tu cosecha y cuánto cuestan los insumos para
            producirla. Datos oficiales de SIPSA (DANE) e IPIA (DANE / UPRA) consultados desde la
            base de datos del sistema.
          </p>
        </div>

        {err && (
          <div className="card"><div className="card-body">No fue posible cargar los datos económicos.</div></div>
        )}

        {!err && !data && (
          <div className="card"><div className="card-body">Cargando información económica…</div></div>
        )}

        {!err && data && (
          <>
            {!data.fromDB && (
              <div className="vista-general-banner" style={{ marginBottom: 16 }}>
                <span className="vg-icon"><Icon.alert /></span>
                <div>
                  <strong>Base de datos no disponible</strong>
                  <p>Los datos económicos se consultan en vivo desde <code>/api/economia</code>. Reintenta en unos minutos.</p>
                </div>
              </div>
            )}

            {data.fromDB && <KpiRow kpis={data.kpis} />}
            <MayoristasCard mayoristas={data.mayoristas} />
            <InsumosCard insumos={data.insumos} tipos={data.tipos_insumo} />
          </>
        )}

      </div>
    </section>
  );
}
