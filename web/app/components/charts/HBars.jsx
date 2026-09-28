"use client";

export default function HBars({ items, unit = "", max }) {
  const peak = max || Math.max(...items.map((i) => i.v), 0.001);
  return (
    <div className="hbars">
      {items.map((it) => (
        <div key={it.l} className="hbar">
          <span className="hbar-lbl" title={it.l}>{it.l}</span>
          <div className="hbar-track"><span style={{ width: `${(it.v / peak) * 100}%` }} /></div>
          <span className="hbar-val">{it.v.toLocaleString("es-CO", { minimumFractionDigits: 1, maximumFractionDigits: 1 })}{unit}</span>
        </div>
      ))}
    </div>
  );
}
