"use client";
import { useEffect, useState } from "react";

/* Guía paso a paso: resalta cada elemento (refs) con un recuadro y un globo de texto. */
export default function TourOverlay({ steps, refs, onClose }) {
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

