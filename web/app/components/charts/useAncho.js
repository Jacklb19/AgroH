"use client";
import { useEffect, useRef, useState } from "react";

/* Mide el ancho real del contenedor para dibujar el SVG en píxeles reales
   (texto legible y alto constante en cualquier pantalla). */
export default function useAncho(inicial = 640) {
  const ref = useRef(null);
  const [ancho, setAncho] = useState(inicial);
  useEffect(() => {
    const el = ref.current;
    if (!el || typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver(([e]) => setAncho(Math.max(260, Math.round(e.contentRect.width))));
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  return [ref, ancho];
}
