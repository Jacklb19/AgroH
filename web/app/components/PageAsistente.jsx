"use client";
import { useState, useRef, useEffect } from "react";
import { Icon } from "./icons";

const SUGERENCIAS = [
  "¿Cuál es el panorama general del sistema?",
  "¿Qué municipios tienen riesgo ALTO?",
  "¿Cuál es el mejor municipio para arroz?",
  "Muéstrame las alertas más críticas",
  "¿Cómo está el rendimiento en Ibagué?",
  "¿Qué cultivo tiene mejor rendimiento promedio?",
];

function BotIcon() {
  return (
    <div className="chat-avatar bot">
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
        <rect x="3" y="11" width="18" height="11" rx="2" ry="2"/>
        <path d="M7 11V7a5 5 0 0 1 10 0v4"/>
        <line x1="12" y1="3" x2="12" y2="7"/>
        <circle cx="8.5" cy="16" r="1.5" fill="currentColor" stroke="none"/>
        <circle cx="15.5" cy="16" r="1.5" fill="currentColor" stroke="none"/>
      </svg>
    </div>
  );
}

function TypingDots() {
  return (
    <div className="chat-bubble ai">
      <BotIcon />
      <div className="bubble-body typing-dots">
        <span /><span /><span />
      </div>
    </div>
  );
}

function Mensaje({ msg }) {
  const isUser = msg.role === "user";
  return (
    <div className={`chat-bubble ${isUser ? "user" : "ai"}`}>
      {!isUser && <BotIcon />}
      <div className="bubble-body">
        {msg.content.split("\n").map((line, i) => (
          <span key={i}>
            {line}
            {i < msg.content.split("\n").length - 1 && <br />}
          </span>
        ))}
      </div>
    </div>
  );
}

export default function PageAsistente() {
  const [messages, setMessages] = useState([]);
  const [input, setInput]       = useState("");
  const [loading, setLoading]   = useState(false);
  const messagesRef = useRef(null);
  const inputRef    = useRef(null);

  useEffect(() => {
    const el = messagesRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [messages, loading]);

  const enviar = async (texto) => {
    const txt = (texto || input).trim();
    if (!txt || loading) return;

    const userMsg  = { role: "user", content: txt };
    const newMsgs  = [...messages, userMsg];
    setMessages(newMsgs);
    setInput("");
    setLoading(true);

    try {
      const res = await fetch("/api/chat", {
        method:  "POST",
        headers: { "Content-Type": "application/json" },
        body:    JSON.stringify({ messages: newMsgs }),
      });
      const data = await res.json();
      setMessages([...newMsgs, {
        role:    "assistant",
        content: data.reply || data.error || "Sin respuesta.",
      }]);
    } catch {
      setMessages([...newMsgs, {
        role:    "assistant",
        content: "Error de conexión. Verifica que el servidor esté activo.",
      }]);
    } finally {
      setLoading(false);
      inputRef.current?.focus();
    }
  };

  const onKey = (e) => {
    if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); enviar(); }
  };

  const vaciar = () => { if (!loading) setMessages([]); };

  return (
    <section className="section">
      <div className="container">

        {/* Encabezado */}
        <div className="section-head">
          <span className="eyebrow blue">Asistente · Gemini 1.5 Flash</span>
          <h2>Consulta la base de datos en lenguaje natural</h2>
          <p>Pregunta sobre predicciones, alertas climáticas, ranking de municipios o estadísticas generales. El asistente consulta la BD real y responde con datos concretos.</p>
        </div>

        <div className="chat-layout">

          {/* Panel lateral de info */}
          <aside className="chat-sidebar">
            <div className="chat-sidebar-card">
              <div className="cs-title">
                <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <circle cx="12" cy="12" r="10"/><path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/><line x1="12" y1="17" x2="12.01" y2="17"/>
                </svg>
                Qué puedes preguntar
              </div>
              <ul className="cs-list">
                <li>Rendimiento predicho por municipio y cultivo</li>
                <li>Alertas climáticas activas y nivel de riesgo</li>
                <li>Ranking de mejores y peores municipios</li>
                <li>Datos climáticos históricos (lluvia, temperatura)</li>
                <li>Estadísticas generales del sistema</li>
              </ul>
            </div>

            <div className="chat-sidebar-card">
              <div className="cs-title">
                <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>
                </svg>
                Tablas disponibles
              </div>
              <ul className="cs-list mono">
                <li>pred_rendimiento</li>
                <li>pred_alerta_climatica</li>
                <li>fact_produccion_agricola</li>
                <li>fact_clima_mensual</li>
                <li>dim_municipio · dim_cultivo</li>
              </ul>
            </div>
          </aside>

          {/* Área principal del chat */}
          <div className="chat-main">

            <div className="chat-messages" ref={messagesRef}>
              {/* Estado vacío con sugerencias */}
              {messages.length === 0 && !loading && (
                <div className="chat-empty">
                  <div className="chat-empty-icon">
                    <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                      <rect x="3" y="11" width="18" height="11" rx="2" ry="2"/>
                      <path d="M7 11V7a5 5 0 0 1 10 0v4"/>
                      <circle cx="8.5" cy="16" r="1.5" fill="currentColor" stroke="none"/>
                      <circle cx="15.5" cy="16" r="1.5" fill="currentColor" stroke="none"/>
                    </svg>
                  </div>
                  <p className="chat-empty-title">¿En qué te ayudo hoy?</p>
                  <p className="chat-empty-sub">Puedes usar una sugerencia o escribir tu propia pregunta.</p>
                  <div className="chat-chips">
                    {SUGERENCIAS.map((s) => (
                      <button key={s} className="chat-chip" onClick={() => enviar(s)}>
                        {s}
                      </button>
                    ))}
                  </div>
                </div>
              )}

              {/* Mensajes */}
              {messages.map((m, i) => <Mensaje key={i} msg={m} />)}
              {loading && <TypingDots />}
            </div>

            {/* Input */}
            <div className="chat-input-wrap">
              {messages.length > 0 && (
                <button className="chat-clear" onClick={vaciar} title="Nueva conversación">
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <polyline points="1 4 1 10 7 10"/><path d="M3.51 15a9 9 0 1 0 .49-3.51"/>
                  </svg>
                  Nueva conversación
                </button>
              )}
              <div className="chat-input-row">
                <textarea
                  ref={inputRef}
                  className="chat-input"
                  rows={1}
                  placeholder="Escribe tu pregunta sobre la base de datos…"
                  value={input}
                  onChange={(e) => setInput(e.target.value)}
                  onKeyDown={onKey}
                  disabled={loading}
                />
                <button
                  className="chat-send"
                  onClick={() => enviar()}
                  disabled={!input.trim() || loading}
                >
                  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round">
                    <line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/>
                  </svg>
                </button>
              </div>
              <p className="chat-hint">Enter para enviar · Shift+Enter para nueva línea · consulta la BD real</p>
            </div>

          </div>
        </div>
      </div>
    </section>
  );
}
