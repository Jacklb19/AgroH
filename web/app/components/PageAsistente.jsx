"use client";
import { useState, useRef, useEffect, Fragment } from "react";
import { Icon } from "./icons";
import { SectionHead } from "./ui";

const IDEAS = [
  { tema: "Rendimiento", icon: Icon.wheat, preguntas: [
    "¿Cuál es el mejor municipio para arroz?",
    "Compara Espinal y Saldaña en rendimiento",
  ] },
  { tema: "Riesgo y clima", icon: Icon.cloudRain, preguntas: [
    "¿Qué municipios tuvieron riesgo climático alto?",
    "¿Cómo está el clima en Ibagué?",
  ] },
  { tema: "Escenarios", icon: Icon.sliders, preguntas: [
    "¿Qué pasa si hay El Niño en Pasto con papa?",
    "Proyecta café en Armenia con sequía",
  ] },
  { tema: "Qué sembrar", icon: Icon.sprout, preguntas: [
    "¿Qué cultivo me recomiendas para Manizales?",
    "¿Cuál es el panorama general del sistema?",
  ] },
];

const CAPACIDADES = [
  "Consultar el rendimiento esperado por municipio y cultivo",
  "Comparar de 2 a 5 municipios",
  "Proyectar escenarios de El Niño, La Niña o sequía",
  "Mostrar qué se siembra más en una zona y cómo le ha ido",
  "Consultar el riesgo climático histórico y el clima de un municipio",
];

/* Formato mínimo para las respuestas: **negrita**, listas con "-", "•" o "1." */
function inline(text) {
  return text.split(/(\*\*[^*]+\*\*)/g).map((part, i) =>
    part.startsWith("**") && part.endsWith("**") ? <strong key={i}>{part.slice(2, -2)}</strong> : <Fragment key={i}>{part}</Fragment>
  );
}

function RichText({ text }) {
  const blocks = [];
  let list = null;
  text.split("\n").forEach((raw, i) => {
    const line = raw.trim();
    const item = line.match(/^(?:[-•*]|\d+[.)])\s+(.*)$/);
    if (item) {
      if (!list) { list = []; blocks.push({ type: "ul", items: list, key: i }); }
      list.push(item[1]);
      return;
    }
    list = null;
    if (line) blocks.push({ type: "p", text: line.replace(/^#+\s*/, ""), key: i });
  });
  return blocks.map((b) =>
    b.type === "ul"
      ? <ul key={b.key}>{b.items.map((it, j) => <li key={j}>{inline(it)}</li>)}</ul>
      : <p key={b.key}>{inline(b.text)}</p>
  );
}

function BotAvatar() {
  return <div className="chat-avatar bot"><Icon.sparkles size={16} /></div>;
}

function Mensaje({ msg, onHablar, hablando }) {
  const isUser = msg.role === "user";
  return (
    <div className={`chat-bubble ${isUser ? "user" : "ai"}`}>
      {!isUser && <BotAvatar />}
      <div className="bubble-body">
        {isUser ? msg.content : <RichText text={msg.content} />}
        {!isUser && onHablar && (
          <button
            type="button"
            className={`chat-speak ${hablando ? "active" : ""}`}
            onClick={onHablar}
            aria-label={hablando ? "Detener lectura" : "Escuchar respuesta"}
          >
            {hablando ? <Icon.stop size={12} /> : <Icon.volume size={13} />}
            {hablando ? "Detener" : "Escuchar"}
          </button>
        )}
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

  /* ── Sesión persistente (memoria en BD) ─────────────────────────── */
  const [sessionId, setSessionId] = useState(null);
  useEffect(() => {
    let sid = null;
    try { sid = localStorage.getItem("agroia_chat_session"); } catch {}
    if (!sid) {
      sid = (crypto.randomUUID && crypto.randomUUID()) || `s-${Date.now()}-${Math.random().toString(36).slice(2)}`;
      try { localStorage.setItem("agroia_chat_session", sid); } catch {}
    }
    setSessionId(sid);
  }, []);

  /* ── Voz (Web Speech API) ───────────────────────────────────────── */
  const [escuchando, setEscuchando]       = useState(false);
  const [vozDisponible, setVozDisponible] = useState(false);
  const [ttsDisponible, setTtsDisponible] = useState(false);
  const recognitionRef = useRef(null);

  useEffect(() => {
    setTtsDisponible(!!window.speechSynthesis);
    const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
    if (!SR) return;
    setVozDisponible(true);
    const rec = new SR();
    rec.lang = "es-CO";
    rec.continuous = false;
    rec.interimResults = true;
    rec.onresult = (e) => setInput(Array.from(e.results).map((r) => r[0].transcript).join(""));
    rec.onend   = () => setEscuchando(false);
    rec.onerror = () => setEscuchando(false);
    recognitionRef.current = rec;
    return () => { try { rec.abort(); } catch {} };
  }, []);

  const toggleVoz = () => {
    const rec = recognitionRef.current;
    if (!rec) return;
    if (escuchando) { rec.stop(); return; }
    try { rec.start(); setEscuchando(true); } catch {}
  };

  /* ── Lectura en voz alta bajo demanda (botón por mensaje) ───────── */
  const [hablandoIdx, setHablandoIdx] = useState(null);

  const toggleHablar = (texto, idx) => {
    if (!window.speechSynthesis) return;
    if (hablandoIdx === idx) {
      window.speechSynthesis.cancel();
      setHablandoIdx(null);
      return;
    }
    const limpio = String(texto || "").replace(/[*_`>#]/g, "").slice(0, 600);
    const u = new SpeechSynthesisUtterance(limpio);
    u.lang = "es-CO";
    u.rate = 1.05;
    u.onend   = () => setHablandoIdx(null);
    u.onerror = () => setHablandoIdx(null);
    window.speechSynthesis.cancel();
    window.speechSynthesis.speak(u);
    setHablandoIdx(idx);
  };

  /* detener la voz al salir de la página */
  useEffect(() => () => { try { window.speechSynthesis?.cancel(); } catch {} }, []);

  useEffect(() => {
    const el = messagesRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [messages, loading]);

  const enviar = async (texto) => {
    const txt = (texto || input).trim();
    if (!txt || loading) return;

    const newMsgs = [...messages, { role: "user", content: txt }];
    setMessages(newMsgs);
    setInput("");
    setLoading(true);

    try {
      const res  = await fetch("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ messages: newMsgs, sessionId }),
      });
      const data = await res.json();
      const reply = data.reply
        || (data.ocupado
          ? "El asistente está recibiendo muchas consultas en este momento. Espera unos segundos y vuelve a preguntar."
          : data.error ? "El asistente no está disponible en este momento. Inténtalo de nuevo más tarde." : "No recibimos respuesta del servidor.");
      if (data.error) console.warn("[asistente]", data.error, data.hint || "");
      setMessages([...newMsgs, { role: "assistant", content: reply }]);
    } catch {
      setMessages([...newMsgs, { role: "assistant", content: "Error de conexión. Revisa tu internet e inténtalo de nuevo." }]);
    } finally {
      setLoading(false);
      inputRef.current?.focus();
    }
  };

  const onKey = (e) => {
    if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); enviar(); }
  };

  return (
    <section className="section page-top">
      <div className="container">
        <SectionHead eyebrow="Asistente con IA" tone="blue" title="Pregunta como le preguntarías a un asesor">
          Escribe o dicta tu pregunta sobre rendimientos, riesgo climático, clima o qué sembrar. El asistente consulta los
          datos de la plataforma y responde en lenguaje sencillo.
        </SectionHead>

        <div className="chat-layout">
          <aside className="chat-sidebar">
            <div className="chat-sidebar-card">
              <div className="cs-title"><Icon.sparkles size={14} /> Qué puede hacer</div>
              <ul className="cs-list">
                {CAPACIDADES.map((c) => <li key={c}>{c}</li>)}
              </ul>
            </div>
            <div className="chat-sidebar-card note">
              <div className="cs-title"><Icon.info size={14} /> Ten en cuenta</div>
              <p>
                Solo responde sobre el agro colombiano y la plataforma AgroIA. Las respuestas se basan en sus datos y
                pueden contener errores: úsalas como orientación y confírmalas con un asistente técnico antes de
                tomar decisiones importantes.
              </p>
            </div>
          </aside>

          <div className="chat-main">
            <div className="chat-messages" ref={messagesRef} aria-live="polite">
              {messages.length === 0 && !loading && (
                <div className="chat-empty">
                  <div className="chat-empty-icon"><Icon.message size={28} strokeWidth={1.6} /></div>
                  <p className="chat-empty-title">¿En qué te ayudo hoy?</p>
                  <p className="chat-empty-sub">Elige una pregunta de ejemplo o escribe la tuya.</p>
                  <div className="idea-groups">
                    {IDEAS.map((g) => (
                      <div key={g.tema} className="idea-group">
                        <div className="idea-title"><g.icon size={14} /> {g.tema}</div>
                        {g.preguntas.map((q) => (
                          <button key={q} className="chat-chip" onClick={() => enviar(q)}>{q}</button>
                        ))}
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {messages.map((m, i) => (
                <Mensaje
                  key={i}
                  msg={m}
                  hablando={hablandoIdx === i}
                  onHablar={m.role === "assistant" && ttsDisponible ? () => toggleHablar(m.content, i) : undefined}
                />
              ))}
              {loading && (
                <div className="chat-bubble ai">
                  <BotAvatar />
                  <div className="bubble-body typing-dots"><span /><span /><span /></div>
                </div>
              )}
            </div>

            <div className="chat-input-wrap">
              {messages.length > 0 && (
                <button className="chat-clear" onClick={() => !loading && setMessages([])}>
                  <Icon.refresh /> Nueva conversación
                </button>
              )}
              <div className="chat-input-row">
                <textarea
                  ref={inputRef}
                  className="chat-input"
                  rows={1}
                  aria-label="Tu pregunta"
                  placeholder={escuchando ? "Escuchando…" : "Escribe tu pregunta…"}
                  value={input}
                  onChange={(e) => setInput(e.target.value)}
                  onKeyDown={onKey}
                  disabled={loading}
                />
                {vozDisponible && (
                  <button
                    type="button"
                    onClick={toggleVoz}
                    aria-label={escuchando ? "Detener dictado" : "Dictar pregunta"}
                    title={escuchando ? "Detener dictado" : "Dictar pregunta"}
                    className={`chat-send mic ${escuchando ? "listening" : ""}`}
                  >
                    <Icon.mic />
                  </button>
                )}
                <button className="chat-send" onClick={() => enviar()} disabled={!input.trim() || loading} aria-label="Enviar">
                  <Icon.send />
                </button>
              </div>
              <p className="chat-hint">
                Enter para enviar · Shift + Enter para nueva línea{vozDisponible ? " · puedes dictar con el micrófono" : ""}
              </p>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
