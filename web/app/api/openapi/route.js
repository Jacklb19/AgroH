const q = (name, desc, required = false, type = "string") => ({ name, in: "query", required, description: desc, schema: { type } });
const ok = (description) => ({ "200": { description }, "503": { description: "Base de datos no disponible" } });

const SPEC = {
  openapi: "3.1.0",
  info: {
    title:       "AgroIA Colombia API",
    description: "API pública sobre datos abiertos colombianos: pronósticos de rendimiento por municipio y cultivo (XGBoost validado con backtest), producción real, precios de insumos y asistente conversacional.",
    version:     "2.0.0",
    license:     { name: "MIT" },
  },
  servers: [{ url: "/api", description: "Servidor actual" }],
  tags: [
    { name: "Pronóstico", description: "Modelo de pronóstico de rendimiento (pred_pronostico)" },
    { name: "Datos",      description: "Catálogo, producción, precios y clima" },
    { name: "Asistente",  description: "Chat conversacional con herramientas SQL" },
    { name: "Sistema",    description: "Salud y metadatos" },
  ],
  paths: {
    "/health":     { get: { tags: ["Sistema"], summary: "Healthcheck de la base de datos", responses: ok("Servicio OK") } },
    "/modelo":     { get: { tags: ["Pronóstico"], summary: "Versión activa del modelo, métricas de backtest, años pronosticados y fase ENSO actual", responses: ok("Metadatos del modelo") } },
    "/resumen":    { get: { tags: ["Datos"], summary: "Cobertura: municipios, cultivos, combinaciones y años de producción", responses: ok("Cifras de cobertura") } },
    "/municipios": { get: { tags: ["Datos"], summary: "Municipios con pronóstico", responses: ok("[{ id, nombre, departamento }]") } },
    "/cultivos": {
      get: {
        tags: ["Datos"], summary: "Cultivos con pronóstico (opcionalmente solo los de un municipio)",
        parameters: [q("muni", "Código DIVIPOLA del municipio")],
        responses: ok("[{ id, nombre, ciclo }]"),
      },
    },
    "/prediccion": {
      post: {
        tags: ["Pronóstico"], summary: "Pronóstico para municipio × cultivo × año, con escenarios, historia real y explicación",
        requestBody: {
          required: true,
          content: { "application/json": { schema: {
            type: "object",
            properties: { id_municipio: { type: "string" }, id_cultivo: { type: "integer" }, anio: { type: "integer" } },
            required: ["id_municipio", "id_cultivo", "anio"],
          } } },
        },
        responses: ok("{ yhat, low, high, escenarios[], historia[], shap[], variabilidad, precision_cultivo } o { sin_datos: true }"),
      },
    },
    "/comparativo": {
      get: {
        tags: ["Pronóstico"], summary: "Pronóstico del mismo cultivo en los municipios del departamento",
        parameters: [q("muni", "Código DIVIPOLA", true), q("cultivo", "id_cultivo", true, "integer"), q("anio", "Año", true, "integer")],
        responses: ok("{ departamento, total, posicion, filas[] }"),
      },
    },
    "/cultivo": {
      get: {
        tags: ["Pronóstico"], summary: "Sin id: cultivos principales. Con id: serie nacional (real, backtest, pronóstico), mejores municipios y cambios",
        parameters: [q("id", "id_cultivo", false, "integer"), q("anio", "Año del pronóstico", false, "integer")],
        responses: ok("{ serie[], top[], cambios }"),
      },
    },
    "/mapa": {
      get: {
        tags: ["Pronóstico"], summary: "Cambio esperado por municipio frente a su último año registrado",
        parameters: [q("anio", "Año del pronóstico", false, "integer")],
        responses: ok("{ puntos[{ municipio, lat, lon, cambio, tendencia }], resumen }"),
      },
    },
    "/recomendacion": {
      post: {
        tags: ["Pronóstico"], summary: "Recomendaciones: ventana de siembra, aptitud del suelo (UPRA) y fase ENSO (NOAA)",
        requestBody: {
          required: true,
          content: { "application/json": { schema: {
            type: "object",
            properties: { id_municipio: { type: "string" }, id_cultivo: { type: "integer" }, semestre: { type: "string", enum: ["A", "B"] } },
            required: ["id_municipio", "id_cultivo"],
          } } },
        },
        responses: ok("{ recomendaciones[] }"),
      },
    },
    "/simular": { post: { tags: ["Pronóstico"], summary: "Simulación orientativa de escenarios con elasticidades agronómicas típicas", responses: ok("{ baseline, proyectado, contribuciones[] }") } },
    "/economia": { get: { tags: ["Datos"], summary: "Índice de precios de insumos (IPIA) y precios mayoristas SIPSA", responses: ok("{ indice[], insumos[], mayoristas }") } },
    "/catalogo": { get: { tags: ["Datos"], summary: "Catálogo de fuentes abiertas y registros cargados", responses: ok("[{ id, titulo, entidad, uri, estrategico, filas }]") } },
    "/clima/actual": {
      get: {
        tags: ["Datos"], summary: "Clima en vivo del municipio (Open-Meteo)",
        parameters: [q("id", "Código DIVIPOLA"), q("municipio", "Nombre (alternativa a id)")],
        responses: ok("Actual + pronóstico 3 días"),
      },
    },
    "/chat": {
      post: {
        tags: ["Asistente"], summary: "Chat con Claude o Groq (herramientas SQL sobre la BD)",
        requestBody: {
          required: true,
          content: { "application/json": { schema: {
            type: "object",
            properties: {
              messages:  { type: "array", items: { type: "object", properties: { role: { type: "string" }, content: { type: "string" } } } },
              sessionId: { type: "string", description: "UUID opcional para persistir la conversación" },
            },
            required: ["messages"],
          } } },
        },
        responses: { "200": { description: "{ reply, sessionId }" } },
      },
    },
  },
};

export async function GET() {
  return Response.json(SPEC);
}
