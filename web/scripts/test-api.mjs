/* Prueba de humo de la API contra datos reales.
   Uso: npm run test:api                       (http://localhost:3000)
        BASE_URL=https://mi-sitio.vercel.app npm run test:api          */

const BASE = (process.env.BASE_URL || "http://localhost:3000").replace(/\/$/, "");
let fallos = 0;

async function probar(nombre, fn) {
  const t0 = Date.now();
  try {
    await fn();
    console.log(`  ✓ ${nombre} (${Date.now() - t0} ms)`);
  } catch (err) {
    fallos += 1;
    console.log(`  ✗ ${nombre}: ${err.message}`);
  }
}

function afirmar(cond, msg) {
  if (!cond) throw new Error(msg);
}

async function get(ruta) {
  const res = await fetch(BASE + ruta);
  afirmar(res.ok, `HTTP ${res.status} en ${ruta}`);
  return res.json();
}

async function post(ruta, body) {
  const res = await fetch(BASE + ruta, {
    method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body),
  });
  afirmar(res.ok, `HTTP ${res.status} en ${ruta}`);
  return res.json();
}

console.log(`API de AgroIA en ${BASE}\n`);
const anio = new Date().getFullYear();
let muni, cultivo;

await probar("GET /api/health", async () => {
  const d = await get("/api/health");
  afirmar(d.ok === true, "la base de datos no responde");
});

await probar("GET /api/modelo", async () => {
  const d = await get("/api/modelo");
  afirmar(d.fromDB && d.version?.id, "sin versión activa del modelo");
  afirmar(d.metricas?.modelo?.r2 > 0.5, "R² ausente o muy bajo");
  afirmar(d.anios.some((a) => a.anio >= anio && a.escenarios.length === 3), `sin pronóstico por escenarios para ${anio} o después`);
});

await probar("GET /api/resumen", async () => {
  const d = await get("/api/resumen");
  afirmar(d.municipios > 1000 && d.cultivos > 100, "cobertura inesperadamente baja");
});

await probar("GET /api/municipios", async () => {
  const d = await get("/api/municipios");
  afirmar(Array.isArray(d) && d.length > 1000, "lista de municipios incompleta");
  muni = d.find((m) => m.id === "73001") || d[0];
  afirmar(muni.nombre && muni.departamento && !/^[A-ZÁÉÍÓÚÑ ]+$/.test(muni.nombre), "nombres sin formato");
});

await probar("GET /api/cultivos?muni=", async () => {
  const d = await get(`/api/cultivos?muni=${muni.id}`);
  afirmar(Array.isArray(d) && d.length > 0, "el municipio no tiene cultivos con pronóstico");
  cultivo = d.find((c) => c.nombre === "Arroz") || d[0];
});

await probar("POST /api/prediccion", async () => {
  const d = await post("/api/prediccion", { id_municipio: muni.id, id_cultivo: cultivo.id, anio });
  afirmar(d.fromDB && !d.sin_datos, "sin pronóstico para una combinación ofrecida en el formulario");
  afirmar(d.low <= d.yhat && d.yhat <= d.high, "el pronóstico está fuera de su rango");
  afirmar(d.escenarios.length === 3, "faltan escenarios");
  afirmar(d.historia.length > 0, "sin historia real");
});

await probar("POST /api/prediccion (año sin pronóstico → sin_datos)", async () => {
  const d = await post("/api/prediccion", { id_municipio: muni.id, id_cultivo: cultivo.id, anio: anio + 20 });
  afirmar(d.sin_datos === true, "debió responder sin_datos en lugar de inventar un valor");
});

await probar("GET /api/comparativo", async () => {
  const d = await get(`/api/comparativo?muni=${muni.id}&cultivo=${cultivo.id}&anio=${anio}`);
  afirmar(d.fromDB && Array.isArray(d.filas), "respuesta inválida");
  afirmar(d.filas.some((f) => f.actual), "no incluye el municipio consultado");
});

await probar("POST /api/recomendacion", async () => {
  const d = await post("/api/recomendacion", { id_municipio: muni.id, id_cultivo: cultivo.id, semestre: "B" });
  afirmar(d.recomendaciones?.length === 3, "se esperaban 3 recomendaciones");
});

await probar("POST /api/simular (base del cliente)", async () => {
  const d = await post("/api/simular", { muni: muni.nombre, cultivo: cultivo.nombre, baseline: 5, lluvia_pct: 0, temp_delta_c: 0, enso: "Neutral", fertilizante_pct: 0 });
  afirmar(d.baseline === 5 && d.proyectado === 5, "con controles en cero el resultado debe igualar la base");
});

await probar("GET /api/cultivo", async () => {
  const lista = await get("/api/cultivo");
  afirmar(lista.length > 5, "pocos cultivos principales");
  const d = await get(`/api/cultivo?id=${lista[0].id}&anio=${anio}`);
  afirmar(d.serie.some((s) => s.real != null) && d.serie.some((s) => s.pronostico != null), "serie incompleta");
});

await probar("GET /api/mapa", async () => {
  const d = await get(`/api/mapa?anio=${anio}`);
  afirmar(d.puntos.length > 1000 && d.resumen.total === d.puntos.length, "mapa incompleto");
});

await probar("GET /api/economia", async () => {
  const d = await get("/api/economia");
  afirmar(d.indice.length > 12 && d.insumos.length > 10, "datos de precios incompletos");
  afirmar(!d.insumos.some((i) => i.nombre.startsWith("2,4-D") && i.grupo === "Fertilizantes"), "herbicida clasificado como fertilizante");
});

await probar("GET /api/catalogo", async () => {
  const d = await get("/api/catalogo");
  afirmar(d.some((f) => f.filas > 0), "ninguna fuente con registros");
});

await probar("GET /api/openapi", async () => {
  const d = await get("/api/openapi");
  afirmar(d.paths["/prediccion"] && d.paths["/modelo"], "especificación desactualizada");
});

console.log(fallos ? `\n${fallos} prueba(s) fallaron.` : "\nTodas las pruebas pasaron.");
process.exit(fallos ? 1 : 0);
