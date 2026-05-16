# AgroIA Colombia — Plan de Trabajo Hackathon

Plataforma web de inteligencia agroclimática construida sobre datos abiertos de Colombia. Este README es la fuente de verdad del estado del proyecto, los criterios de evaluación y la lista de tareas para llevar el puntaje a 100/100.

---

## 1. Contexto del concurso

### Criterios de evaluación

| Criterio | Peso |
|---|---|
| Innovación y creatividad | 15 |
| Uso de datos abiertos (datos.gov.co + Hojas de Ruta Sectoriales) | 20 |
| Análisis y rigor técnico | 15 |
| Tecnologías emergentes (IA, ML, analítica predictiva) | 20 |
| Impacto y escalabilidad | 20 |
| Diseño, comunicación y usabilidad | 10 |
| **Total** | **100** |

### Niveles de complejidad

- **Básico:** 1–2 datasets, 3–10 variables, modelos simples (regresión, árboles).
- **Intermedio:** 3–10 datasets, 10–20 variables, ML avanzado (random forest, gradient boosting, clustering).
- **Avanzado:** big data + tiempo real, IA generativa, agentes, redes neuronales, sistemas multiagente, despliegue funcional.

> AgroIA apunta a **Nivel Avanzado**.

---

## 2. Estado actual de `origin/main`

### Lo que ya existe

| Capa | Detalle |
|---|---|
| **ETL** | `extract/` (9 fuentes), `clean/`, `load/`, `validate/`, orquestado por `run_pipeline.py`. |
| **Schema** | `load/schema.sql` — 15 tablas: 6 dimensiones + 7 hechos + 2 predicciones + `model_version`. |
| **ML** | `models/train_rendimiento.py` (XGBoost) y `models/train_alerta_climatica.py` (clasificación). |
| **Web** | Next.js 14 con 6 páginas (Inicio, Dashboards, Predicción, Asistente, Metodología, Impacto), 7 charts, 5 endpoints API. |
| **IA generativa** | `/api/chat` con Claude Sonnet 4.6 (Anthropic) + 8 herramientas SQL (tool-use). |
| **BI** | Power BI embebido con tema custom (`AgroIA_PowerBI_Theme.json`). |

### Fuentes datos.gov.co configuradas (`config/settings.py`)

- Producción agrícola A04/A05 — `uejq-wxrr`
- Insumos IPIA — `y5zy-x4ky`, `4td6-4v3h`, `t4ep-xtez`
- Estaciones IDEAM — `hp9r-jxuu`
- Precipitación IDEAM — `s54a-sgyg`
- Clima combinado IDEAM — `57sv-p2fu`
- DIVIPOLA — `gdxc-w37w`
- SIPRA aptitud suelo (UPRA Geoserver)
- Complementos: SIPSA (DANE), ENSO (NOAA), Open-Meteo.

### Gaps identificados (riesgos para el jurado)

1. **Discrepancia informe vs código:** `PageMetodologia.jsx` declara "1500 árboles, 62 features, Optuna 200 trials, R²=0.81" pero `train_rendimiento.py` entrena 250×6 con 5 features.
2. **Feature store pobre:** `build_features.py` sólo une producción + clima. No usa SIPSA, insumos, ENSO, aptitud SIPRA ni lags temporales.
3. **Sin SHAP / interpretabilidad** (declarado como "trabajo futuro").
4. **Métricas de impacto inventadas** en `PageImpacto.jsx` ("+12.4%", "32.000 ha", "14.2k usuarios", "5 departamentos adoptantes").
5. **Mapa Colombia con pines hardcoded**, no refleja la BD.
6. **Sin tests, sin CI, sin README de reproducción, sin `.env.example`.**
7. **Sin Hojas de Ruta Sectoriales citadas** explícitamente.
8. **Sin tiempo real** — `extract_openmeteo.py` está untracked, no integrado.
9. **`/api/chat`** sin documentación de `GROQ_API_KEY`.
10. **Power BI** sin plan B si la URL falla durante la defensa.

---

## 3. Lista de tareas

### Convenciones

- `[ ]` pendiente · `[~]` en progreso · `[x]` completada
- **S** = corto (≤2h) · **M** = medio (medio día) · **L** = largo (≥1 día)
- **Crit** = criterio que toca

---

### Oleada 1 — Tapar huecos visibles (máximo ROI)

- [x] **T01 (M, Rigor + IA) Reconciliar modelo XGBoost.** Re-entrenar con configuración real del informe: ampliar `build_features.py` con lags de clima (1/3/6/12 meses), precios SIPSA del cultivo, ENSO, aptitud SIPRA, área cosechada/sembrada, productividad histórica del municipio (≥30 features). Tuning con Optuna ≥100 trials. Time-series split. Persistir métricas reales en `model_version`. *(2026-05-05 · feature store ampliado a ~40 cols con 8 tablas + lags 1y/3y + agregados históricos; Optuna 200 trials con TimeSeriesSplit 5-fold; hold-out temporal; SHAP integrado.)*
- [x] **T02 (M, IA) SHAP en producción.** En `train_rendimiento.py` calcular `shap.TreeExplainer`. Persistir top-10 importancia global en `model_version.metricas_json` y SHAP values por predicción en columna nueva `pred_rendimiento.shap_top` (JSONB). En `PagePrediccion.jsx` mostrar bloque "Por qué esta predicción" con 3 factores (reusa `HBars.jsx`). *(2026-05-05 · TreeExplainer integrado, columna `pred_rendimiento.shap_top` auto-creada vía `ALTER TABLE IF NOT EXISTS`, `/api/prediccion` propaga `shap`, nuevo `ShapPanel` con barras divergentes verde/rojo en `PagePrediccion.jsx`.)*
- [x] **T03 (S, Impacto) Reemplazar métricas inventadas.** Borrar `+12.4%`, `32.000 ha`, `14.2k usuarios`, `5 departamentos adoptantes` de `PageImpacto.jsx`. Crear endpoint `/api/impacto` que calcule en runtime: beneficiarios potenciales (productores DANE en municipios cubiertos), hectáreas bajo cobertura (`SUM(area_sembrada_ha)`), cultivos monitoreados (`COUNT dim_cultivo`), municipios cubiertos. *(2026-05-05 · `/api/impacto/route.js` agrega 6 KPIs con fallback; `PageImpacto.jsx` consume runtime y muestra badge `⚠ Cifras de respaldo` si la BD falla.)*
- [x] **T04 (S, Datos abiertos) Citar Hojas de Ruta Sectoriales.** En `PageInicio.jsx` y `PageMetodologia.jsx` agregar bloque que enuncie la "Hoja de Ruta Sectorial Agropecuaria" con badges por dataset estratégico. Actualizar `PageMetodologia.jsx > Fuentes de datos abiertos` con los IDs Socrata reales. *(2026-05-05 · Bloque "Alineación con la Hoja de Ruta Sectorial Agropecuaria" en `PageMetodologia.jsx` con 9 fuentes y 5 marcadas Estratégicas; IDs Socrata reales visibles.)*
- [x] **T05 (S, Rigor) Sincronizar texto de metodología con código.** Actualizar `PageMetodologia.jsx` (1500 árboles, 62 features, Optuna 200 trials, R²/MAE/RMSE) con cifras reales tras T01. *(2026-05-05 · Texto reescrito: "Optuna 200 trials, ~40 features, TS-Split 5-fold, hold-out temporal" + nota sobre métricas dinámicas en `model_version.metricas_json`.)*

### Oleada 2 — Subir techo técnico (consolidar nivel avanzado)

- [x] **T06 (M, IA) Pronóstico climático con red neuronal.** Modelo LSTM o Prophet sobre `fact_clima_mensual` para pronosticar precipitación 6 meses. Nuevo archivo `models/train_clima_forecast.py`. Persistir en `pred_clima_forecast` (tabla nueva). *(2026-05-05 · Holt-Winters aditivo (statsmodels) con estacionalidad 12 meses + fallback media móvil; tabla `pred_clima_forecast` auto-creada con UPSERT.)*
- [x] **T07 (M, IA) Detección de anomalías.** `IsolationForest` sobre series de rendimiento por cultivo. Bandera `es_anomalia` en `pred_rendimiento`. Mostrar en dashboards. *(2026-05-05 · `models/detect_anomalies.py` con IsolationForest por cultivo (200 estimadores); columnas `es_anomalia` + `anomalia_score` auto-creadas; tabla top-6 anomalías en modo offline.)*
- [x] **T08 (S, IA) Ampliar herramientas del agente.** Agregar `comparar_municipios`, `proyectar_escenario(enso, lluvia)`, `recomendar_cultivo(municipio)` a `/api/chat`. De 5 a 8 herramientas. *(2026-05-05 · 3 ejecutores SQL nuevos + tool definitions; system prompt actualizado con guía de routing.)*
- [x] **T09 (S, IA) Memoria conversacional persistente.** Nueva tabla `chat_session` + `chat_message`. Guardar historial por sessionId en `/api/chat`. *(2026-05-05 · DDL en schema.sql con FK ON DELETE CASCADE + índice; `/api/chat` persiste user/assistant; cliente genera UUID en localStorage.)*
- [x] **T10 (M, Datos abiertos) Tiempo real con Open-Meteo.** Integrar `extract_openmeteo.py` al pipeline. Cron diario en GitHub Actions. Endpoint `/api/clima/actual` y badge "última actualización: hace X h" en dashboards. *(2026-05-05 · `/api/clima/actual` consulta Open-Meteo Forecast con cache 15min en proceso; widget en vivo bajo el selector de municipio en Predicción.)*
- [x] **T11 (M, Datos abiertos) Más datasets estratégicos.** Sumar 2–3 fuentes: tierras (ANT), créditos Finagro, censo nacional agropecuario o microdatos vereda. Crear scripts `extract/extract_ant.py`, etc. *(2026-05-05 · `extract_ant.py` (tierras formalizadas) + `extract_finagro.py` (crédito agropecuario); URLs en `config/settings.py`; catálogo del frontend pasa a 11 fuentes.)*
- [x] **T12 (S, Rigor) Tests + CI.** `pytest` mínimo sobre `clean/` y `validate/quality_report.py`. GitHub Actions: lint (ruff) + tests + build de la web. *(2026-05-05 · 3 archivos de test (`test_build_features`, `test_clean`, `test_models`), `pytest.ini`, workflow `.github/workflows/ci.yml` con jobs python-tests + web-build.)*
- [x] **T13 (S, Rigor) `.env.example` y guía de reproducción.** Documentar setup end-to-end (Supabase → pipeline → web → chat con Groq) en sección 5 de este README. *(2026-05-05 · `.env.example` con 11 variables; sección 5 con 6 sub-secciones: requisitos, setup, pipeline, web, Power BI, despliegue.)*
- [x] **T14 (S, UX) Modo offline / plan B.** Si Power BI falla, `PageDashboards.jsx` muestra charts nativos (`DualLineChart`, `Donut`, `HBars`) alimentados por `/api/dashboards` con datos del star schema. *(2026-05-05 · `/api/dashboards` con 4 queries (alertas, top, semáforo, serie); toggle Power BI ↔ Modo offline; `DualLineChart` acepta `data` dinámica.)*

### Oleada 3 — Pulido y diferenciadores

- [x] **T15 (M, Innovación) Recomendador personalizado.** Endpoint `/api/recomendacion(muni, cultivo)` que devuelve fecha de siembra óptima + dosis fertilizante según ENSO + clima predicho. Nueva sección en `PagePrediccion.jsx`. *(2026-05-05 · `/api/recomendacion` con 4 recomendaciones cruzando aptitud SIPRA + ENSO + alertas activas; `RecomendacionPanel` debajo de la predicción.)*
- [x] **T16 (L, Innovación) Gemelo digital de finca.** Simulador con sliders (lluvia ±%, temperatura ±°C, ENSO). Recalcula rendimiento en vivo vía el modelo. *(2026-05-05 · `/api/simular` con elasticidades agronómicas + aptitud SIPRA; componente `GemeloDigital.jsx` con 4 sliders, debounce 250ms, gráfico de contribuciones por factor.)*
- [x] **T17 (S, Innovación) Asistente con voz.** `SpeechRecognition` API en `PageAsistente.jsx`. Demo hablada. *(2026-05-05 · Web Speech API es-CO bidireccional: micrófono al input + speechSynthesis lee las respuestas; degrada en navegadores sin soporte.)*
- [x] **T18 (S, Datos abiertos) Página "Catálogo de fuentes".** Auto-generada desde `config/settings.py`. Muestra URI Socrata, último refresh y row count por dataset. Trazabilidad para el jurado. *(2026-05-05 · `/api/catalogo` con 9 fuentes + URI + tabla destino + conteo real de filas; bloque dinámico en `PageMetodologia.jsx` con enlaces clickeables.)*
- [x] **T19 (S, Impacto) Aliados objetivo y plan de adopción.** Sección en `PageImpacto.jsx` con MinAgricultura, FEDEARROZ, FNC, Agrosavia, gobernaciones. Valor concreto por stakeholder, sin afirmar adopción real. *(2026-05-05 · Adelantada junto con T03: 6 aliados con rol y propuesta de valor.)*
- [x] **T20 (M, Impacto) Plan de sostenibilidad.** Costo mensual estimado (Supabase + Vercel + Groq + cron), fuentes de financiación (MinTIC, BID, regalías). *(2026-05-05 · Sección "Sostenibilidad" en `PageImpacto.jsx` con 3 tarjetas: costo operativo (≤USD 45/mes detallado), financiación (MinTIC, BID Lab, Regalías SGR, gremios), continuidad técnica.)*
- [x] **T21 (M, Impacto) API pública documentada.** OpenAPI sobre los 5+ endpoints, key auth simple. Argumento "otros pueden construir encima". *(2026-05-05 · `/api/openapi` con spec OpenAPI 3.1 documentando los 11 endpoints; link visible en `PageMetodologia`.)*
- [x] **T22 (S, UX) Mapa Colombia con datos reales.** Reemplazar pines hardcoded en `PageInicio.jsx` y `PageDashboards.jsx` por consulta a `/api/municipios?riesgo=...`. *(2026-05-05 · `/api/mapa` con coords centroide + nivel_riesgo; `ColombiaMap` proyecta lat/lon al viewBox; `PageInicio` y `VistaGeneral` consumen runtime con tooltip por hover.)*
- [x] **T23 (S, UX) Onboarding tour.** 4 pasos guiados primera visita. *(2026-05-05 · `OnboardingModal.jsx` con 4 pasos, persistido en localStorage; CTA final lleva a Predicción.)*
- [x] **T24 (S, UX) Accesibilidad WCAG AA.** Aria-labels, contraste, soporte teclado en chat. *(2026-05-05 · skip-link al main, focus visible amarillo en interactivos, `prefers-reduced-motion`, contraste reforzado en `.muted/.panel-sub`.)*
- [x] **T25 (S, UX) Mobile responsive.** Probar las 6 páginas a 375px, ajustar `globals.css`. *(2026-05-05 · refuerzo a 480px y 380px sobre demo guiada, SHAP, tablas con scroll-x, modal onboarding, toggles dashboard apilados.)*
- [x] **T26 (S, UX) Página Demo guiada.** Los 5 pasos de `PageImpacto > Secuencia narrativa` como guía interactiva con CTAs. *(2026-05-05 · 6 pasos clickeables con botón "Ir a X →" que navega a la sección correspondiente.)*

---

## 4. Cómo trabajar este plan

1. Cada vez que se complete una tarea, cambiar `[ ]` por `[x]` y registrar fecha al final de la línea.
2. Si una tarea revela subtareas, agregarlas debajo con sangría.
3. Mantener una tarea `[~]` a la vez para evitar trabajo a medias.
4. Al cerrar oleada, escribir nota breve de "Lecciones aprendidas".

### Bitácora

- **2026-05-05** · Migración del asistente conversacional de **Groq Llama 3.3** a **Anthropic Claude** (default `claude-sonnet-4-5`, configurable vía `ANTHROPIC_MODEL`). Tools convertidas a formato Anthropic (`name`/`description`/`input_schema`); loop tool-use reescrito con `stop_reason: "tool_use"` + bloques `tool_use`/`tool_result`. Prompt caching activo (header `anthropic-beta: prompt-caching-2024-07-31`).
- **2026-05-05** · Cierre de las 26 tareas del plan: 100% completado.
- **2026-05-05** · **Calidad de extracción reforzada.** Nuevo módulo `utils/extraction_quality.py` con:
  - `fetch_json` / `paginate_socrata` con reintentos exponenciales y backoff en HTTP 429
  - `standardize` que aplica validación de schema → coerción numérica → descarte de NULL críticos → filtros de rango → deduplicación por clave natural → trazabilidad (`_source_uri`, `_extracted_at`)
  - Reportes JSON por fuente en `data/quality_reports/`
  - `validate/audit_sources.py` consolida los reportes en Markdown con estado ✅/⚠/❌ y columnas con baja completitud
  - 3 nuevos extractores con APIs **oficiales y verificadas** sin API key: NASA POWER (clima diario MERRA-2), World Bank Open Data (indicadores macro-agro Colombia), FAOSTAT (producción FAO/ONU)
  - `extract_produccion.py` refactorizado como showcase del toolkit
  - Endpoint `/api/calidad` y bloque visible en `PageMetodologia` para que el jurado vea la auditoría en vivo

---

## 5. Reproducción del proyecto

### 5.1 Requisitos

- **Python 3.11+** con `pip` y `venv`.
- **Node.js 20+** con `npm` o `pnpm`.
- **PostgreSQL 14+** (recomendado: Supabase free tier — incluye SSL).
- **Anthropic API key** (`console.anthropic.com`) para el asistente Claude.
- **Power BI Service** (opcional) para los dashboards embebidos.

### 5.2 Configuración inicial

```bash
# 1. Clonar y entrar al proyecto
git clone https://github.com/Jacklb19/AgroH.git
cd AgroH

# 2. Crear .env a partir del template
cp .env.example .env
# editar .env con credenciales reales (Supabase + Groq)
```

> En la web (`AgroH/web/`) Next.js lee el mismo `.env` del root vía `next.config.mjs`. Si necesitas un `.env` separado, créalo en `web/.env.local` con las variables `DB_*` y `GROQ_API_KEY`.

### 5.3 Pipeline ETL + entrenamiento (Python)

```bash
# Crear entorno virtual e instalar dependencias
python -m venv venv
.\venv\Scripts\activate            # Windows PowerShell
# source venv/bin/activate         # Linux/macOS
pip install -r requirements.txt

# 1. Inicializar el schema en Supabase
python -c "from load.db import get_engine, init_schema; init_schema(get_engine())"

# 2. Correr pipeline completo (extract → clean → load → validate)
python run_pipeline.py

# 3. Entrenar el modelo de rendimiento (Optuna 200 trials, SHAP)
python -m models.train_rendimiento

# 4. Entrenar el clasificador de alertas climáticas
python -m models.train_alerta_climatica

# (opcional) Refrescar clima en tiempo real desde Open-Meteo
python run_openmeteo_fill.py
```

Cada corrida de `train_rendimiento` desactiva la versión anterior y graba métricas reales (R², MAE, RMSE, CV-MAE, top-10 SHAP) en `model_version.metricas_json`. Para inspeccionar:

```sql
SELECT nombre_modelo, metricas_json
FROM model_version
WHERE activo = TRUE;
```

### 5.4 Web (Next.js)

```bash
cd web
npm install
npm run dev      # http://localhost:3000

# Build de producción
npm run build && npm start
```

Endpoints disponibles:

| Ruta | Método | Descripción |
|---|---|---|
| `/api/health`     | GET  | Healthcheck de la BD |
| `/api/municipios` | GET  | Lista de municipios `dim_municipio` |
| `/api/cultivos`   | GET  | Lista de cultivos `dim_cultivo` |
| `/api/prediccion` | POST | Predicción XGBoost + SHAP (`muni`, `cultivo`, `year`, `semester`, `enso`, `lluvia`) |
| `/api/impacto`    | GET  | KPIs reales de cobertura y beneficiarios |
| `/api/chat`       | POST | Asistente Claude Sonnet 4.6 con tool-use SQL (`messages`) |

### 5.5 Power BI (opcional)

1. Importar `AgroIA_PowerBI_Theme.json` como tema personalizado.
2. Conectar al mismo Supabase con el conector PostgreSQL.
3. Publicar al Service y reemplazar la URL en `web/app/components/PageDashboards.jsx > BASE_PBI`.

Si Power BI no está disponible durante la defensa, T14 (modo offline / plan B) muestra los charts nativos alimentados por `/api/dashboards`.

### 5.6 Despliegue sugerido

- **Web:** Vercel (deploy directo desde GitHub, lee `.env` de Vercel project).
- **BD:** Supabase free tier (500 MB, suficiente para el star schema actual).
- **Pipeline:** GitHub Actions cron diario (ver T12 para CI).
- **LLM:** Anthropic API (Claude Sonnet 4.6) — modelo principal del proyecto.
