# Informe de cambios — Rediseño web, corrección de datos y modelo de pronóstico

**Proyecto:** AgroIA Colombia · **Rama:** `rediseno-web` (GitHub `Jacklb19/AgroH`) · **Base:** `main` en `9b0d2cc` · **Fecha:** 24 de septiembre de 2026

Este documento describe **todo** lo que se cambió, por qué y cómo trasladarlo a otro repositorio. Está pensado para que otra persona (o un asistente de código) pueda reproducir el trabajo sin haber estado en la sesión.

---

## Índice

1. [Resumen ejecutivo](#1-resumen-ejecutivo)
2. [Diagnóstico inicial: qué estaba mal](#2-diagnóstico-inicial-qué-estaba-mal)
3. [Commits de la rama](#3-commits-de-la-rama)
4. [Base de datos (Supabase)](#4-base-de-datos-supabase)
5. [Modelo de pronóstico](#5-modelo-de-pronóstico)
6. [Pipeline de Python y pruebas](#6-pipeline-de-python-y-pruebas)
7. [Web: arquitectura](#7-web-arquitectura)
8. [Web: página por página](#8-web-página-por-página)
9. [API: endpoint por endpoint](#9-api-endpoint-por-endpoint)
10. [Asistente conversacional](#10-asistente-conversacional)
11. [Robustez y rendimiento](#11-robustez-y-rendimiento)
12. [Verificación realizada](#12-verificación-realizada)
13. [Guía para trasladar todo al repositorio real](#13-guía-para-trasladar-todo-al-repositorio-real)
14. [Pendientes y recomendaciones](#14-pendientes-y-recomendaciones)
15. [Anexos](#15-anexos)

---

## 1. Resumen ejecutivo

| Área | Antes | Después |
|---|---|---|
| Estructura web | SPA con estado de React (sin URLs), 6 secciones | 5 páginas con rutas reales: `/`, `/prediccion`, `/datos`, `/asistente`, `/metodologia` |
| Datos mostrados | KPIs inventados, comparativo falso, "vista general ilustrativa", fórmula de respaldo que inventaba rendimientos | Todo sale de la BD; si no hay dato, se dice |
| Rendimiento histórico | 36 % de las filas infladas (se sumaban semestres) | Producción ÷ área cosechada; migración aplicada y pipeline corregido |
| Modelo | Solo había "predicciones" de 2019-2024 (años ya conocidos); el modelo no se guardaba | Modelo de pronóstico real para 2025-2027, con backtest, rango del 90 % y escenarios El Niño / La Niña |
| Métricas publicadas | "R² > 0,80, MAE < 0,5" (no correspondían a nada medido) | R² 0,893 y error típico 3,2 % en años no vistos; comparadas con métodos simples |
| Selector de periodo | Año 2026-2028 + semestre (incluía periodos pasados; el año no cambiaba el resultado) | "¿Cuándo vas a sembrar?": solo temporadas con ventana de siembra abierta y con pronóstico |
| Asistente | Consultaba tablas infladas; instrucciones le pedían inventar datos si faltaban | Usa el modelo nuevo, dice "no tengo datos", clima en vivo, reintenta ante límite de Groq |
| Economía | Oculta; mostraba un índice como si fueran pesos | Pestaña "Precios e insumos" con el índice bien interpretado y precios SIPSA |

---

## 2. Diagnóstico inicial: qué estaba mal

### 2.1 Contenido de la web
- **KPIs inventados** (`KpiStrip.jsx`): "42.8k predicciones generadas", "+34 % trimestre", "214 alertas activas", "+12 últimas 24h", con sparklines fijas.
- **Inicio**: gráfica "Maíz 4.8 t/ha, +12 %, R²=.81" y semáforo 18/31/51 % escritos a mano.
- **Dashboards**: la "Vista general" entera era ilustrativa (lo decía su propio aviso) y aparecía antes de los tableros reales.
- **Predicción**: "Comparativo regional" mostraba siempre Espinal, Guamo, Saldaña y Purificación (Tolima) con cualquier municipio. El mensaje de carga decía "1.500 árboles · inferencia distribuida" (era una consulta a la BD).
- **Cifras inconsistentes**: 85+ / 87 / >50 cultivos; "Cinco vistas" con 3 pestañas; "Claude Sonnet 4.6" mientras el código usaba otro modelo o Groq; guía "de cinco pasos" con seis.
- **Jerga técnica** visible: nombres de tablas (`dim_municipio`, `pred_alerta_climatica`…), consultas SQL en textos, "Modo offline · Plan B".
- **Faltaba** el planteamiento del problema, los hallazgos y las limitaciones; **sobraba** la "Guía para el jurado" y el onboarding con jerga.
- **Navegación sin URLs**: no se podía compartir un enlace ni usar "atrás".

### 2.2 Datos y modelo (descubierto al conectarse a Supabase)
- **Rendimiento inflado**: `load/load_facts.py` hacía `groupby(...).sum()` sobre todas las columnas, **incluida `rendimiento_t_ha`**, al unir semestres A/B y variedades. 32.571 de 91.551 filas quedaron infladas (arroz Ibagué 15,2 en lugar de 7,6 t/ha; maíz 20 en lugar de 5).
- **No había pronóstico**: `pred_rendimiento` solo tenía los valores ajustados de 2019-2024 (años con dato real). Elegir 2026, 2027 o 2028 devolvía siempre lo de 2024. El semestre no afectaba la predicción real.
- **Métricas reales del modelo anterior** (en `model_version`): R² 0,687 y MAE 7,1 t/ha, no "R² > 0,80, MAE < 0,5".
- El rango de confianza era ±MAE (±7,1 t/ha) para todos los cultivos, incluso para café (~1 t/ha).
- `pred_rendimiento` guardaba mes = 1 y la API de dashboards filtraba mes = 12: la serie "real vs predicho" salía vacía.
- No existía la columna `shap_top` en la BD desplegada: el panel "por qué" nunca podía mostrarse.
- `/api/recomendacion` consultaba `probabilidad_deficit_hidrico`, columna que no existe en la BD desplegada: fallaba en silencio.
- El esquema desplegado difiere de `load/schema.sql`: `build_features.py` no podría correr contra Supabase (`fact_alerta_enso` solo tiene `fase_enso` e `indice_oni`).
- **"Alertas activas"** no eran actuales: clasificación anual 2019-2024 de 119 municipios, modelo con exactitud 51 % y recall 0 en ALTO.
- **Precios de insumos**: los valores (~160) son un **índice** IPIA, no pesos; además había herbicidas clasificados como fertilizantes.
- **Precios mayoristas**: un solo mes (abril 2026), 33 productos, 4 centrales.
- **Rutas de API prerenderizadas**: `/api/mapa`, `/api/impacto`, `/api/dashboards`, `/api/health`… se generaban en el build y en producción podían quedar congeladas.

---

## 3. Commits de la rama

| Commit | Contenido |
|---|---|
| `4aa8775` | Rediseño de la web: rutas reales, nuevo Inicio, Predicción, Explorar datos, Asistente, Cómo funciona; eliminación de contenido falso; CSS reescrito. Integra los 13 commits de `main` (Groq, botón "Escuchar", vecinos, Economía). |
| `58f9de3` | Modelo de pronóstico, corrección de datos, APIs conectadas al modelo, asistente reescrito, pruebas. |
| `9d61b4e` | Migración SQL 002 (rendimiento + vista de Power BI) y su reversión. |
| (este informe) | `docs/INFORME_REDISENO.md` |

---

## 4. Base de datos (Supabase)

> **Todo lo que se escribió en la BD fue aditivo** (tablas y filas nuevas), excepto la migración 002, que **ejecutó el usuario** desde el SQL Editor con respaldo previo.

### 4.1 Objetos nuevos

**Tabla `pred_pronostico`** (la crea `models/train_pronostico.py --write` si no existe):

```sql
CREATE TABLE IF NOT EXISTS pred_pronostico (
    id                   BIGSERIAL PRIMARY KEY,
    id_version           INT NOT NULL REFERENCES model_version(id_version),
    id_municipio         CHAR(5) NOT NULL REFERENCES dim_municipio(id_municipio),
    id_cultivo           INT NOT NULL REFERENCES dim_cultivo(id_cultivo),
    anio                 SMALLINT NOT NULL,
    tipo                 VARCHAR(12) NOT NULL CHECK (tipo IN ('backtest', 'pronostico')),
    escenario            VARCHAR(10) NOT NULL,          -- 'Real' (backtest), 'Neutral', 'El Niño', 'La Niña'
    rendimiento_predicho DOUBLE PRECISION NOT NULL,
    limite_inferior      DOUBLE PRECISION,               -- rango probable 90 %
    limite_superior      DOUBLE PRECISION,
    rendimiento_real     DOUBLE PRECISION,               -- solo en backtest
    shap_top             JSONB,                          -- top-3 factores, solo en pronóstico
    UNIQUE (id_version, id_municipio, id_cultivo, anio, escenario)
);
CREATE INDEX IF NOT EXISTS idx_pred_pronostico_consulta
    ON pred_pronostico (id_version, id_municipio, id_cultivo, anio);
```

**Filas en `model_version`:**

| id_version | nombre_modelo | activo | Nota |
|---|---|---|---|
| 1 | xgboost_rendimiento | true | Modelo anterior (sin cambios). La web ya no lo usa. |
| 2 | xgboost_alerta_climatica | true | Modelo anterior de alertas (sin cambios). Experimental. |
| 3 | xgboost_pronostico | **false** | Primera versión del pronóstico. |
| 4 | xgboost_pronostico | **true** | Versión activa (con limpieza de errores de área). |

**Filas en `pred_pronostico`:**

| Versión | backtest | pronóstico |
|---|---|---|
| 3 | 43.749 | 122.248 |
| 4 | 42.985 | 122.248 |

Pronóstico = 17.464 combinaciones × (2025 Neutral + 2026 × 3 escenarios + 2027 × 3 escenarios).

### 4.2 Migración 002 (aplicada el 24-09-2026, 07:00 UTC)

Archivos: `load/migrations/002_corregir_rendimiento.sql` y `load/migrations/002_revertir.sql`.

1. `CREATE TABLE backup_002_fact_produccion_agricola AS TABLE fact_produccion_agricola` (con RLS activado al ejecutarla).
2. `rendimiento_t_ha = produccion_total_ton / area_cosechada_ha` donde ambos > 0; `NULL` en el resto (3.401 filas).
3. `CREATE OR REPLACE VIEW v_predicciones_modelo` leyendo `pred_pronostico` (versión activa), con **las mismas columnas y tipos** que la vista anterior (no rompe Power BI). Años 2022-2024 = backtest con valor real; años siguientes = pronóstico neutral.
4. `INSERT INTO schema_migrations (2, …)`; el script se niega a correr dos veces.

Estado verificado: `schema_migrations` tiene la versión 2; arroz en Ibagué promedio 7,70 t/ha.

`v_dashboard_agro` no se tocó: lee `rendimiento_t_ha`, que quedó corregido.

### 4.3 Permisos
El rol `anon` no tiene privilegios sobre ninguna tabla ni vista del esquema `public` (verificado). La web y el pipeline se conectan como `postgres` (dueño de las tablas), por lo que RLS no les afecta.

---

## 5. Modelo de pronóstico

**Archivo:** `models/train_pronostico.py` · **Documentación corta:** `docs/modelo_pronostico.md`

### 5.1 Qué predice
Rendimiento (t/ha cosechada) de un cultivo en un municipio para los años **siguientes** al último dato de producción, con rango probable del 90 % y escenarios El Niño / La Niña.

### 5.2 Datos y limpieza
- Objetivo: `produccion_total_ton / area_cosechada_ha` de `fact_produccion_agricola` (2019-2024, 93.486 filas, 90.085 calculables).
- Limpieza (`limpiar_objetivo`):
  1. Rendimientos a más de 3,5 desviaciones robustas (MAD, escala log) dentro del cultivo → descartados.
  2. Rendimientos > 500 t/ha → descartados.
  3. **Error de reporte de área**: años con área < 40 % del máximo de la combinación **y** rendimiento > 2,5 × el de sus años normales (área ≥ 50 % del máximo) → descartados. Ejemplo: arroz en Puerto Concordia 2022-2024 (70 ha y 37 t/ha frente a 600+ ha y 5-7 t/ha).
- Resultado: 87.662 filas válidas.

### 5.3 Variables (solo información conocida antes de la cosecha)
`y_lag1`, `y_lag2`, `y_hist_mean`, `y_hist_std`, `n_prev`, `y_trend`, `area_lag1_log`, `crop_nat_lag1`, `crop_dept_prior`, `crop_nat_prior`, `id_cultivo`, `dept_code`, `id_region`, `lat`, `lon`, `permanente`, `aptitud` (UPRA), `clim_lluvia`, `clim_temp` (climatología del municipio), `oni` (escenario), `oni_lag1`.

Etiquetas legibles para la web en `web/lib/labels.js`.

### 5.4 Formulación
- **Punto de partida**: rendimiento del año anterior (o promedio histórico si falta).
- XGBoost aprende el **cambio logarítmico** `log(y / base)` con **pérdida absoluta** (`reg:absoluteerror`, `base_score=0`).
- El cambio se **atenúa** con `ALPHA = 0.5`: `yhat = base × exp(0.5 × predicción)`.
- Hiperparámetros: `n_estimators=1200, max_depth=7, learning_rate=0.02, subsample=0.8, colsample_bytree=0.7, min_child_weight=8`.
- **Pronóstico recursivo**: el pronóstico neutral de un año alimenta los rezagos del siguiente.
- **Escenarios**: ONI = 0 (Neutral), +1 (El Niño), −1 (La Niña). Años ya cerrados (2025) usan el ONI real.
- **Rango del 90 %**: cuantiles 5 % y 95 % del error logarítmico del backtest **por cultivo** (global si el cultivo tiene < 40 casos).
- **Explicación por predicción**: `pred_contribs` de XGBoost (equivalente a SHAP, sin depender del paquete `shap`), top-3 factores convertidos a t/ha aproximadas.

### 5.5 Validación y métricas (versión 4)
Backtest de origen móvil: para 2022, 2023 y 2024 se entrena solo con años anteriores.

| Método | n | R² | MAE (t/ha) | RMSE (t/ha) | Error relativo mediano |
|---|---|---|---|---|---|
| **Modelo AgroIA** (todas las filas) | 42.985 | **0,893** | 1,51 | 5,29 | 3,2 % |
| Modelo AgroIA (mismas filas que la referencia) | 42.090 | **0,897** | 1,45 | **5,22** | 3,0 % |
| Repetir el año anterior | 42.090 | 0,889 | **1,41** | 5,40 | 0,2 % |
| Promedio histórico | 42.985 | 0,854 | 2,41 | 6,17 | 12,6 % |

- Cobertura del rango del 90 %: **89,8 %** (bien calibrado).
- Importancia (top-10): `crop_nat_prior` 0,140 · `crop_nat_lag1` 0,078 · `permanente` 0,064 · `y_trend` 0,048 · `y_lag1` 0,047 · `id_cultivo` 0,046 · `aptitud` 0,045 · `oni_lag1` 0,045 · `crop_dept_prior` 0,044 · `oni` 0,044.

**Lectura honesta:** muchos municipios reportan el mismo rendimiento varios años seguidos, por lo que "repetir el año anterior" es casi imbatible en error medio. El modelo lo supera en R² y en errores grandes (RMSE) y aporta rango y escenarios. Con 6 años de datos, el efecto de El Niño / La Niña sobre el rendimiento es pequeño: los escenarios suelen diferir < 2 %.

### 5.6 Experimentos descartados (para no repetirlos)
- Objetivo en nivel log con pérdida cuadrática: R² 0,86 pero error relativo mediano 11 % (peor que la referencia).
- Cambio con pérdida cuadrática o Huber: peor que con pérdida absoluta.
- Variables de "repetición" (tasa de años con cifra idéntica): no mejoraron.
- Atenuación α ∈ {0 … 1}: 0,5 equilibra error medio y RMSE.
- Filtros por múltiplos de la mediana del cultivo: descartan maíz tecnificado legítimo (10-12 t/ha frente a mediana 2).
- No se usan `optuna` ni `shap` (no instalados y no necesarios).

### 5.7 Cómo reentrenar
```bash
# Variables: SUPABASE_DB_HOST, SUPABASE_DB_PORT, SUPABASE_DB_NAME, SUPABASE_DB_USER, SUPABASE_DB_PASSWORD
python -m models.train_pronostico            # entrena y evalúa, no escribe (≈ 5 min)
python -m models.train_pronostico --write    # nueva versión en model_version + filas en pred_pronostico
```
Cada `--write` desactiva la versión anterior de `xgboost_pronostico` y no borra nada. También corre dentro de `run_pipeline.py` (paso de modelos).

---

## 6. Pipeline de Python y pruebas

| Archivo | Cambio |
|---|---|
| `load/load_facts.py` | Al agrupar filas de un mismo municipio × cultivo × año, suma área y producción y **recalcula** `rendimiento_t_ha = producción / área cosechada` (NULL si no es calculable). Ya no suma el rendimiento. |
| `run_pipeline.py` | `run_models` ejecuta también `train_and_forecast(write=True)`. |
| `models/train_pronostico.py` | Nuevo (sección 5). |
| `tests/test_pronostico.py` | Nuevo: 5 pruebas (sin fugas de información futura, sin historia no hay pronóstico, limpieza MAD, error de área tipo Puerto Concordia, el cargue no suma rendimientos). |
| `README.md` | Métricas reales de la v4, cobertura real, interpretación y nota de la corrección de datos. |
| `docs/modelo_pronostico.md` | Nuevo: documentación del modelo. |
| `.gitignore` | Añade `.vercel`. |

Dependencias Python: no se añadió ninguna (usa `pandas`, `numpy`, `xgboost`, `sqlalchemy`, `psycopg2`, ya en `requirements.txt`). Para correr las pruebas: `pip install pytest`.

---

## 7. Web: arquitectura

### 7.1 Rutas (Next.js App Router)
| Ruta | Archivo | Componente |
|---|---|---|
| `/` | `web/app/page.js` | `PageInicio` |
| `/prediccion` | `web/app/prediccion/page.js` | `PagePrediccion` |
| `/datos` | `web/app/datos/page.js` | `PageDatos` |
| `/asistente` | `web/app/asistente/page.js` | `PageAsistente` |
| `/metodologia` | `web/app/metodologia/page.js` | `PageMetodologia` |

- Cada `page.js` es de servidor y exporta `metadata` (título y descripción por página); `layout.js` define la plantilla de título `%s · AgroIA Colombia`.
- `layout.js` monta `Nav` y `Footer` una sola vez.
- `next.config.mjs`: redirecciones permanentes `/dashboards → /datos` e `/impacto → /`.
- `web/app/icon.svg`: favicon (brote verde). Ojo: el `.gitignore` raíz ignora `*.svg`; se añadió con `git add -f`.

### 7.2 Componentes
**Eliminados:** `Hero.jsx`, `KpiStrip.jsx`, `OnboardingModal.jsx`, `PageImpacto.jsx`, `PageDashboards.jsx`, `PageEconomia.jsx`, `charts/Sparkline.jsx`, `charts/KpiSpark.jsx`, `charts/Donut.jsx`, `charts/DualLineChart.jsx`.

**Nuevos:**
| Archivo | Función |
|---|---|
| `ui.jsx` | `SectionHead`, `Source` (línea "Fuente: …"), `RiskBadge` |
| `PageDatos.jsx` | Página "Explorar datos" (reemplaza Dashboards) |
| `PreciosInsumos.jsx` | Pestaña "Precios e insumos" |
| `TourOverlay.jsx` | Guía paso a paso (extraída de Predicción) |
| `charts/SerieChart.jsx` | Serie anual: real, backtest (puntos huecos), pronóstico con banda |
| `charts/useAncho.js` | Hook que mide el ancho real del contenedor (gráficos responsivos) |

**Reescritos:** `Nav.jsx` (Link + `usePathname`), `Footer.jsx` (enlaces reales a GitHub, docs, API y fuentes), `PageInicio.jsx`, `PagePrediccion.jsx`, `PageAsistente.jsx`, `PageMetodologia.jsx`, `GemeloDigital.jsx`, `icons.js` (set único con `size` configurable), `charts/ColombiaMap.jsx` (contorno real de Colombia, proyección equirectangular con misma escala, 1.100 puntos), `charts/ConfidenceBar.jsx`, `charts/HBars.jsx`.

### 7.3 Librerías propias (`web/lib`)
| Archivo | Contenido |
|---|---|
| `content.js` | Textos editoriales: problema, pruebas ANOVA, para quién, fuentes, limitaciones, trabajo futuro, URLs del repo |
| `format.js` | `fmtNum`, `fmtCompact`, `signed`, `SEMESTRE`, `temporadasDisponibles()` |
| `labels.js` | Traducción de variables del modelo a lenguaje claro |
| `modelo.js` | `VERSION_ACTIVA` (subconsulta), `RENDIMIENTO_REAL` (expresión SQL), `titulo()` (nombres en mayúsculas → "San José de Cúcuta"), `variabilidad()`, `errorBD()` |
| `db.js` | Pool `pg` (máx. 5, espera de conexión 20 s) con reintento ante errores de conexión |

### 7.4 Estilos
`web/public/styles.css` reescrito completo (~1.100 líneas): se conservan los tokens de color y tipografías (Inter, Inter Tight, JetBrains Mono); se eliminó código muerto (tutorial, placeholders, KPIs, donut, semáforo); tonos reutilizables `.green/.blue/.amber/.violet` vía variables `--tone`; responsive en 1100/900/640 px; `min-width: 0` en celdas de grid para que los gráficos no desborden.

---

## 8. Web: página por página

### 8.1 Inicio (`/`)
1. **Hero** con cifras reales (`/api/resumen`: 1.102 municipios con pronóstico, 163 cultivos, 17.464 pronósticos, cosechas 2019-24) y panel **"Pronóstico de cosecha {año}"**: mapa de Colombia con cada municipio coloreado por cambio esperado frente a su último año (`/api/mapa`).
2. **El problema**: 3 tarjetas (clima, datos dispersos, decisiones sin herramientas).
3. **Cuatro formas de usar AgroIA** (enlaces a cada página).
4. **Hallazgos ANOVA** (4 tarjetas, p < 0,001) con enlace a la evidencia.
5. **Cómo funciona** en 3 pasos + tira de fuentes.
6. **Para quién** (productores, gremios/crédito, gobiernos locales, investigación).
7. Llamado final a probar una predicción.

### 8.2 Predicción (`/prediccion`)
- **Formulario:** Departamento → Municipio (1.102, desde `/api/municipios`) → Cultivo (solo los que tienen pronóstico en ese municipio, `/api/cultivos?muni=`) → **"¿Cuándo vas a sembrar?"** (`temporadasDisponibles`: ventana A hasta abril, B hasta octubre; solo años con pronóstico por escenarios en `/api/modelo`). Clima en vivo del municipio (`/api/clima/actual?id=`).
- **Resultado** (`/api/prediccion`): rendimiento esperado, barra de rango del 90 %, frase en lenguaje simple (cambio frente al último año real), variabilidad histórica (coeficiente de variación → baja/media/alta), error típico del modelo en ese cultivo, último dato real, **gráfico de historia** (real + lo que el modelo habría predicho + pronóstico), **escenarios** La Niña / normal / El Niño (con aviso si difieren < 3 %), **"Por qué este resultado"** (top-3 factores).
- **Siguientes pasos** (pestañas): *Qué hacer* (`/api/recomendacion`: calendario típico, aptitud UPRA, fase ENSO actual), *Comparar con la región* (`/api/comparativo`), *¿Y si cambia el clima?* (simulador `/api/simular`, parte de la predicción mostrada; aproximación orientativa).
- Sin datos → mensaje "Sin pronóstico para esta combinación". BD caída → mensaje de servicio no disponible (ya no se inventan números).
- Guía paso a paso (`TourOverlay`), cierra con Escape.

### 8.3 Explorar datos (`/datos`)
Pestañas:
1. **Resumen**: "¿Qué tan bien predice el modelo?" (R², error típico, cobertura y barras de RMSE frente a métodos simples, desde `/api/modelo`); "El Niño / La Niña hoy" (última fase ONI de NOAA); **"Explora un cultivo"** (12 cultivos principales; serie nacional; cuántos municipios suben/estables/bajan; mayor rendimiento esperado con filtro de plausibilidad P95), desde `/api/cultivo`.
2. **Precios e insumos**: índice IPIA mensual de fertilizantes vs agroquímicos con el máximo marcado; KPIs (subida hasta el pico, índice actual, cambio frente a 2018); tabla por insumo (cambio 12 meses, frente a su máximo); precios mayoristas SIPSA de abril 2026 por producto (`/api/economia`).
3. **Panorama general / Producción / Clima y alertas**: los 3 tableros Power BI (enlace público existente) con guía "Qué encontrarás", aviso de que se actualizan por separado y estado de carga.

### 8.4 Asistente (`/asistente`)
- Barra lateral: "Qué puede hacer" y "Ten en cuenta" (uso responsable).
- Estado vacío con preguntas de ejemplo agrupadas (Rendimiento, Riesgo y clima, Escenarios, Qué sembrar).
- Respuestas con formato (negritas, listas), botón **Escuchar** por mensaje (aporte del equipo), dictado por micrófono, aviso específico si el servicio está saturado.

### 8.5 Cómo funciona (`/metodologia`)
Proceso en 4 pasos · El modelo (qué información usa, qué pesa más —dinámico—, "sin caja negra", tabla de precisión frente a métodos simples, versión y fecha de entrenamiento) · Evidencia ANOVA (4 pruebas con boxplots y estadísticas plegables) · Fuentes (14, con estado "N registros" o "Sin cargar") · Limitaciones (6) + Próximos pasos + **Transparencia** (corrección del 36 % de rendimientos; modelo de alertas experimental) · Abierto y sostenible · Detalles técnicos plegables (arquitectura, recursos, comandos para reproducir, calidad por fuente).

---

## 9. API: endpoint por endpoint

| Endpoint | Estado | Entrada | Salida / fuente | Caché |
|---|---|---|---|---|
| `GET /api/health` | modificado | — | `SELECT NOW()` | dinámico |
| `GET /api/modelo` | **nuevo** | — | versión activa, `metricas_json`, años con pronóstico y escenarios, fase ENSO más reciente, métricas del modelo de alertas | revalidate 300 s |
| `GET /api/resumen` | **nuevo** | — | municipios, cultivos, combinaciones, años de producción | 3600 s |
| `GET /api/municipios` | reescrito | — | `[{id, nombre, departamento}]` solo con pronóstico; nombres con `titulo()` | 3600 s |
| `GET /api/cultivos` | reescrito | `?muni=` | `[{id, nombre, ciclo}]` con pronóstico (en ese municipio) | dinámico |
| `POST /api/prediccion` | reescrito | `{id_municipio, id_cultivo, anio}` | pronóstico neutral, escenarios, historia real + backtest, último real, variabilidad, precisión del cultivo, `shap`; `sin_datos` si no hay; 503 si la BD falla | — |
| `GET /api/comparativo` | reescrito | `?muni&cultivo&anio` | ranking del departamento (top 6 + el consultado), cambio frente a su último año | dinámico |
| `GET /api/cultivo` | **nuevo** | `?id&anio` (sin id: top 12 cultivos) | serie nacional (mediana real, backtest, pronóstico con banda), top 8 municipios (≥ 3 años, ≤ P95 del cultivo), sube/estable/baja | dinámico |
| `GET /api/mapa` | reescrito | `?anio` | por municipio: cambio mediano esperado frente al último real, tendencia (±3 %), resumen | 3600 s |
| `POST /api/recomendacion` | reescrito | `{id_municipio, id_cultivo, semestre}` | calendario típico (cultivos transitorios conocidos), aptitud UPRA, fase ENSO NOAA | — |
| `POST /api/simular` | modificado | + `baseline` opcional | si llega `baseline`, parte de él y no vuelve a sumar aptitud | — |
| `GET /api/economia` | reescrito | — | índice IPIA mensual (fertilizantes/agroquímicos), insumos con nombres legibles, precios SIPSA del último mes | 3600 s |
| `GET /api/catalogo` | modificado | — | 14 fuentes con conteo exacto (2 consultas: tablas existentes + `UNION ALL`) | 300 s |
| `GET /api/clima/actual` | modificado | `?id=` (o `?municipio=`) | busca por código DIVIPOLA; nombres con `titulo()` | caché en memoria 15 min |
| `POST /api/chat` | modificado | — | ver sección 10 | — |
| `GET /api/openapi` | reescrito | — | especificación OpenAPI 3.1 actualizada (v2.0.0) | 300 s |
| `GET /api/anova`, `/api/calidad` | + revalidate | — | sin cambios de lógica | 300 s |
| `GET /api/impacto` | **eliminado** | — | reemplazado por `/api/resumen` | — |
| `GET /api/dashboards` | **eliminado** | — | reemplazado por `/api/modelo` + `/api/cultivo` | — |

Todas las consultas del pronóstico usan `VERSION_ACTIVA` y `RENDIMIENTO_REAL` de `web/lib/modelo.js`.

---

## 10. Asistente conversacional

Archivo: `web/app/api/chat/route.js` (sobre la versión con Groq del equipo).

- **Instrucciones (system prompt):** describen la BD real; prohíben inventar cifras ("usa SOLO cifras que aparezcan en los resultados", "menciona siempre el año"); si falta un dato, decirlo; aclarar que el riesgo ENSO es histórico; no afirmar "sin riesgo" sin datos; no comparar rendimientos entre cultivos distintos. Se eliminó la instrucción anterior que pedía responder con conocimiento general "sin mencionar que no hay información".
- **Herramientas reescritas** sobre `pred_pronostico` y el rendimiento corregido: `buscar_prediccion`, `top_rendimiento` (con filtro P95), `resumen_general`, `comparar_municipios`, `proyectar_escenario` (tres escenarios reales del modelo), `recomendar_cultivo` (lo más sembrado, variabilidad, pronóstico con rango, aptitud UPRA), `buscar_clima` (**clima en vivo Open-Meteo** + histórico IDEAM rotulado con fecha), `listar_alertas` (rotulada como histórica/experimental).
- **Búsqueda sin tildes** (`translate(lower(...))`) y **resolución de cultivo** (coincidencia exacta primero: "papa" ≠ "Papaya").
- **Límite de Groq (plan gratuito ~8.000 tokens/min):** reintento automático esperando lo que indica Groq (≤ 15 s, 2 veces); resultados de herramientas recortados a 3.500 caracteres; solo los últimos 8 mensajes del historial; si aún así se alcanza el límite, responde 429 `{ocupado: true}` y la página muestra "El asistente está recibiendo muchas consultas…".

---

## 11. Robustez y rendimiento

- `web/lib/db.js`: pool máx. 5, `connectionTimeoutMillis` 20 s, **reintento** ante `timeout exceeded when trying to connect`, `ECONNRESET`, `ENOTFOUND`, etc. (primer arranque en frío del pooler de Supabase tardaba > 8 s).
- `revalidate` en rutas GET para que no queden congeladas en el build; `/api/health` dinámico.
- `/api/catalogo`: de 14 `COUNT(*)` en paralelo (4,2 s, saturaba el pool) a 2 consultas (≈ 1 s).
- Gráficos con ancho medido (`useAncho`) en vez de escalar el SVG: texto legible en móvil.

---

## 12. Verificación realizada

| Prueba | Resultado |
|---|---|
| `python -m pytest tests -q` | **16 pasan** |
| `npm run test:api` (15 comprobaciones, contra la BD real) | **15/15**, dos corridas seguidas, incluido arranque en frío |
| Navegador (Playwright + Edge), 1440 px y 390 px: 5 páginas, flujo completo de predicción (Ibagué/arroz y Pasto/papa), 3 pestañas de siguientes pasos, pestañas de datos, redirecciones, tour | Sin errores de consola ni de red |
| Asistente (Groq) | Respuestas con datos reales; "no tengo datos" cuando corresponde; clima en vivo; aptitud verificada contra la BD |
| `npm run build` | Compila sin errores |
| Vista previa de Vercel | Desplegada (`Ready`) pero **no probada**: tiene protección de despliegue (requiere sesión de Vercel) |

`web/scripts/test-api.mjs` puede apuntar a cualquier URL: `BASE_URL=https://… npm run test:api`.

---

## 13. Guía para trasladar todo al repositorio real

### 13.1 Qué hay que trasladar
1. **Código** (72 archivos, lista completa en el anexo 15.1).
2. **Base de datos**: tabla `pred_pronostico` + filas del modelo; migración 002.
3. **Variables de entorno**.

### 13.2 Código — elige una opción

**Opción A — el repositorio real comparte historia con `Jacklb19/AgroH`** (su `main` contiene `9b0d2cc`):
```bash
git remote add agroh https://github.com/Jacklb19/AgroH.git
git fetch agroh rediseno-web
git checkout -b rediseno-web agroh/rediseno-web     # o: git merge agroh/rediseno-web
```

**Opción B — historia distinta, pero mismo código de partida:** usar el parche entregado junto a este informe (`agroia-rediseno.patch`, los commits desde `9b0d2cc`):
```bash
git checkout -b rediseno-web
git am --3way agroia-rediseno.patch       # aplica los commits con su mensaje
# si hay conflictos: resolver, git add <archivos>, git am --continue
```

**Opción C — código muy distinto:** copiar los archivos del anexo 15.1 respetando la columna Acción (A = crear, M = reemplazar, D = borrar), y revisar a mano los modificados que el otro repo haya cambiado (sobre todo `chat/route.js`, `PageAsistente.jsx`, `styles.css`).

Después de cualquiera de las opciones:
- `git add -f web/app/icon.svg` si el `.gitignore` ignora `*.svg`.
- No hay dependencias npm nuevas. `cd web && npm install`.

### 13.3 Base de datos
- **Si el repositorio real usa la misma Supabase** (proyecto Vercel `agro-h2`): **no hay que hacer nada**. La tabla, las versiones 3 y 4 y la migración 002 ya están aplicadas.
- **Si usa otra base de datos:**
  1. Configurar `SUPABASE_DB_*` y ejecutar `python -m models.train_pronostico --write` (crea `pred_pronostico` y la versión activa).
  2. Ejecutar `load/migrations/002_corregir_rendimiento.sql` en el SQL Editor (elegir "Run and enable RLS").
  3. Verificar: `SELECT * FROM schema_migrations;` debe mostrar la versión 2.

### 13.4 Variables de entorno
| Variable | Dónde | Uso |
|---|---|---|
| `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` | Vercel (Production y Preview) / `web/.env.local` | Web |
| `GROQ_API_KEY` (+ `GROQ_MODEL` opcional) | Vercel / `web/.env.local` | Asistente |
| `ANTHROPIC_API_KEY` (+ `ANTHROPIC_MODEL`) | Opcional; si existe, el chat usa Claude en lugar de Groq | Asistente |
| `SUPABASE_DB_HOST`, `SUPABASE_DB_PORT`, `SUPABASE_DB_NAME`, `SUPABASE_DB_USER`, `SUPABASE_DB_PASSWORD` | `.env` raíz (no se sube) | Pipeline Python |

`web/.env.local` y `.vercel/` están en `.gitignore`. Para obtener las variables de producción: `npx vercel link --project agro-h2` y `npx vercel env pull web/.env.local --environment=production` (las marcadas como *Sensitive*, como `GROQ_API_KEY`, no se descargan: agregarlas a mano).

### 13.5 Verificación tras trasladar
```bash
python -m pytest tests -q                  # 16 pruebas
cd web && npm run dev                      # http://localhost:3000
npm run test:api                           # 15 comprobaciones contra la BD
npm run build                              # debe compilar sin errores
```
Checklist manual:
- [ ] Inicio muestra 1.102 municipios y el mapa de pronóstico.
- [ ] Predicción: Tolima → Ibagué → Arroz → 2.º semestre 2026 ≈ 7,6 t/ha.
- [ ] Nariño → Pasto → Papa ≈ 21 t/ha; escenarios casi iguales con aviso.
- [ ] Explorar datos → Resumen: R² ≈ 0,89; Precios e insumos: pico de fertilizantes en 2022.
- [ ] Asistente responde "¿Qué pasa si hay El Niño en Pasto con papa?" con cifras de la BD.
- [ ] `/dashboards` redirige a `/datos`; `/impacto` a `/`.

---

## 14. Pendientes y recomendaciones

1. **Power BI**: el reporte vive en Power BI Service (cuenta `@campusucc.edu.co`, enlace "Publicar en la web"); el `.pbix` no está en el repositorio. Tras la migración 002 hay que **actualizar el modelo semántico** (app.powerbi.com → Mi área de trabajo → modelo semántico → Actualizar ahora; si pide credenciales, configurar las de PostgreSQL). Conviene activar la actualización programada y guardar el `.pbix` en el repositorio.
2. **Vista previa de Vercel**: probarla con sesión iniciada (o `BASE_URL=… npm run test:api` si se desactiva la protección).
3. **Límite de Groq**: el plan gratuito admite ~2-3 preguntas por minuto. Para uso público, subir de plan o usar Anthropic.
4. **Datos**: cargar producción 2007-2018 (permitiría medir el efecto de El Niño); clima NASA POWER para todos los municipios; modelo por semestre para transitorios.
5. **Limpieza opcional de la BD**: `pred_rendimiento` y la versión 1 del modelo ya no se usan en la web; las filas de la versión 3 de `pred_pronostico` pueden borrarse; `backup_002_fact_produccion_agricola` puede eliminarse cuando se confirme que todo está bien.
6. **Esquema**: `load/schema.sql` no coincide con la BD desplegada; `models/build_features.py` y `train_rendimiento.py` no corren contra ella. Recomendado sincronizar el esquema o retirar esos scripts.
7. **Modelo de alertas** (v2): exactitud 51 %, recall 0 en ALTO; no se usa en pronósticos. Reentrenar o retirar.

---

## 15. Anexos

### 15.1 Inventario de archivos (desde `9b0d2cc`)
A = añadido · M = modificado · D = eliminado

```
M  .gitignore
M  README.md
A  docs/modelo_pronostico.md
A  docs/INFORME_REDISENO.md
M  load/load_facts.py
A  load/migrations/002_corregir_rendimiento.sql
A  load/migrations/002_revertir.sql
A  models/train_pronostico.py
M  run_pipeline.py
A  tests/test_pronostico.py
M  web/app/api/anova/route.js
M  web/app/api/calidad/route.js
M  web/app/api/catalogo/route.js
M  web/app/api/chat/route.js
M  web/app/api/clima/actual/route.js
A  web/app/api/comparativo/route.js
A  web/app/api/cultivo/route.js
M  web/app/api/cultivos/route.js
D  web/app/api/dashboards/route.js
M  web/app/api/economia/route.js
M  web/app/api/health/route.js
D  web/app/api/impacto/route.js
M  web/app/api/mapa/route.js
A  web/app/api/modelo/route.js
M  web/app/api/municipios/route.js
M  web/app/api/openapi/route.js
M  web/app/api/prediccion/route.js
M  web/app/api/recomendacion/route.js
A  web/app/api/resumen/route.js
M  web/app/api/simular/route.js
A  web/app/asistente/page.js
M  web/app/components/Footer.jsx
M  web/app/components/GemeloDigital.jsx
D  web/app/components/Hero.jsx
D  web/app/components/KpiStrip.jsx
M  web/app/components/Nav.jsx
D  web/app/components/OnboardingModal.jsx
M  web/app/components/PageAsistente.jsx
D  web/app/components/PageDashboards.jsx
A  web/app/components/PageDatos.jsx
D  web/app/components/PageEconomia.jsx
D  web/app/components/PageImpacto.jsx
M  web/app/components/PageInicio.jsx
M  web/app/components/PageMetodologia.jsx
M  web/app/components/PagePrediccion.jsx
A  web/app/components/PreciosInsumos.jsx
A  web/app/components/TourOverlay.jsx
M  web/app/components/charts/ColombiaMap.jsx
M  web/app/components/charts/ConfidenceBar.jsx
D  web/app/components/charts/Donut.jsx
D  web/app/components/charts/DualLineChart.jsx
M  web/app/components/charts/HBars.jsx
D  web/app/components/charts/KpiSpark.jsx
A  web/app/components/charts/SerieChart.jsx
D  web/app/components/charts/Sparkline.jsx
A  web/app/components/charts/useAncho.js
M  web/app/components/icons.js
A  web/app/components/ui.jsx
A  web/app/datos/page.js
A  web/app/icon.svg
M  web/app/layout.js
A  web/app/metodologia/page.js
M  web/app/page.js
A  web/app/prediccion/page.js
A  web/lib/content.js
M  web/lib/db.js
A  web/lib/format.js
A  web/lib/labels.js
A  web/lib/modelo.js
M  web/next.config.mjs
M  web/package.json            (script "test:api")
M  web/public/styles.css
A  web/scripts/test-api.mjs
```

### 15.2 Estado de la BD al cierre (24-09-2026)
| Objeto | Filas |
|---|---|
| `fact_produccion_agricola` | 93.486 (rendimiento corregido) |
| `backup_002_fact_produccion_agricola` | 93.486 (RLS activo) |
| `pred_pronostico` | 331.230 (v3: 165.997 · v4: 165.233) |
| `model_version` | 4 (activa de pronóstico: v4) |
| `schema_migrations` | versiones 0, 1, 2 |

### 15.3 Decisiones de diseño relevantes
- Paleta: verde (principal/favorable), ámbar (atención), rojo (solo alerta), azul (datos secundarios). Tipografías Inter / Inter Tight / JetBrains Mono.
- Rankings de "mejores municipios": ≥ 3 años de datos y pronóstico ≤ percentil 95 del cultivo (los extremos suelen ser errores de reporte).
- Tendencia del mapa: ±3 % frente al último año; "sube/baja" en Explorar un cultivo: ±5 %.
- Temporadas: ventana de siembra A hasta abril, B hasta octubre.
- El simulador "¿Y si…?" usa elasticidades agronómicas genéricas: se presenta como aproximación orientativa, no como predicción del modelo.
