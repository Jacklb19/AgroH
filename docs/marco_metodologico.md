# Marco Metodológico — AgroIA Colombia

> Para profundizar en los detalles teóricos, metodológicos y de diseño de la solución, puedes consultar los siguientes accesos:
>
> **[Informe Técnico de la Solución (PDF)](documentation/informe_tecnico.pdf)** — *Análisis profundo del desarrollo, métricas y resultados.* ([Descarga directa](documentation/informe_tecnico.pdf?raw=true&inline=false))
>
> **[Manual de Usuario (PDF)](documentation/manual_usuario.pdf)** — *Guía paso a paso para configurar y operar la interfaz.* ([Descarga directa](documentation/manual_usuario.pdf?raw=true&inline=false))
>
> **[Diagrama de Arquitectura (Imagen)](documentation/arquitectura_sistema.png)** — *Mapa conceptual de la infraestructura del sistema.*

---

## 1. Visión General Metodológica

AgroIA Colombia adopta una metodología **CRISP-DM** (Cross-Industry Standard Process for Data Mining) adaptada al contexto agro-climático, dividida en cinco fases principales:

```
Fuentes de Datos → ETL Pipeline → Base de Datos Estrella → Modelos IA → Aplicación Web
```

Cada fase está implementada en módulos Python independientes y orquestada por `run_pipeline.py`.

---

## 2. Fase 1: Extracción de Datos (Extract)

### 2.1 Fuentes y Métodos de Extracción

El módulo `extract/` contiene scripts especializados para cada fuente:

| Script | Fuente | Método | Volumen estimado |
|--------|--------|--------|-----------------|
| `extract_divipola.py` | DANE — datos.gov.co (Socrata) | REST API JSON | 1.122 registros |
| `extract_produccion.py` | DANE EVA — datos.gov.co (Socrata) | REST API JSON | ~200.000 registros |
| `extract_ideam_estaciones.py` | IDEAM — datos.gov.co (Socrata) | REST API JSON | 991 estaciones |
| `extract_ideam_clima.py` | IDEAM — datos.gov.co (Socrata) | REST API paginada | ~5M registros |
| `extract_noaa_enso.py` | NOAA Climate Prediction Center | Web scraping + parsing | ~3.000 registros mensuales |
| `extract_sipsa.py` | DANE SIPSA — Microdatos | Descarga CSV/Excel | ~800.000 registros |
| `extract_sipra.py` | UPRA — GeoServer WFS | GeoJSON | ~50.000 polígonos |
| `extract_nasa_power.py` | NASA POWER MERRA-2 | REST API por punto geográfico | Diario por municipio |
| `extract_insumos.py` | DANE SIPSA Insumos | Descarga CSV/Excel | ~500.000 registros |
| `extract_openmeteo.py` | Open-Meteo | REST API JSON | Histórico meteorológico |

### 2.2 Protocolo de Extracción

1. **Autenticación**: Se utiliza un token Socrata (`SOCRATA_TOKEN`) para acceso prioritario a las APIs de datos.gov.co, aumentando los límites de rate y el tamaño de página de 1.000 a 50.000 registros.
2. **Paginación**: Todas las APIs Socrata se consumen con paginación usando `$offset` y `$limit`.
3. **Manejo de errores**: Cada script implementa reintentos exponenciales (máx. 3) y logging de errores en `logs/etl_run.log`.
4. **Caché local**: Los archivos descargados se guardan en `data/raw/` para evitar descargas repetidas.

---

## 3. Fase 2: Transformación y Limpieza (Clean)

### 3.1 Limpieza de Datos Climáticos (`clean_clima.py`)

- **Unificación de series**: Combina precipitación de estaciones IDEAM con datos de temperatura/humedad de la fuente combinada, usando `id_estacion` como llave.
- **Agregación mensual**: Convierte registros crudos (cada 10 minutos o diarios) a promedios/sumas mensuales.
- **Detección de outliers**: Elimina registros donde la precipitación mensual excede 3 desviaciones estándar de la media histórica de la estación.
- **Join espacial**: Asigna cada estación al municipio más cercano dentro de un radio de 50 km usando distancia Haversine.

### 3.2 Limpieza de Producción (`clean_municipios.py`)

- **Normalización DIVIPOLA**: Todos los códigos de municipio se convierten a strings de exactamente 5 dígitos con `str.zfill(5)`.
- **Resolución de nombres**: Se aplica fuzzy matching para corregir variaciones ortográficas en nombres de municipios (ej. "Bogota", "Bogotá D.C.", "BOGOTÁ").
- **Imputación de valores faltantes**: Las áreas y producciones nulas se imputan con 0 (ausencia de cultivo), no con la media, para preservar la semántica de los datos.

### 3.3 Limpieza de Precios (`clean_precios.py`, `clean_insumos.py`)

- **Deflactación**: Los precios nominales se ajustan por el IPC para obtener precios reales comparables entre años.
- **Normalización de categorías**: Las categorías de insumos (fertilizante, agroquimico, semilla, combustible) se normalizan a vocabulario controlado.
- **Eliminación de duplicados**: Se aplican ventanas temporales para detectar y eliminar registros duplicados en el mismo mes.

### 3.4 Limpieza de Suelos (`clean_suelo.py`)

- **Unión espacial SIPRA**: Los polígonos GeoJSON de aptitud de suelo se intersectan con los polígonos municipales de DIVIPOLA para obtener la clase de aptitud dominante por municipio × cultivo.
- **Mapeo de categorías**: `alta → 3`, `moderada → 2`, `marginal → 1`, `no_apta → 0` para uso como feature numérica en el modelo.

---

## 4. Fase 3: Carga a Base de Datos (Load)

### 4.1 Diseño: Star Schema

La base de datos sigue un **esquema estrella** optimizado para consultas analíticas OLAP:

```
Dimensiones:
  dim_municipio       → dim_region_natural
  dim_cultivo
  dim_tiempo          (con banderas ENSO)
  dim_estacion_ideam  → dim_municipio
  dim_central_abastos → dim_municipio

Tablas de Hechos:
  fact_produccion_agricola   → municipio + cultivo + tiempo
  fact_clima_mensual         → estacion + municipio + tiempo
  fact_precios_mayoristas    → central + cultivo + tiempo
  fact_precios_insumos       → tiempo + region
  fact_alerta_enso           → tiempo + region
  fact_aptitud_suelo         → municipio + cultivo

Predicciones:
  pred_rendimiento           → municipio + cultivo + tiempo
  pred_alerta_climatica      → municipio + tiempo

Vistas Power BI:
  v_dashboard_agro
  v_monitor_climatico
  v_predicciones_modelo
  v_alertas_climaticas
```

### 4.2 Motor: PostgreSQL (Supabase)

- **Motor**: PostgreSQL 15 alojado en Supabase (tier gratuito con 500 MB de almacenamiento).
- **Conexión**: SQLAlchemy con pool de conexiones y manejo de transacciones.
- **Upsert**: Se usa la estrategia `INSERT ... ON CONFLICT DO UPDATE` para garantizar idempotencia del pipeline: ejecutar el ETL dos veces no duplica registros.

---

## 5. Fase 4: Modelado de Machine Learning

### 5.1 Feature Engineering (`build_features.py`)

El **Feature Store** construye una tabla analítica con la granularidad:

> **Una fila = una observación de cosecha** (municipio × cultivo × año)

Variables generadas:

- **Clima anual**: Suma/promedio de precipitación, temperatura, humedad y brillo solar por año y municipio.
- **Desagregación semestral**: Lluvia y temperatura para semestre A (Ene–Jun) y semestre B (Jul–Dic).
- **Lags temporales**: Variables de clima, ENSO y precios del año anterior (lag 1) y de 3 años atrás (lag 3), capturando efectos de largo plazo en cultivos permanentes.
- **Historia del municipio**: Rendimiento y producción histórica promedio del par municipio×cultivo (excluyendo el año actual para evitar data leakage).
- **Codificación de categorías**: Municipio, departamento y región con label encoding; aptitud de suelo como score ordinal.
- **Variables ENSO**: SPI, anomalía de lluvia, probabilidades de déficit/exceso e indicador binario de año El Niño.

### 5.2 Entrenamiento del Modelo (`train_rendimiento.py`)

**Algoritmo**: XGBoost Regressor

**Protocolo de validación temporal**:
- Se usa `TimeSeriesSplit` con 5 folds para la búsqueda de hiperparámetros, respetando el orden cronológico de los datos (sin data leakage futuro).
- Para la evaluación final, se aplica un **hold-out temporal**: los últimos 20% de años forman el conjunto de prueba (los datos más recientes, nunca vistos durante el entrenamiento).

**Optimización de hiperparámetros con Optuna (200 trials)**:
```
n_estimators:     400 – 1.500
max_depth:        4 – 10
learning_rate:    0.01 – 0.15 (log-uniforme)
subsample:        0.6 – 1.0
colsample_bytree: 0.6 – 1.0
min_child_weight: 1 – 10
reg_alpha:        0.001 – 5.0 (L1, log-uniforme)
reg_lambda:       0.001 – 5.0 (L2, log-uniforme)
```

**Métricas de evaluación**:
- MAE (Mean Absolute Error) — minimizado durante Optuna
- RMSE (Root Mean Square Error)
- R² (Coeficiente de Determinación)

**Versionado**: Cada entrenamiento registra una nueva versión en `model_version` con las métricas completas en formato JSONB.

### 5.3 Explicabilidad con SHAP

Para cada predicción, el sistema calcula:
- **Top-10 features globales** (importancia media del valor absoluto de SHAP en muestra de 1.000 observaciones).
- **Top-3 features por predicción** (con valor SHAP y dirección del impacto: positivo/negativo), almacenados como `JSONB` en `pred_rendimiento.shap_top`.

Esto permite al usuario ver, por ejemplo:  
*"El rendimiento del maíz en Montería 2025 fue predicho en 3.8 t/ha. Los factores más influyentes fueron: lluvia_acumulada_anual (+0.4 t/ha), clase_aptitud_score (+0.2 t/ha) y precio_insumo_promedio (−0.1 t/ha)."*

### 5.4 Modelo de Alertas Climáticas (`train_alerta_climatica.py`)

Un modelo complementario de clasificación que predice el nivel de riesgo climático (`BAJO`, `MEDIO`, `ALTO`) por municipio y mes, usando como entrada las señales ENSO y las series temporales de precipitación. El resultado se almacena en `pred_alerta_climatica` y se visualiza en el mapa de la pantalla de Inicio.

---

## 6. Fase 5: Análisis Estadístico ANOVA (`validate/anova_tests.py`)

### 6.1 Protocolo Estadístico Completo

Cada prueba ANOVA sigue los siguientes pasos:

1. **Test de Levene**: Verifica la homogeneidad de varianzas entre grupos (supuesto del ANOVA paramétrico). Si se viola, se documenta.
2. **ANOVA de una vía** (`scipy.stats.f_oneway`): Calcula el estadístico F y el p-valor.
3. **Post-hoc Tukey HSD** (`statsmodels.stats.multicomp.pairwise_tukeyhsd`): Identifica qué pares de grupos son significativamente diferentes entre sí.
4. **Visualización**: Boxplots con medianas anotadas, escala legible y paleta de colores semántica (rojo para El Niño, azul para La Niña, verde para Neutro).

### 6.2 Las 4 Pruebas ANOVA Implementadas

| # | Variable (Y) | Factor (X) | Grupos | Pregunta |
|---|--------------|------------|--------|----------|
| 1 | Precipitación mensual (mm) | Fase ENSO | El Niño, La Niña, Neutro | ¿El fenómeno ENSO modifica significativamente la lluvia? |
| 2 | Precio de insumos (COP) | Tipo de insumo | fertilizante, agroquimico, semilla, combustible | ¿Existen diferencias de precio entre categorías de insumos? |
| 3 | Precipitación mensual (mm) | Trimestre del año | Q1 (Ene–Mar), Q2 (Abr–Jun), Q3 (Jul–Sep), Q4 (Oct–Dic) | ¿Hay bimodalidad estacional de lluvias en Colombia? |
| 4 | Precipitación satelital (mm, NASA) | Municipio | Ibagué, Pasto, Villavicencio | ¿Las diferencias geográficas de lluvia son estadísticamente significativas? |

### 6.3 Nivel de Significancia

- `p < 0.05`: Significativo (★)
- `p < 0.01`: Muy significativo (★★)
- `p < 0.001`: Altamente significativo (★★★)

Todos los resultados del proyecto obtuvieron **★★★ (p < 0.001)**.

---

## 7. Fase 6: Aplicación Web (Frontend)

### 7.1 Stack y Arquitectura Frontend

| Capa | Tecnología | Función |
|------|-----------|---------|
| Framework | Next.js 14 (App Router) | Routing, SSR, API Routes |
| UI | React 18 | Componentes declarativos |
| Gráficas | SVG nativo en React | Visualizaciones sin dependencias de gráficas |
| Datos | Supabase JS Client | Consultas a PostgreSQL en tiempo real |
| Despliegue | Vercel | CI/CD automático desde GitHub |
| BI | Power BI Embedded | Dashboards corporativos |

### 7.2 Secciones de la Aplicación

1. **Inicio**: KPIs clave (municipios en alerta, cultivos analizados), mapa SVG de riesgo territorial y resumen de alertas ENSO activas.
2. **Dashboards**: Gráficos interactivos de producción, clima y precios con filtros por municipio, cultivo y año.
3. **Predicción**: Formulario inteligente que llama al modelo y retorna el rendimiento predicho con intervalos de confianza y explicación SHAP.
4. **Asistente IA**: Chat con memoria de sesión que responde preguntas en lenguaje natural sobre clima, cultivos y precios.
5. **Metodología**: Documentación del pipeline, métricas del modelo, boxplots ANOVA y análisis estadístico interactivo.
6. **Impacto**: Alcance territorial: municipios cubiertos, cultivos analizados y productores potenciales beneficiados.

---

## 8. Calidad de Datos (`validate/`)

### 8.1 Checks Automáticos (`quality_report.py`)

| Check | Umbral | Indicador |
|-------|--------|-----------|
| % municipios con estación climática | ≥ 80% | Cobertura espacial |
| % registros de producción sin municipio | ≤ 0% | Integridad de FK |
| % registros con rendimiento 0 o NULL | ≤ 5% | Completitud |
| Modelos activos duplicados | = 0 | Unicidad de versión activa |
| % estaciones sin municipio asignado | ≤ 5% | Integridad espacial |
| Duplicados en `fact_clima_mensual` | = 0 | Integridad temporal |

### 8.2 Auditoría (`audit_clean.py`, `audit_sources.py`)

Scripts que generan reportes HTML con distribuciones de variables, detección de outliers multivariados y comparación de estadísticos descriptivos entre fuentes.

---

*Versión del documento: 1.0 — Julio 2026*
