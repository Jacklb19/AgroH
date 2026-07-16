# Arquitectura del Sistema — AgroIA Colombia

## Diagrama de Arquitectura

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        FUENTES DE DATOS EXTERNAS                            │
│                                                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐     │
│  │  IDEAM   │  │   DANE   │  │   UPRA   │  │   NOAA   │  │   NASA   │     │
│  │datos.gov │  │datos.gov │  │ GeoServer│  │   ENSO   │  │  POWER   │     │
│  │  Socrata │  │  Socrata │  │  WFS/GeoJ│  │  (ASCII) │  │ (MERRA2) │     │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘     │
│       │              │              │              │              │          │
└───────┼──────────────┼──────────────┼──────────────┼──────────────┼─────────┘
        │              │              │              │              │
        ▼              ▼              ▼              ▼              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CAPA ETL (Python)                                   │
│                                                                             │
│  ┌───────────────────┐   ┌────────────────────┐   ┌─────────────────────┐  │
│  │   extract/        │   │      clean/         │   │       load/         │  │
│  │                   │   │                     │   │                     │  │
│  │ extract_ideam_*   │──▶│ clean_clima.py      │──▶│ load_dimensions.py  │  │
│  │ extract_produccion│   │ clean_municipios.py │   │ load_facts.py       │  │
│  │ extract_noaa_enso │   │ clean_precios.py    │   │ db.py (SQLAlchemy)  │  │
│  │ extract_sipsa.py  │   │ clean_insumos.py    │   │ schema.sql (DDL)    │  │
│  │ extract_sipra.py  │   │ clean_suelo.py      │   │                     │  │
│  │ extract_nasa_*    │   │                     │   │                     │  │
│  └───────────────────┘   └────────────────────┘   └──────────┬──────────┘  │
│                                                               │             │
│  Orquestador: run_pipeline.py  ─────────────────────────────▶│             │
└──────────────────────────────────────────────────────────────┼─────────────┘
                                                               │
                                                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                 BASE DE DATOS (PostgreSQL 15 — Supabase)                    │
│                                                                             │
│  DIMENSIONES:                           HECHOS:                            │
│  ┌────────────────────┐                 ┌────────────────────────────────┐  │
│  │ dim_region_natural │                 │ fact_produccion_agricola       │  │
│  │ dim_municipio      │◀──FK──────────▶│ fact_clima_mensual             │  │
│  │ dim_tiempo         │                 │ fact_precios_mayoristas        │  │
│  │ dim_cultivo        │                 │ fact_precios_insumos           │  │
│  │ dim_estacion_ideam │                 │ fact_alerta_enso               │  │
│  │ dim_central_abastos│                 │ fact_aptitud_suelo             │  │
│  └────────────────────┘                 └────────────────────────────────┘  │
│                                                                             │
│  MODELO IA:                             VISTAS (Power BI):                 │
│  ┌────────────────────────────────┐     ┌────────────────────────────────┐  │
│  │ model_version (métricas JSONB) │     │ v_dashboard_agro               │  │
│  │ pred_rendimiento + shap_top    │     │ v_monitor_climatico            │  │
│  │ pred_alerta_climatica          │     │ v_predicciones_modelo          │  │
│  │ chat_session / chat_message    │     │ v_alertas_climaticas           │  │
│  └────────────────────────────────┘     └────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                          │                          │
               ┌──────────▼──────────┐   ┌──────────▼──────────┐
               │  MODELOS IA (Python) │   │  Power BI Desktop    │
               │                     │   │                      │
               │ models/             │   │ AgroIA_PowerBI_      │
               │  build_features.py  │   │ Theme.json           │
               │  train_rendimiento  │   │ v_dashboard_agro     │
               │  train_alerta_*     │   │ v_monitor_climatico  │
               │  detect_anomalies   │   │                      │
               │                     │   │ (Informe corporativo)│
               │ validate/           │   └─────────────────────┘
               │  anova_tests.py     │
               │  quality_report.py  │
               └──────────┬──────────┘
                          │ predicciones + SHAP
                          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CAPA FRONTEND (Next.js 14 + React 18)                    │
│                                                                             │
│  web/app/                                                                   │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐         │
│  │  Inicio  │ │Dashboard │ │Predicción│ │Asistente │ │Metodolog.│         │
│  │          │ │          │ │          │ │    IA    │ │+ ANOVA   │         │
│  │ Mapa SVG │ │ Gráficas │ │Formulario│ │   Chat   │ │Boxplots  │         │
│  │ KPIs     │ │ Clima    │ │ XGBoost  │ │  con     │ │Estadíst. │         │
│  │ Alertas  │ │ Producción│ │ SHAP    │ │ memoria  │ │          │         │
│  │  ENSO    │ │ Precios  │ │ explain  │ │ sesión   │ │          │         │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘         │
│                                                                             │
│                    Despliegue: Vercel (CI/CD automático)                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Componentes Principales

### 1. Pipeline ETL — `run_pipeline.py`

Orquestador que coordina la ejecución de todos los módulos en 5 pasos:

| Paso | Módulo | Descripción |
|------|--------|-------------|
| PASO 1 | `run_core_etl()` | Extrae DIVIPOLA, Producción EVA, Estaciones IDEAM, Clima mensual. Carga dimensiones y hechos core. |
| PASO 2 | `run_extended_etl()` | Extrae ENSO, SIPSA precios, SIPRA suelos, Insumos. Carga hechos extendidos. |
| PASO 3 | `run_nasa_fill()` | Rellena brechas de clima con datos satelitales NASA POWER. |
| PASO 4 | `run_models()` | Entrena XGBoost (rendimiento) y modelo de alertas climáticas. Persiste predicciones y SHAP. |
| PASO 5 | `run_anova()` | Ejecuta las 4 pruebas ANOVA y genera boxplots en `data/quality_reports/`. |

**Comando de ejecución:**
```bash
python run_pipeline.py --mode all --once
```

**Modos disponibles:**
```
--mode all        → Ejecuta todos los pasos
--mode etl        → Solo ETL (pasos 1-3)
--mode models     → Solo modelos (paso 4)
--mode validate   → Solo ANOVA (paso 5)
--once            → Ejecutar una sola vez (sin loop)
```

---

### 2. Base de Datos — Star Schema

**Motor**: PostgreSQL 15 (Supabase)  
**Conexión**: SQLAlchemy + psycopg2  
**Configuración**: Variables de entorno en `.env`

```
SUPABASE_DB_HOST=xxx.supabase.co
SUPABASE_DB_PORT=5432
SUPABASE_DB_NAME=postgres
SUPABASE_DB_USER=postgres
SUPABASE_DB_PASSWORD=***
```

**Estadísticas del esquema:**
- 6 tablas de dimensión
- 6 tablas de hechos
- 2 tablas de predicciones IA
- 2 tablas de chat (Asistente IA)
- 4 vistas SQL optimizadas para Power BI
- ~18 tablas en total

---

### 3. Modelos de Inteligencia Artificial

#### Modelo de Rendimiento (`xgboost_rendimiento`)
- **Tipo**: Regresión (XGBRegressor)
- **Target**: `rendimiento_t_ha` (toneladas por hectárea)
- **Features**: ~35 variables (clima, ENSO, precios, suelos, lags temporales)
- **Optimización**: Optuna TPE — 200 experimentos
- **Validación**: TimeSeriesSplit 5-fold + hold-out temporal (últimos 20% de años)
- **Salida**: `pred_rendimiento` con intervalos de confianza y valores SHAP

#### Modelo de Alertas Climáticas (`train_alerta_climatica`)
- **Tipo**: Clasificación (BAJO / MEDIO / ALTO)
- **Target**: Nivel de riesgo climático por municipio-mes
- **Salida**: `pred_alerta_climatica` con score de probabilidad y descripción

#### Detección de Anomalías (`detect_anomalies.py`)
- Identifica municipios con rendimientos anormalmente bajos respecto a su historial.
- Complementa el sistema de alertas.

---

### 4. Análisis Estadístico ANOVA

```
validate/anova_tests.py
    │
    ├── Test 1: Precipitación vs. ENSO (El Niño / La Niña / Neutro)
    │   └── Boxplot: anova_enso_lluvia.png
    │
    ├── Test 2: Precio vs. Tipo de Insumo
    │   └── Boxplot: anova_insumos_precio.png
    │
    ├── Test 3: Precipitación vs. Trimestre
    │   └── Boxplot: anova_estacionalidad_lluvia.png
    │
    └── Test 4: Precipitación NASA vs. Municipio
        └── Boxplot: anova_nasa_municipio.png

Protocolo por prueba:
1. Test de Levene (homogeneidad de varianzas)
2. ANOVA f_oneway (scipy.stats)
3. Tukey HSD post-hoc (statsmodels)
4. Visualización con matplotlib
```

---

### 5. Frontend — Next.js 14

**Directorio**: `web/`  
**Despliegue**: Vercel (CI/CD automático desde GitHub)

| Archivo/Directorio | Descripción |
|--------------------|-------------|
| `web/app/` | Páginas de la aplicación (App Router Next.js) |
| `web/lib/` | Clientes de Supabase y utilidades |
| `web/public/` | Assets estáticos (imágenes, íconos) |
| `web/next.config.mjs` | Configuración de Next.js |
| `web/.env` | Variables de entorno del frontend |
| `web/POWERBI_INTEGRATION.md` | Documentación de integración Power BI |

**Variables de entorno del frontend:**
```
NEXT_PUBLIC_SUPABASE_URL=https://xxx.supabase.co
NEXT_PUBLIC_SUPABASE_ANON_KEY=***
NEXT_PUBLIC_OPENAI_API_KEY=***  (Asistente IA)
```

---

### 6. Contenedorización — Docker

```dockerfile
# Dockerfile — Imagen del pipeline ETL
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "run_pipeline.py", "--mode", "all", "--once"]
```

```bash
# Construcción y ejecución
docker build -t agroia/colombia:latest .
docker run --env-file .env agroia/colombia:latest
```

**Railway deployment** (`railway.json`): Configurado para despliegue automático del pipeline en Railway.app con variables de entorno secretas.

---

## Flujo de Datos — Diagrama de Secuencia

```
Usuario ──[Abre la app]──▶ Next.js (Vercel)
                              │
                              ├──[/inicio]──▶ Supabase
                              │               └── v_alertas_climaticas
                              │               └── pred_alerta_climatica
                              │
                              ├──[/prediccion]──▶ Supabase
                              │                  └── pred_rendimiento + shap_top
                              │
                              ├──[/dashboard]──▶ Supabase
                              │                 └── v_dashboard_agro
                              │                 └── v_monitor_climatico
                              │
                              └──[/asistente]──▶ OpenAI API
                                                └── Contexto: chat_session/message

run_pipeline.py (nocturno/manual)
    │
    ├── Extrae datos de APIs externas
    ├── Limpia y normaliza
    ├── Carga a PostgreSQL (Supabase)
    ├── Entrena modelos XGBoost + Optuna
    ├── Persiste predicciones + SHAP
    └── Ejecuta pruebas ANOVA → genera boxplots
```

---

## Tecnologías y Versiones

| Componente | Tecnología | Versión |
|-----------|-----------|---------|
| Python | Python | 3.11+ |
| ETL | pandas, SQLAlchemy | 2.x |
| ML | XGBoost, Optuna, SHAP | latest |
| Estadística | scipy, statsmodels | latest |
| BD | PostgreSQL (Supabase) | 15 |
| Frontend | Next.js + React | 14 / 18 |
| Despliegue web | Vercel | — |
| Despliegue pipeline | Railway / Docker | — |
| BI | Power BI Desktop | — |

---

*Versión del documento: 1.0 — Julio 2026*
