# AgroIA Colombia — Pipeline de Datos Agrícolas

Proyecto para el hackathon **Datos al Ecosistema 2026**.
Sistema ETL + modelos de IA para análisis de rendimiento agrícola y alertas climáticas en Colombia.

---

## Para los integrantes del equipo — primeros pasos

Si acabás de clonar el repo, seguí estos pasos en orden. No necesitás descargar nada manualmente, todos los archivos de datos ya están incluidos en el repositorio.

### Paso 1 — Clonar el repositorio

```bash
git clone <URL-del-repo>
cd AgroH
```

### Paso 2 — Instalar dependencias

```bash
pip install -r requirements.txt
```

### Paso 3 — Crear el archivo `.env`

Crear un archivo llamado `.env` en la raíz del proyecto (al lado de `run_pipeline.py`).
Pedirle a Steven los valores de conexión a la base de datos y copiarlos así:

```env
SUPABASE_DB_HOST=<host de la base de datos>
SUPABASE_DB_PORT=5432
SUPABASE_DB_NAME=postgres
SUPABASE_DB_USER=<usuario>
SUPABASE_DB_PASSWORD=<contraseña>
```

> **Nunca subas el `.env` al repositorio.** Ya está en el `.gitignore`.

### Paso 4 — Correr el pipeline

```bash
# Ejecutar todo de una sola vez
python run_pipeline.py --once --mode all
```

Si querés correrlo por partes:

```bash
python run_pipeline.py --once --mode core       # Descarga y carga datos principales
python run_pipeline.py --once --mode extended   # Precios, suelo, censo, insumos
python run_pipeline.py --once --mode models     # Entrena los modelos de IA
```

El pipeline descarga automáticamente los datos que no están en el repo (producción, clima, estaciones IDEAM). Los datos manuales como SIPSA, SIPRA y CNA **ya están en el repo**, no hay que descargar nada extra.

### Paso 5 — (Opcional) Shapefile de Frontera Agrícola

Este archivo pesa 765 MB y no puede subirse a Git. El pipeline funciona perfectamente sin él, pero si necesitás trabajar con la capa de Frontera Agrícola:

1. Ir a [sipra.upra.gov.co](https://sipra.upra.gov.co) → Descarga de Información → Frontera Agrícola
2. Descargar el shapefile y descomprimirlo
3. Copiar todos los archivos (`Frontera_Agricola_Jun2025.*`) a:
   ```
   data/raw/manual/sipra/
   ```

---

## Qué hace este proyecto

Descarga, limpia y carga datos agrícolas y climáticos de Colombia en una base de datos PostgreSQL con esquema estrella. Luego entrena dos modelos de IA:

1. **Predicción de rendimiento** (XGBoost Regressor) — predice toneladas/hectárea por municipio y cultivo.
2. **Alerta climática** (XGBoost Classifier) — clasifica el riesgo climático como BAJO / MEDIO / ALTO por municipio.

Los datos quedan listos para conectar Power BI o cualquier dashboard.

---

## Arquitectura

```
Fuentes externas (APIs + archivos manuales)
            ↓
   extract/   →   clean/   →   load/
            ↓
       PostgreSQL (17 tablas)
            ↓
   models/  →  pred_rendimiento + pred_alerta_climatica
            ↓
   validate/ → reporte de calidad de datos
```

---

## Estructura del proyecto

```
AgroH/
├── config/
│   ├── settings.py               # Rutas, años, URLs de fuentes
│   └── synonyms_municipios.csv   # Equivalencias de nombres de municipios
│
├── extract/                      # Descarga de datos desde APIs y archivos
│   ├── extract_divipola.py       # DIVIPOLA — códigos municipales oficiales
│   ├── extract_produccion.py     # Producción agrícola A04/A05 (datos.gov.co)
│   ├── extract_ideam_estaciones.py  # Estaciones meteorológicas IDEAM
│   ├── extract_ideam_clima.py    # Clima mensual IDEAM (precipitación, temperatura)
│   ├── extract_ideam_pdf.py      # Boletines ENSO en PDF (pdfplumber)
│   ├── extract_municipios_geo.py # Genera polígonos Voronoi desde DIVIPOLA
│   ├── extract_sipra.py          # Aptitud de suelos SIPRA/UPRA (archivos del repo)
│   ├── extract_sipsa.py          # Precios mayoristas SIPSA/DANE (archivos del repo)
│   └── extract_insumos.py        # Precios de insumos (API + serie sintética IPIA)
│
├── clean/                        # Normalización y limpieza
│   ├── clean_municipios.py       # Resuelve nombres de municipios a códigos DIVIPOLA
│   ├── clean_clima.py            # Unifica clima a granularidad mensual
│   ├── clean_precios.py          # Normaliza precios SIPSA
│   ├── clean_suelo.py            # Agrega aptitud de suelo por municipio
│   └── clean_insumos.py          # Normaliza precios de insumos
│
├── load/
│   ├── schema.sql                # DDL completo — crea las 17 tablas
│   ├── db.py                     # Conexión SQLAlchemy + función upsert()
│   ├── load_dimensions.py        # Carga dimensiones (región, tiempo, municipio...)
│   └── load_facts.py             # Carga hechos (producción, clima, precios...)
│
├── models/
│   ├── train_rendimiento.py      # XGBoost para predicción de rendimiento agrícola
│   └── train_alerta_climatica.py # XGBoost para clasificación de riesgo climático
│
├── validate/
│   └── quality_report.py         # Reporte de calidad con alertas automáticas
│
├── data/
│   └── raw/
│       ├── manual/
│       │   ├── sipra/            # Capas GeoJSON de aptitud SIPRA (en el repo)
│       │   ├── sipsa/            # Archivos Excel de precios SIPSA (en el repo)
│       │   ├── cna/              # CSV del Censo Nacional Agropecuario 2014
│       │   └── municipios/       # GeoJSON de polígonos municipales (se genera solo)
│       └── boletines_pdf/        # PDFs de boletines ENSO del IDEAM (en el repo)
│
├── logs/                         # Logs de ejecución (no se suben a git)
├── run_pipeline.py               # Orquestador principal
├── requirements.txt
└── AGROIA_README.md              # Este archivo
```

---

## Datos en el repo vs datos que descarga el pipeline

| Dato | Origen | Está en el repo |
|---|---|---|
| Aptitud de suelos SIPRA (arroz, maíz, papa) | sipra.upra.gov.co | ✅ Sí |
| Precios SIPSA 2013-2022 + mensuales | dane.gov.co | ✅ Sí |
| Censo Agropecuario CNA 2014 | DANE | ✅ Sí |
| Boletines ENSO IDEAM (PDF) | ideam.gov.co | ✅ Sí |
| Producción agrícola A04/A05 | datos.gov.co API | ❌ Se descarga solo |
| Estaciones IDEAM | datos.gov.co API | ❌ Se descarga solo |
| DIVIPOLA (municipios) | datos.gov.co API | ❌ Se descarga solo |
| Clima mensual IDEAM | datos.gov.co API | ❌ Se descarga solo |
| Frontera Agrícola (shapefile) | sipra.upra.gov.co | ❌ Muy grande (765 MB) |

> Los datos que "se descargan solos" se guardan localmente en `data/raw/` la primera vez.
> En ejecuciones siguientes se cargan desde caché sin llamar la API de nuevo.

---

## Base de datos — Tablas generadas

### Dimensiones

| Tabla | Descripción | Registros aprox. |
|---|---|---|
| `dim_region_natural` | 5 regiones naturales de Colombia | 5 |
| `dim_tiempo` | Meses desde 2007 hasta 2025 | 228 |
| `dim_municipio` | 1.122 municipios con código DIVIPOLA | 1.122 |
| `dim_cultivo` | 247 cultivos con familia y ciclo | 247 |
| `dim_estacion_ideam` | 9.400 estaciones meteorológicas | 9.400 |
| `dim_central_abastos` | Centrales de abasto del SIPSA | ~92 |

### Hechos

| Tabla | Descripción | Registros aprox. |
|---|---|---|
| `fact_produccion_agricola` | Área, producción y rendimiento por municipio/cultivo/año | 145.322 |
| `fact_clima_mensual` | Precipitación, temperatura, humedad mensual | 1.858+ |
| `fact_alerta_enso` | Fase ENSO, SPI e índices climáticos por trimestre | 18+ |
| `fact_precios_mayoristas` | Precio promedio COP/kg por producto y central | 21.249 |
| `fact_aptitud_suelo` | Aptitud agrícola por municipio y cultivo (SIPRA) | 51 |
| `fact_censo_agropecuario` | Indicadores del CNA 2014 por departamento | 33 |
| `fact_precios_insumos` | Precios de fertilizantes, agroquímicos, mano de obra | 936 |

### Predicciones (modelos IA)

| Tabla | Descripción |
|---|---|
| `pred_rendimiento` | Rendimiento predicho t/ha con intervalo de confianza |
| `pred_alerta_climatica` | Nivel de riesgo BAJO/MEDIO/ALTO por municipio |
| `model_version` | Registro de versiones de modelos entrenados |

---

## Modelos de IA

### Modelo 1 — Rendimiento Agrícola

- **Algoritmo:** XGBoost Regressor
- **Target:** rendimiento en toneladas/hectárea
- **Features:** año, área sembrada, área cosechada, temperatura, precipitación
- **Métricas:** R² = 0.75 | MAE = 4.04 t/ha | RMSE = 12.09 t/ha
- **Salida:** 145.322 predicciones en `pred_rendimiento`

### Modelo 2 — Alerta Climática

- **Algoritmo:** XGBoost Classifier (BAJO / MEDIO / ALTO)
- **Features:** precipitación, temperatura, humedad, brillo solar, fase ENSO, SPI
- **Nota:** con más datos IDEAM históricos (desbloquear los `.skip`) mejora el modelo
- **Salida:** 1.578 predicciones en `pred_alerta_climatica`

---

## Caché y archivos `.skip`

El pipeline no re-descarga datos si ya existen localmente:

- **CSV/Parquet en `data/raw/`** → si el archivo existe, se usa el caché.
- **Archivos `.skip` en `data/raw/clima/`** → marcan años que fallaron por timeout en la API de IDEAM. Para forzar una re-descarga de un año específico:

```bash
# Ejemplo: reintentar descarga del clima 2023
del data\raw\clima\clima_combinado_mensual_2023.skip
python run_pipeline.py --once --mode core
```

---

## Modo automático (scheduler semanal)

```bash
# Corre el ETL core cada lunes 2:00 AM y modelos cada lunes 4:00 AM (hora Bogotá)
python run_pipeline.py

# En segundo plano sin ventana (Windows)
pythonw run_pipeline.py
```

---

## Próximos pasos del proyecto

- [ ] Conectar Power BI a PostgreSQL (5 dashboards: clima, rendimiento, precios, alertas, insumos)
- [ ] Publicar dashboards en Power BI Service y embeber en página web
- [ ] Agregar API de clima en tiempo real (OpenWeatherMap)
- [ ] Chatbot con Claude API que consulta la BD y genera recomendaciones en lenguaje natural
- [ ] Página web con los 5 dashboards embebidos + chat widget

---

## Equipo

Hackathon Datos al Ecosistema 2026 — AgroIA Colombia
